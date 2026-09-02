"""Retry policy for a partner API that throttles, 503s, and expires its own URLs.

Three layers, in order: the pure decisions with no SDK at all, the wait-strategy
adapter on its own, then the whole orchestration through the real durable runtime
in-process.
"""

import json

import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.config import JitterStrategy
from aws_durable_execution_sdk_python.exceptions import CallableRuntimeError
from aws_durable_execution_sdk_python.lambda_service import InvocationStatus
from aws_durable_execution_sdk_python.waits import WaitStrategyConfig
from aws_durable_execution_sdk_python.waits import create_wait_strategy
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

from conftest_flaky_api_sync import DOWNLOAD_URL
from conftest_flaky_api_sync import EXPORT_ID
from conftest_flaky_api_sync import SUBSCRIBERS
from conftest_flaky_api_sync import export_failed
from conftest_flaky_api_sync import export_ready
from conftest_flaky_api_sync import export_running
from conftest_flaky_api_sync import forbidden
from conftest_flaky_api_sync import install_partner_fakes
from conftest_flaky_api_sync import throttled
from conftest_flaky_api_sync import unavailable
from conftest_flaky_api_sync import url_expired
from flaky_api_sync import handler as handler_module
from flaky_api_sync.logic import DownloadUrlExpired
from flaky_api_sync.logic import PartnerApiError
from flaky_api_sync.logic import RetryLimits
from flaky_api_sync.logic import backoff_delay_seconds
from flaky_api_sync.logic import chunked
from flaky_api_sync.logic import export_is_running
from flaky_api_sync.logic import failing_type_name
from flaky_api_sync.logic import parse_retry_after
from flaky_api_sync.logic import plan_block_retry
from flaky_api_sync.logic import plan_step_retry
from flaky_api_sync.logic import to_write_request

CHANGED_SINCE = '2026-08-18T00:00:00Z'
STEP_LIMITS = RetryLimits(max_attempts=5, base_seconds=2, cap_seconds=60)
BLOCK_LIMITS = RetryLimits(max_attempts=3, base_seconds=5, cap_seconds=30)


@pytest.fixture
def partner(monkeypatch):
    return install_partner_fakes(monkeypatch)


def run(event: dict):
    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        return runner.run(input=json.dumps(event), timeout=60)


def payload(result) -> dict:
    """The handler's return value. `result.result` is None on a failed execution."""
    assert result.result is not None, f'execution did not return: {result.error}'
    return json.loads(result.result)


def test_a_throttle_and_a_gateway_error_are_both_retried():
    assert plan_step_retry(throttled(30), 1, STEP_LIMITS).should_retry
    assert plan_step_retry(unavailable(), 1, STEP_LIMITS).should_retry


def test_a_client_error_is_not_retried():
    """The step must fail fast: an identical second request fails identically."""
    assert not plan_step_retry(forbidden(), 1, STEP_LIMITS).should_retry
    assert not plan_step_retry(PartnerApiError(422, 'unknown entity'), 1, STEP_LIMITS).should_retry


def test_an_error_that_did_not_come_from_the_partner_is_not_retried():
    assert not plan_step_retry(KeyError('changedSince'), 1, STEP_LIMITS).should_retry


def test_retry_after_overrides_the_computed_backoff():
    assert plan_step_retry(throttled(45), 1, STEP_LIMITS).delay_seconds == 45


def test_an_http_date_retry_after_is_ignored():
    """Reading the date form needs the clock, which a replayed step must not do."""
    assert parse_retry_after('Wed, 19 Aug 2026 07:28:00 GMT') is None
    assert parse_retry_after('30') == 30


def test_the_backoff_doubles_until_it_reaches_the_cap():
    delays = [backoff_delay_seconds(attempt, base_seconds=2, cap_seconds=16) for attempt in range(1, 7)]
    assert delays == [2, 4, 8, 16, 16, 16]


def test_the_attempt_budget_stops_even_a_retryable_error():
    assert plan_step_retry(unavailable(), 4, STEP_LIMITS).should_retry
    assert not plan_step_retry(unavailable(), 5, STEP_LIMITS).should_retry


def test_an_expired_url_keeps_only_its_class_name_across_the_step_boundary():
    """A step re-raises as CallableRuntimeError, so `isinstance` no longer holds."""
    crossed = CallableRuntimeError(
        message='410 signed download url rejected',
        error_type='DownloadUrlExpired',
        data=None,
        stack_trace=None,
    )

    assert not isinstance(crossed, DownloadUrlExpired)
    assert failing_type_name(crossed) == 'DownloadUrlExpired'
    assert plan_block_retry(crossed, 1, BLOCK_LIMITS).should_retry


def test_a_throttle_does_not_restart_the_download_block():
    """The step's own policy already answered it; a second pass changes nothing."""
    assert not plan_block_retry(throttled(5), 1, BLOCK_LIMITS).should_retry


def test_the_block_budget_stops_a_repeatedly_expiring_url():
    assert not plan_block_retry(url_expired(), 3, BLOCK_LIMITS).should_retry


def test_records_are_batched_to_the_batch_write_item_limit():
    records = [{'id': f'sub_{index}'} for index in range(60)]

    batches = list(chunked(records, 25))

    assert [len(batch) for batch in batches] == [25, 25, 10]


def test_a_partner_record_becomes_a_typed_dynamodb_put_request():
    assert to_write_request(SUBSCRIBERS[1]) == {
        'PutRequest': {
            'Item': {
                'subscriberId': {'S': 'sub_1002'},
                'planCode': {'S': 'starter-monthly'},
                'subscriptionStatus': {'S': 'past_due'},
                'mrrCents': {'N': '2900'},
            }
        }
    }


def test_the_sdk_wait_strategy_returns_a_field_wait_for_condition_cannot_read():
    """Why the adapter exists. Passing this straight in raises AttributeError."""
    strategy = create_wait_strategy(
        WaitStrategyConfig(
            should_continue_polling=export_is_running,
            initial_delay=Duration.from_seconds(1),
            jitter_strategy=JitterStrategy.FULL,
        )
    )

    decision = strategy(export_running(), 1)

    assert decision.should_wait
    assert not hasattr(decision, 'should_continue')


def test_the_adapter_stops_polling_a_ready_export():
    decide = handler_module.build_export_poll_strategy()

    assert decide(export_ready(3), 1).should_continue is False
    assert decide(export_running(), 1).should_continue is True


def test_the_jittered_poll_delay_never_drops_below_a_second():
    """`create_wait_strategy` clamps up after jitter, so full jitter cannot busy-poll."""
    decide = handler_module.build_export_poll_strategy()

    delays = {decide(export_running(), attempt).delay_seconds for attempt in range(1, 20)}

    assert min(delays) >= 1


def test_a_clean_export_writes_every_subscriber(partner):
    api, dynamo = partner

    result = run({'changedSince': CHANGED_SINCE})

    assert payload(result) == {
        'status': 'synced',
        'exportId': EXPORT_ID,
        'records': 3,
        'partnerRecordCount': 3,
    }
    assert api.requested_since == CHANGED_SINCE
    assert len(dynamo.batches) == 1
    assert len(dynamo.batches[0]['RequestItems']['test-subscribers']) == 3


def test_a_throttled_export_request_is_retried_until_it_succeeds(partner):
    api, _dynamo = partner
    api.request_export_script = [throttled(1), throttled(1), EXPORT_ID]

    result = run({'changedSince': CHANGED_SINCE})

    assert payload(result)['status'] == 'synced'
    assert result.get_step('request_export').attempt == 3
    assert api.calls['request_export'] == 3


def test_a_permanent_error_is_not_retried(partner):
    """The whole point of classifying: one attempt, then the execution fails."""
    api, dynamo = partner
    api.request_export_script = [forbidden()]

    result = run({'changedSince': CHANGED_SINCE})

    assert result.status is InvocationStatus.FAILED
    assert result.get_step('request_export').attempt == 1
    assert api.calls['request_export'] == 1
    assert dynamo.batches == []


def test_a_retryable_error_still_stops_at_the_attempt_budget(partner):
    api, dynamo = partner
    api.request_export_script = [unavailable()]

    result = run({'changedSince': CHANGED_SINCE})

    assert result.status is InvocationStatus.FAILED
    assert result.get_step('request_export').attempt == 3
    assert dynamo.batches == []


def test_the_poll_attempt_count_is_the_number_of_polls(partner):
    api, _dynamo = partner
    api.get_export_script = [export_running(), export_running(), export_ready(3)]

    result = run({'changedSince': CHANGED_SINCE})

    assert payload(result)['status'] == 'synced'
    assert result.get_step('export_ready').attempt == 3
    assert api.calls['get_export'] == 3


def test_an_export_the_partner_abandons_is_reported_rather_than_downloaded(partner):
    api, dynamo = partner
    api.get_export_script = [export_running(), export_failed()]

    result = run({'changedSince': CHANGED_SINCE})

    assert payload(result) == {
        'status': 'export-unavailable',
        'exportId': EXPORT_ID,
        'partnerStatus': 'FAILED',
    }
    assert api.calls['mint_download_url'] == 0
    assert dynamo.batches == []


def test_an_export_that_never_finishes_stops_at_the_poll_budget(partner):
    """`WaitStrategyConfig.max_attempts` gives up, and the handler reads the last state."""
    api, dynamo = partner
    api.get_export_script = [export_running()]

    result = run({'changedSince': CHANGED_SINCE})

    assert payload(result)['status'] == 'export-unavailable'
    assert api.calls['get_export'] == 4
    assert dynamo.batches == []


def test_an_expired_download_url_restarts_the_block_rather_than_the_step(partner):
    """The step declines a 410, so `with_retry` re-mints the URL and starts over."""
    api, dynamo = partner
    api.download_records_script = [url_expired(), SUBSCRIBERS]

    result = run({'changedSince': CHANGED_SINCE})
    drain = result.get_context('drain')

    assert payload(result)['records'] == 3
    assert api.calls['mint_download_url'] == 2
    assert api.calls['download_records'] == 2
    assert len(dynamo.batches) == 1
    assert [op.name for op in drain.child_operations].count('mint_download_url') == 2
    assert drain.get_operation_by_name('drain-backoff-1') is not None


def test_the_expired_step_itself_is_never_retried(partner):
    """Proof the 410 failed fast: the first store attempt was also its last."""
    api, _dynamo = partner
    api.download_records_script = [url_expired(), SUBSCRIBERS]

    result = run({'changedSince': CHANGED_SINCE})
    drain = result.get_context('drain')

    assert [op.name for op in drain.child_operations][:3] == [
        'mint_download_url',
        'store_records',
        'drain-backoff-1',
    ]
    assert drain.get_step('store_records').attempt == 1


def test_a_url_that_keeps_expiring_stops_at_the_block_budget(partner):
    api, dynamo = partner
    api.download_records_script = [url_expired()]

    result = run({'changedSince': CHANGED_SINCE})

    assert result.status is InvocationStatus.FAILED
    assert api.calls['mint_download_url'] == 2
    assert dynamo.batches == []


def test_the_subscriber_write_happens_once_despite_the_step_retries(partner):
    """A second batch write would mean a side effect escaped its step and replayed."""
    api, dynamo = partner
    api.request_export_script = [throttled(1), EXPORT_ID]

    result = run({'changedSince': CHANGED_SINCE})

    assert payload(result)['status'] == 'synced'
    assert len(dynamo.batches) == 1
    assert api.calls['download_records'] == 1
    assert api.calls['mint_download_url'] == 1


def test_the_handler_body_really_does_re_enter(partner, monkeypatch):
    """Guards the test above: it only proves anything if a replay happened."""
    _api, _dynamo = partner
    entries: list[str] = []
    original = handler_module.build_export_poll_strategy

    def counting():
        entries.append('body')
        return original()

    monkeypatch.setattr(handler_module, 'build_export_poll_strategy', counting)

    run({'changedSince': CHANGED_SINCE})

    assert len(entries) > 1, 'the body entered once, so no replay was exercised'


def test_the_download_reads_the_url_the_partner_minted(partner):
    api, _dynamo = partner

    run({'changedSince': CHANGED_SINCE})

    assert api.downloaded_urls == [DOWNLOAD_URL]
