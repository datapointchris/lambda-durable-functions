"""Fanning a scoring run out with context.map, and what the map does not do.

Layer 1 is the pure batching and banding logic. Layer 2 drives the handler
through the real durable runtime, where the assertions that matter are call
counts on the fakes rather than return values — a map iteration re-enters the
handler body, and only a checkpointed step is spared.
"""

import json
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.concurrency.models import BatchResult
from aws_durable_execution_sdk_python.config import ItemBatcher
from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.exceptions import DurableFunctionsTestError

from batch_scoring.logic import UNIDENTIFIED_APPLICATION
from batch_scoring.logic import batch_summary
from batch_scoring.logic import group_into_batches
from batch_scoring.logic import partition_scorable
from batch_scoring.logic import risk_band
from batch_scoring.logic import roll_up_batches
from batch_scoring.logic import score_rows
from conftest_batch_scoring import COMPLETE_FEATURES
from conftest_batch_scoring import MAX_CONCURRENT_BATCHES
from conftest_batch_scoring import application_ids
from conftest_batch_scoring import handler_module
from conftest_batch_scoring import install_fakes
from conftest_batch_scoring import scoring_event
from conftest_batch_scoring import seed_features

# Three batches of three. The last two digits of an id are its probability of
# default, so APP-0090 declines, APP-0050 refers, and APP-0010 approves.
NINE_APPLICATIONS = application_ids(10, 20, 40, 50, 60, 70, 80, 90, 5)
SUMMARY_KEY = 'scores/run-2026-08-19/summary.json'


@pytest.fixture
def scoring(monkeypatch):
    return install_fakes(monkeypatch)


def run(event: dict):
    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        return runner.run(input=json.dumps(event), timeout=60)


def payload(result) -> dict:
    """The handler's return value. `result.result` is None on a failed execution."""
    assert result.result is not None, f'execution did not return: {result.error}'
    return json.loads(result.result)


def failure_message(result) -> str:
    """The error text of an execution that did not return."""
    assert result.result is None, 'execution succeeded'
    assert result.error is not None and result.error.message is not None
    return result.error.message


def complete_row(application_id: str) -> dict:
    return {'applicationId': application_id, **COMPLETE_FEATURES}


# region layer 1: pure logic


def test_applications_are_grouped_into_fixed_size_batches_with_a_short_tail():
    assert group_into_batches(['a', 'b', 'c', 'd', 'e'], 2) == [['a', 'b'], ['c', 'd'], ['e']]


def test_an_empty_run_produces_no_batches():
    assert group_into_batches([], 25) == []


def test_no_batch_is_ever_empty():
    batches = group_into_batches(application_ids(*range(7)), 3)
    assert all(batches)


def test_a_batch_size_below_one_is_rejected():
    with pytest.raises(ValueError, match='at least 1'):
        group_into_batches(['a'], 0)


def test_an_incomplete_row_is_reported_rather_than_scored():
    rows = [complete_row('APP-0010'), {**complete_row('APP-0020'), 'creditScore': None}]
    scorable, rejected = partition_scorable(rows)
    assert [row['applicationId'] for row in scorable] == ['APP-0010']
    assert rejected == ('APP-0020',)


def test_a_row_that_lost_its_identifier_is_still_reported():
    _scorable, rejected = partition_scorable([{'annualIncome': 1}])
    assert rejected == (UNIDENTIFIED_APPLICATION,)


@pytest.mark.parametrize(
    ('probability', 'band'),
    [
        (0.0, 'approve'),
        (0.32, 'approve'),
        (0.33, 'refer'),
        (0.65, 'refer'),
        (0.66, 'decline'),
        (1.0, 'decline'),
    ],
)
def test_the_risk_bands_split_at_their_thresholds(probability, band):
    assert risk_band(probability) == band


def test_a_short_probability_list_names_the_mismatch():
    with pytest.raises(ValueError, match='1 probabilities for 2 rows'):
        score_rows([complete_row('APP-0010'), complete_row('APP-0020')], [0.1])


def test_a_batch_summary_carries_a_key_rather_than_the_score_rows():
    scored = score_rows([complete_row('APP-0090')], [0.9])
    summary = batch_summary(2, scored, ('APP-0020',), 'scores/run-1/batch-0002.jsonl')
    assert summary == {
        'batchIndex': 2,
        'scoresKey': 'scores/run-1/batch-0002.jsonl',
        'scored': 1,
        'rejected': ['APP-0020'],
        'bands': {'approve': 0, 'refer': 0, 'decline': 1},
    }


def test_the_roll_up_sums_bands_and_gathers_every_rejection():
    first = batch_summary(0, score_rows([complete_row('APP-0010')], [0.1]), ('APP-0030',), 'k0')
    second = batch_summary(1, score_rows([complete_row('APP-0090')], [0.9]), ('APP-0020',), 'k1')
    assert roll_up_batches([first, second]) == {
        'batches': 2,
        'scored': 2,
        'rejected': ['APP-0020', 'APP-0030'],
        'bands': {'approve': 1, 'refer': 0, 'decline': 1},
    }


# endregion
# region layer 2: orchestration


def test_every_batch_is_scored_and_the_run_summary_is_written(scoring):
    s3, _endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)

    result = run(scoring_event(NINE_APPLICATIONS))

    assert payload(result) == {
        'runId': 'run-2026-08-19',
        'summaryKey': SUMMARY_KEY,
        'batches': 3,
        'scored': 9,
        'rejected': [],
        'bands': {'approve': 3, 'refer': 3, 'decline': 3},
        'failedBatches': [],
    }
    assert s3.keys_under('scores/') == [
        'scores/run-2026-08-19/batch-0000.jsonl',
        'scores/run-2026-08-19/batch-0001.jsonl',
        'scores/run-2026-08-19/batch-0002.jsonl',
        SUMMARY_KEY,
    ]


def test_the_endpoint_is_called_once_per_batch_not_once_per_application(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)

    run(scoring_event(NINE_APPLICATIONS))

    assert len(endpoint.invocations) == 3
    assert [len(batch) for batch in endpoint.invocations] == [3, 3, 3]
    assert len(s3.get_calls) == 9


def test_max_concurrency_caps_how_many_batches_are_in_flight(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)

    run(scoring_event(NINE_APPLICATIONS))

    assert endpoint.peak_concurrency == MAX_CONCURRENT_BATCHES
    assert endpoint.peak_concurrency < len(endpoint.invocations)


def test_an_incomplete_application_is_reported_without_failing_its_batch(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS, incomplete=('APP-0060',))

    result = run(scoring_event(NINE_APPLICATIONS))

    assert payload(result)['rejected'] == ['APP-0060']
    assert payload(result)['scored'] == 8
    assert payload(result)['failedBatches'] == []
    assert ['APP-0050', 'APP-0070'] in endpoint.invocations


def test_a_batch_lost_within_tolerance_still_finishes_the_run(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.permanent['APP-0060'] = 'ValidationError'

    result = run(scoring_event(NINE_APPLICATIONS))

    assert payload(result)['batches'] == 2
    assert payload(result)['scored'] == 6
    assert payload(result)['bands'] == {'approve': 3, 'refer': 1, 'decline': 2}
    assert SUMMARY_KEY in s3.objects


def test_a_lost_batch_is_reported_by_message_on_the_batch_result(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.permanent['APP-0060'] = 'ValidationError'

    failures = payload(run(scoring_event(NINE_APPLICATIONS)))['failedBatches']

    assert len(failures) == 1
    assert 'ValidationError' in failures[0]


def test_a_run_that_loses_more_batches_than_the_tolerance_is_aborted(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.permanent.update({'APP-0060': 'ValidationError', 'APP-0090': 'ValidationError'})

    result = run(scoring_event(NINE_APPLICATIONS))

    assert '2 of 3 batches failed' in failure_message(result)


def test_an_aborted_run_keeps_its_partial_scores_but_publishes_no_summary(scoring):
    """The scores a surviving batch wrote are real. A summary would claim the night finished."""
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.permanent.update({'APP-0060': 'ValidationError', 'APP-0090': 'ValidationError'})

    run(scoring_event(NINE_APPLICATIONS))

    assert 'scores/run-2026-08-19/batch-0000.jsonl' in s3.objects
    assert SUMMARY_KEY not in s3.objects


def test_the_map_is_one_operation_holding_a_named_context_per_batch(scoring):
    s3, _endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)

    result = run(scoring_event(NINE_APPLICATIONS))

    assert [op.name for op in result.operations] == ['score_batches', 'publish_summary']
    map_operation = result.get_context('score_batches')
    assert {child.name for child in map_operation.child_operations} == {
        'batch-0000-APP-0010',
        'batch-0001-APP-0050',
        'batch-0002-APP-0080',
    }


def test_each_batch_context_holds_its_own_score_and_store_steps(scoring):
    s3, _endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)

    result = run(scoring_event(NINE_APPLICATIONS))

    first_batch = result.get_context('score_batches').get_context('batch-0000-APP-0010')
    assert [step.name for step in first_batch.child_operations] == ['score', 'store']


def test_a_step_inside_a_map_iteration_is_unreachable_with_get_step(scoring):
    """`get_step` scans only top-level operations. `get_all_operations` recurses."""
    s3, _endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)

    result = run(scoring_event(NINE_APPLICATIONS))

    with pytest.raises(DurableFunctionsTestError, match="'score' not found"):
        result.get_step('score')
    assert sum(1 for op in result.get_all_operations() if op.name == 'score') == 3


# endregion
# region replay


def test_a_throttled_batch_is_retried_and_the_others_are_not_re_run(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.transient['APP-0050'] = 1

    result = run(scoring_event(NINE_APPLICATIONS))

    assert payload(result)['scored'] == 9
    assert len(endpoint.invocations) == 4
    assert endpoint.invocations.count(['APP-0050', 'APP-0060', 'APP-0070']) == 2


def test_every_scores_object_is_written_exactly_once_despite_the_replay(scoring):
    """A second write of a batch means a side effect escaped its step."""
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.transient['APP-0050'] = 1

    run(scoring_event(NINE_APPLICATIONS))

    assert len(s3.put_objects) == 4
    assert [put['Key'] for put in s3.put_objects].count(SUMMARY_KEY) == 1


def test_the_handler_body_really_does_re_enter(scoring, monkeypatch):
    """Guards the two tests above: they only prove anything if a replay happened."""
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.transient['APP-0050'] = 1
    entries: list[str] = []
    original = handler_module.group_into_batches

    def counting(*args, **kwargs):
        entries.append('body')
        return original(*args, **kwargs)

    monkeypatch.setattr(handler_module, 'group_into_batches', counting)

    run(scoring_event(NINE_APPLICATIONS))

    assert len(entries) > 1, 'the handler body ran once, so nothing was replayed'


# endregion
# region SDK limits


OBSERVED_ITEMS: list[dict[str, str]] = []


@durable_execution
def batcher_probe(event: dict, context: DurableContext) -> dict:
    def per_item(child_context: DurableContext, item, index: int, _items):
        def record(_step_context):
            OBSERVED_ITEMS.append({'type': type(item).__name__, 'item': str(item)})
            return index

        return child_context.step(record, name='record')

    result: BatchResult[dict[str, Any]] = context.map(
        event['items'],
        per_item,
        name='probe',
        config=MapConfig(item_batcher=ItemBatcher(max_items_per_batch=2, batch_input={'model': 'v3'})),
    )
    return {'iterations': result.total_count}


def test_item_batcher_does_not_group_anything_in_sdk_1_7_0():
    """`MapConfig.item_batcher` is accepted and never read.

    `map_handler` builds one executable per input and `MapExecutor.execute_item`
    passes the raw item, so a batch size of 2 over 4 items still yields 4
    iterations of `str`. Nothing in the SDK constructs the `BatchedInput` its own
    map signature offers, which is why the handler groups before the map.
    """
    OBSERVED_ITEMS.clear()
    with DurableFunctionTestRunner(batcher_probe) as runner:
        result = runner.run(input=json.dumps({'items': ['a', 'b', 'c', 'd']}), timeout=30)

    assert result.result is not None
    assert json.loads(result.result)['iterations'] == 4
    assert [item['type'] for item in OBSERVED_ITEMS] == ['str'] * 4
    assert sorted(item['item'] for item in OBSERVED_ITEMS) == ['a', 'b', 'c', 'd']


# endregion
