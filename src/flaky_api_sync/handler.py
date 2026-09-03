"""Sync subscriber records out of a billing partner's API that throttles and 503s.

The partner exposes an asynchronous export: request one, poll until it is built,
then download it through a short-lived signed URL. Three failures need three
different answers, and choosing between them is what this example is for.

A 429 or a 503 is a good request arriving at a bad moment, so the step retries it
with backoff and honors `Retry-After` when the partner sent one. A 401 or a 422
is the request itself being wrong, so it fails the execution on the first attempt
rather than spending four more minutes reaching the same answer. An expired
download URL is neither: that request can never succeed again, but minting a
fresh URL and starting the download over can, so `with_retry` re-runs the whole
block instead.

The handler keeps the shape a Lambda reviewer expects: module-scope clients,
module-scope configuration, and `lambda_handler(event, context)` at module level.
Tests reach every seam with `monkeypatch.setattr` — see
tests/conftest_flaky_api_sync.py.
"""

import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable

import boto3
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python import WithRetryConfig
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python import with_retry
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.config import JitterStrategy
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.types import DurableContext as DurableContextInterface
from aws_durable_execution_sdk_python.types import StepContext
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import WaitDecision
from aws_durable_execution_sdk_python.waits import WaitForConditionConfig
from aws_durable_execution_sdk_python.waits import WaitForConditionDecision
from aws_durable_execution_sdk_python.waits import WaitStrategyConfig
from aws_durable_execution_sdk_python.waits import create_wait_strategy

from flaky_api_sync.logic import DownloadUrlExpired
from flaky_api_sync.logic import PartnerApiError
from flaky_api_sync.logic import RetryLimits
from flaky_api_sync.logic import RetryPlan
from flaky_api_sync.logic import chunked
from flaky_api_sync.logic import export_is_running
from flaky_api_sync.logic import is_url_expiry_status
from flaky_api_sync.logic import parse_retry_after
from flaky_api_sync.logic import plan_block_retry
from flaky_api_sync.logic import plan_step_retry
from flaky_api_sync.logic import to_write_request

PARTNER_API_URL = os.environ['PARTNER_API_URL']
PARTNER_API_TOKEN = os.environ['PARTNER_API_TOKEN']
SUBSCRIBER_TABLE = os.environ['SUBSCRIBER_TABLE']

HTTP_TIMEOUT_SECONDS = 10
WRITE_BATCH_SIZE = 25

REQUEST_MAX_ATTEMPTS = 5
REQUEST_BASE_DELAY_SECONDS = 2
REQUEST_MAX_DELAY_SECONDS = 60

POLL_MAX_ATTEMPTS = 120
POLL_INITIAL_DELAY_SECONDS = 5
POLL_MAX_DELAY_SECONDS = 120
POLL_BACKOFF_RATE = 1.5

DOWNLOAD_MAX_ATTEMPTS = 3
DOWNLOAD_BASE_DELAY_SECONDS = 5
DOWNLOAD_MAX_DELAY_SECONDS = 30


class PartnerBillingApi:
    """urllib client that turns every HTTP failure into a typed `PartnerApiError`."""

    def __init__(self, base_url: str, token: str) -> None:
        self._base_url = base_url.rstrip('/')
        self._token = token

    def _call(self, method: str, path: str, body: dict | None = None) -> dict:
        request = urllib.request.Request(
            f'{self._base_url}{path}',
            method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization': f'Bearer {self._token}', 'Content-Type': 'application/json'},
        )
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as http_error:
            raise PartnerApiError(
                http_error.code,
                http_error.reason or 'partner request failed',
                parse_retry_after(http_error.headers.get('Retry-After')),
            ) from http_error

    def request_export(self, since: str) -> str:
        body = {'entity': 'subscriptions', 'changedSince': since}
        return self._call('POST', '/v2/exports', body)['exportId']

    def get_export(self, export_id: str) -> dict:
        return self._call('GET', f'/v2/exports/{export_id}')

    def mint_download_url(self, export_id: str) -> str:
        return self._call('POST', f'/v2/exports/{export_id}/download')['url']

    def download_records(self, download_url: str) -> list[dict]:
        try:
            with urllib.request.urlopen(download_url, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return json.loads(response.read())['records']
        except urllib.error.HTTPError as http_error:
            if is_url_expiry_status(http_error.code):
                raise DownloadUrlExpired(http_error.code) from http_error
            raise PartnerApiError(http_error.code, http_error.reason or 'download failed') from http_error


dynamodb_client = boto3.client('dynamodb')
partner_api = PartnerBillingApi(PARTNER_API_URL, PARTNER_API_TOKEN)


def to_retry_decision(plan: RetryPlan) -> RetryDecision:
    if not plan.should_retry:
        return RetryDecision.no_retry()
    return RetryDecision.retry(Duration.from_seconds(plan.delay_seconds))


def partner_retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
    """The `StepConfig` retry policy for every call to the partner."""
    limits = RetryLimits(REQUEST_MAX_ATTEMPTS, REQUEST_BASE_DELAY_SECONDS, REQUEST_MAX_DELAY_SECONDS)
    return to_retry_decision(plan_step_retry(error, attempts_made, limits))


def download_retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
    """The `with_retry` policy for the mint-then-download block."""
    limits = RetryLimits(DOWNLOAD_MAX_ATTEMPTS, DOWNLOAD_BASE_DELAY_SECONDS, DOWNLOAD_MAX_DELAY_SECONDS)
    return to_retry_decision(plan_block_retry(error, attempts_made, limits))


def as_condition_strategy(
    wait_strategy: Callable[[dict, int], WaitDecision],
) -> Callable[[dict, int], WaitForConditionDecision]:
    """Adapt a `create_wait_strategy` callable to what `wait_for_condition` reads.

    `create_wait_strategy` returns `WaitDecision`, whose field is `should_wait`.
    The wait_for_condition executor reads `WaitForConditionDecision.should_continue`
    and nothing in the SDK converts between them, so passing the strategy straight
    into `WaitForConditionConfig` raises `AttributeError` on the first poll.
    """

    def decide(observed: dict, attempts_made: int) -> WaitForConditionDecision:
        decision: WaitDecision = wait_strategy(observed, attempts_made)
        if not decision.should_wait:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(decision.delay)

    return decide


def build_export_poll_strategy() -> Callable[[dict, int], WaitForConditionDecision]:
    """Exponential backoff with full jitter, built from the SDK's own wait strategy."""
    return as_condition_strategy(
        create_wait_strategy(
            WaitStrategyConfig(
                should_continue_polling=export_is_running,
                max_attempts=POLL_MAX_ATTEMPTS,
                initial_delay=Duration.from_seconds(POLL_INITIAL_DELAY_SECONDS),
                max_delay=Duration.from_seconds(POLL_MAX_DELAY_SECONDS),
                backoff_rate=POLL_BACKOFF_RATE,
                jitter_strategy=JitterStrategy.FULL,
            )
        )
    )


def write_subscribers(records: list[dict]) -> int:
    """Upsert the export into DynamoDB, batched to the BatchWriteItem limit."""
    written = 0
    for batch in chunked(records, WRITE_BATCH_SIZE):
        dynamodb_client.batch_write_item(RequestItems={SUBSCRIBER_TABLE: [to_write_request(record) for record in batch]})
        written += len(batch)
    return written


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    changed_since = event['changedSince']

    def open_export(step_context: StepContext) -> str:
        export_id = partner_api.request_export(changed_since)
        step_context.logger.info('export %s requested for changes since %s', export_id, changed_since)
        return export_id

    export_id = context.step(
        open_export,
        name='request_export',
        config=StepConfig(retry_strategy=partner_retry_strategy),
    )

    def read_export_status(_state: dict, check_context: WaitForConditionCheckContext) -> dict:
        export = partner_api.get_export(export_id)
        check_context.logger.info('export %s is %s', export_id, export['status'])
        return {'status': export['status'], 'record_count': export.get('recordCount', 0)}

    observed = context.wait_for_condition(
        check=read_export_status,
        config=WaitForConditionConfig(
            wait_strategy=build_export_poll_strategy(),
            initial_state={'status': 'RUNNING', 'record_count': 0},
        ),
        name='export_ready',
    )
    if observed['status'] != 'READY':
        return {'status': 'export-unavailable', 'exportId': export_id, 'partnerStatus': observed['status']}

    def drain_export(child_context: DurableContextInterface, _attempt: int) -> int:
        """`with_retry` types its callable against the narrower context Protocol.

        The Protocol carries step, run_in_child_context, map, parallel, wait and
        create_callback — not wait_for_condition, invoke, or execution_context.
        Annotating this parameter with the concrete class fails the assignment.
        """

        def mint_url(step_context: StepContext) -> str:
            download_url = partner_api.mint_download_url(export_id)
            step_context.logger.info('minted a download url for export %s', export_id)
            return download_url

        download_url = child_context.step(
            mint_url,
            name='mint_download_url',
            config=StepConfig(retry_strategy=partner_retry_strategy),
        )

        def store_records(step_context: StepContext) -> int:
            written = write_subscribers(partner_api.download_records(download_url))
            step_context.logger.info('wrote %d subscriber record(s)', written)
            return written

        return child_context.step(
            store_records,
            name='store_records',
            config=StepConfig(retry_strategy=partner_retry_strategy),
        )

    written = with_retry(
        context,
        drain_export,
        config=WithRetryConfig(retry_strategy=download_retry_strategy),
        name='drain',
    )
    return {
        'status': 'synced',
        'exportId': export_id,
        'records': written,
        'partnerRecordCount': observed['record_count'],
    }
