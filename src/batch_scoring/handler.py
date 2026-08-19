"""Nightly credit risk scoring: fan a day's loan applications out over a model endpoint.

One application per model call would be tens of thousands of invocations, so the
run groups applications into batches and gives each batch its own map iteration.
Two things are expected to go wrong on any given night and neither should sink
the run: a feature row arrives incomplete, and the endpoint throttles. The first
is dropped and reported by the batch that found it; the second is retried, and
only a run that loses more batches than the tolerance allows is abandoned.

The handler keeps the shape a Lambda reviewer expects: module-scope clients,
module-scope configuration, and `lambda_handler(event, context)` at module
level. Tests reach every seam with `monkeypatch.setattr` — see
tests/conftest_batch_scoring.py.
"""

import json
import os
from collections.abc import Sequence
from typing import Any, cast

import boto3
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.concurrency.models import CompletionReason
from aws_durable_execution_sdk_python.config import (
    BatchedInput,
    CompletionConfig,
    Duration,
    ItemBatcher,
    JitterStrategy,
    MapConfig,
    StepConfig,
)
from aws_durable_execution_sdk_python.retries import RetryStrategyConfig, create_retry_strategy
from aws_durable_execution_sdk_python.types import StepContext

from batch_scoring.logic import (
    ScoringRunAborted,
    batch_operation_name,
    batch_summary,
    group_into_batches,
    partition_scorable,
    roll_up_batches,
    score_rows,
)

s3_client = boto3.client('s3')
sagemaker_runtime_client = boto3.client('sagemaker-runtime')

FEATURE_BUCKET = os.environ['FEATURE_BUCKET']
FEATURE_PREFIX = os.environ.get('FEATURE_PREFIX', 'features/')
SCORE_BUCKET = os.environ['SCORE_BUCKET']
SCORE_PREFIX = os.environ.get('SCORE_PREFIX', 'scores/')
SCORING_ENDPOINT_NAME = os.environ['SCORING_ENDPOINT_NAME']

APPLICATIONS_PER_BATCH = 25
MAX_CONCURRENT_BATCHES = 4
TOLERATED_BATCH_FAILURES = 2
SCORING_MAX_ATTEMPTS = 4
SCORING_RETRY_DELAY_SECONDS = 5
RETRYABLE_ENDPOINT_ERRORS = ('ThrottlingException', 'ModelNotReadyException', 'ServiceUnavailable')


def scoring_step_config() -> StepConfig:
    """Retry only what a second attempt can fix.

    The default preset retries every exception six times. A payload the model
    container rejected is rejected identically on attempt six, so matching on
    the throttling and cold-start codes is what keeps a poisoned batch from
    holding a map iteration open for minutes.
    """
    return StepConfig(
        retry_strategy=create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=SCORING_MAX_ATTEMPTS,
                initial_delay=Duration.from_seconds(SCORING_RETRY_DELAY_SECONDS),
                max_delay=Duration.from_seconds(SCORING_RETRY_DELAY_SECONDS),
                jitter_strategy=JitterStrategy.NONE,
                retryable_errors=list(RETRYABLE_ENDPOINT_ERRORS),
            )
        )
    )


def load_feature_rows(application_ids: Sequence[str]) -> list[dict]:
    """Read one feature row per application out of the feature store prefix."""
    rows = []
    for application_id in application_ids:
        response = s3_client.get_object(Bucket=FEATURE_BUCKET, Key=f'{FEATURE_PREFIX}{application_id}.json')
        rows.append(json.loads(response['Body'].read()))
    return rows


def invoke_scoring_endpoint(rows: Sequence[dict]) -> list[float]:
    """Score a whole batch in one endpoint call."""
    if not rows:
        return []
    response = sagemaker_runtime_client.invoke_endpoint(
        EndpointName=SCORING_ENDPOINT_NAME,
        ContentType='application/json',
        Body=json.dumps({'instances': list(rows)}).encode(),
    )
    return json.loads(response['Body'].read())['probabilities']


def write_batch_scores(run_id: str, batch_index: int, scored: Sequence[dict]) -> str:
    key = f'{SCORE_PREFIX}{run_id}/batch-{batch_index:04d}.jsonl'
    body = '\n'.join(json.dumps(row) for row in scored).encode()
    s3_client.put_object(Bucket=SCORE_BUCKET, Key=key, Body=body)
    return key


def write_run_summary(run_id: str, summary: dict) -> str:
    key = f'{SCORE_PREFIX}{run_id}/summary.json'
    s3_client.put_object(Bucket=SCORE_BUCKET, Key=key, Body=json.dumps(summary).encode())
    return key


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    run_id = event['runId']
    batches = group_into_batches(event['applicationIds'], APPLICATIONS_PER_BATCH)
    step_config = scoring_step_config()

    def score_batch(
        batch_context: DurableContext,
        item: list[str] | BatchedInput[Any, list[str]],
        index: int,
        _batches: Sequence[list[str]],
    ) -> dict:
        """One map iteration. Its `batch_context` is a full DurableContext of its own.

        The declared item type is the union `map` promises. The `BatchedInput` half
        of it is unreachable while `item_batcher` goes unread, so the cast is safe.
        """
        batch = cast(list[str], item)

        def score(step_context: StepContext) -> dict:
            rows = load_feature_rows(batch)
            scorable, rejected = partition_scorable(rows)
            probabilities = invoke_scoring_endpoint(scorable)
            step_context.logger.info(
                'batch %d: %d scorable, %d rejected', index, len(scorable), len(rejected)
            )
            return {'scored': score_rows(scorable, probabilities), 'rejected': list(rejected)}

        scored = batch_context.step(score, name='score', config=step_config)

        def store(step_context: StepContext) -> str:
            key = write_batch_scores(run_id, index, scored['scored'])
            step_context.logger.info('batch %d wrote %s', index, key)
            return key

        scores_key = batch_context.step(store, name='store')
        return batch_summary(index, scored['scored'], scored['rejected'], scores_key)

    batch_result = context.map(
        batches,
        score_batch,
        name='score_batches',
        config=MapConfig(
            max_concurrency=MAX_CONCURRENT_BATCHES,
            # Inert in SDK 1.7.0: map_handler never reads config.item_batcher, so every
            # iteration still gets one raw input. group_into_batches does the grouping.
            item_batcher=ItemBatcher(max_items_per_batch=APPLICATIONS_PER_BATCH),
            completion_config=CompletionConfig(tolerated_failure_count=TOLERATED_BATCH_FAILURES),
            item_namer=batch_operation_name,
        ),
    )

    # A map never raises. An exceeded tolerance is a SUCCEEDED execution carrying a
    # BatchResult that says so, and publishing its partial scores would read as a full night.
    if batch_result.completion_reason is CompletionReason.FAILURE_TOLERANCE_EXCEEDED:
        raise ScoringRunAborted(
            f'{batch_result.failure_count} of {batch_result.total_count} batches failed, '
            f'tolerating {TOLERATED_BATCH_FAILURES}'
        )

    summary = roll_up_batches(batch_result.get_results())

    def publish(step_context: StepContext) -> str:
        key = write_run_summary(run_id, summary)
        step_context.logger.info('run summary at %s', key)
        return key

    summary_key = context.step(publish, name='publish_summary')
    return {
        'runId': run_id,
        'summaryKey': summary_key,
        **summary,
        'failedBatches': [error.message for error in batch_result.get_errors()],
    }
