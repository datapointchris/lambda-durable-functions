"""A nightly refresh: three source extracts in parallel, then a handoff.

Orders, clickstream and inventory come from three unrelated systems and have no
reason to run one after another, so each is a named branch of a single parallel
operation. Branches share one execution environment, and each holds its rows in
memory until they are staged, so `MAX_CONCURRENT_EXTRACTS` caps how many extracts
are resident at once rather than letting the largest three coincide.

Loading the warehouse wants a different timeout, a different memory size and a
different IAM role, so it is a second durable function reached with
`context.invoke` rather than a fourth stage here. The chained invoke is durable:
this execution suspends and resumes with the load's result.

Two branches are dedicated functions and carry `@durable_parallel_branch`. The
third is built from a body shared with every other snapshot-table dataset, which
has no single name to decorate, so `ParallelBranch` names it at the call site.

`CompletionConfig` never raises. Exceeding `tolerated_failure_count` sets the
batch's `completion_reason` and leaves the result in the caller's hands, so
losing two of three sources is read off the batch and abandons the night rather
than publishing a warehouse that is mostly yesterday.

The handler keeps the shape a Lambda reviewer expects: module-scope clients,
module-scope configuration, and `lambda_handler(event, context)` at module
level. Tests reach every seam with `monkeypatch.setattr`.
"""

import json
import os
from collections.abc import Sequence
from typing import Any

import boto3
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python import ParallelBranch
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python import durable_parallel_branch
from aws_durable_execution_sdk_python.concurrency.models import CompletionReason
from aws_durable_execution_sdk_python.config import CompletionConfig
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.config import InvokeConfig
from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.types import StepContext

from pipeline_chain.logic import DATASETS
from pipeline_chain.logic import build_load_payload
from pipeline_chain.logic import extract_summary
from pipeline_chain.logic import is_transient
from pipeline_chain.logic import run_date_from_event
from pipeline_chain.logic import staging_key

s3_client = boto3.client('s3')
rds_data_client = boto3.client('rds-data')
dynamodb_client = boto3.client('dynamodb')

STAGING_BUCKET = os.environ['STAGING_BUCKET']
STAGING_PREFIX = os.environ.get('STAGING_PREFIX', 'staging/')
ORDERS_CLUSTER_ARN = os.environ['ORDERS_CLUSTER_ARN']
ORDERS_SECRET_ARN = os.environ['ORDERS_SECRET_ARN']
ORDERS_DATABASE = os.environ.get('ORDERS_DATABASE', 'commerce')
CLICKSTREAM_BUCKET = os.environ['CLICKSTREAM_BUCKET']
CLICKSTREAM_PREFIX = os.environ.get('CLICKSTREAM_PREFIX', 'events/')
INVENTORY_TABLE = os.environ['INVENTORY_TABLE']
LOAD_FUNCTION_NAME = os.environ['LOAD_FUNCTION_NAME']

MAX_CONCURRENT_EXTRACTS = 2
TOLERATED_EXTRACT_FAILURES = 1
EXTRACT_MAX_ATTEMPTS = 4
EXTRACT_RETRY_DELAY_SECONDS = 20
HANDOFF_TIMEOUT_SECONDS = 900


def retry_transient_source(error: Exception, attempt: int) -> RetryDecision:
    """Retry a source that refused the attempt; give up on anything else.

    Failing fast on a permanent error is what gets the other two datasets loaded
    tonight, because the parallel operation tolerates one dead branch.
    """
    if attempt >= EXTRACT_MAX_ATTEMPTS or not is_transient(type(error).__name__):
        return RetryDecision.no_retry()
    return RetryDecision.retry(Duration.from_seconds(EXTRACT_RETRY_DELAY_SECONDS))


EXTRACT_STEP_CONFIG = StepConfig(retry_strategy=retry_transient_source)


def read_orders(run_date: str) -> list[dict]:
    """One day of orders from Aurora, through the Data API."""
    response = rds_data_client.execute_statement(
        resourceArn=ORDERS_CLUSTER_ARN,
        secretArn=ORDERS_SECRET_ARN,
        database=ORDERS_DATABASE,
        sql='SELECT order_id, customer_id, total_cents FROM orders WHERE order_date = :run_date',
        parameters=[{'name': 'run_date', 'value': {'stringValue': run_date}}],
        formatRecordsAs='JSON',
    )
    return json.loads(response['formattedRecords'])


def list_clickstream_keys(run_date: str) -> list[str]:
    """Every clickstream object the collector wrote for the run date."""
    keys: list[str] = []
    paginator = s3_client.get_paginator('list_objects_v2')
    prefix = f'{CLICKSTREAM_PREFIX}dt={run_date}/'
    for page in paginator.paginate(Bucket=CLICKSTREAM_BUCKET, Prefix=prefix):
        keys.extend(stored_object['Key'] for stored_object in page.get('Contents', []) if not stored_object['Key'].endswith('/'))
    return keys


def read_snapshot_table(table: str, run_date: str) -> list[dict]:
    """One day of rows from a DynamoDB table keyed on the snapshot date."""
    response = dynamodb_client.query(
        TableName=table,
        KeyConditionExpression='snapshot_date = :run_date',
        ExpressionAttributeValues={':run_date': {'S': run_date}},
    )
    return response.get('Items', [])


def stage_records(key: str, records: Sequence[object]) -> None:
    """Write one dataset's extract where the load function expects it."""
    body = '\n'.join(json.dumps(record) for record in records).encode()
    s3_client.put_object(Bucket=STAGING_BUCKET, Key=key, Body=body)


@durable_parallel_branch(name='extract-orders')
def extract_orders(context: DurableContext, run_date: str) -> dict:
    """Two steps, because the Aurora read is the expensive half.

    A failed staging write replays the write alone; the query stays checkpointed.
    """

    def read(step_context: StepContext) -> list[dict]:
        rows = read_orders(run_date)
        step_context.logger.info('orders read rows=%d', len(rows))
        return rows

    rows = context.step(read, name='read_orders', config=EXTRACT_STEP_CONFIG)

    def stage(step_context: StepContext) -> dict:
        key = staging_key(STAGING_PREFIX, 'orders', run_date)
        stage_records(key, rows)
        step_context.logger.info('orders staged at %s', key)
        return extract_summary('orders', key, len(rows))

    return context.step(stage, name='stage_orders', config=EXTRACT_STEP_CONFIG)


@durable_parallel_branch(name='extract-clickstream')
def extract_clickstream(context: DurableContext, run_date: str) -> dict:
    """One step: listing the collector's output is cheap enough to repeat."""

    def index(step_context: StepContext) -> dict:
        keys = list_clickstream_keys(run_date)
        key = staging_key(STAGING_PREFIX, 'clickstream', run_date)
        stage_records(key, [{'key': found} for found in keys])
        step_context.logger.info('clickstream parts=%d', len(keys))
        return extract_summary('clickstream', key, len(keys))

    return context.step(index, name='index_clickstream', config=EXTRACT_STEP_CONFIG)


def snapshot_branch(context: DurableContext, dataset: str, table: str, run_date: str) -> dict:
    """A branch body for any dataset held in a DynamoDB snapshot table."""

    def snapshot(step_context: StepContext) -> dict:
        items = read_snapshot_table(table, run_date)
        key = staging_key(STAGING_PREFIX, dataset, run_date)
        stage_records(key, items)
        step_context.logger.info('%s items=%d', dataset, len(items))
        return extract_summary(dataset, key, len(items))

    return context.step(snapshot, name=f'snapshot_{dataset}', config=EXTRACT_STEP_CONFIG)


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    run_date = run_date_from_event(event)

    def extract_inventory(branch_context: DurableContext) -> dict:
        return snapshot_branch(branch_context, 'inventory', INVENTORY_TABLE, run_date)

    extracts = context.parallel(
        functions=[
            extract_orders(run_date),
            extract_clickstream(run_date),
            ParallelBranch(func=extract_inventory, name='extract-inventory'),
        ],
        name='extract_sources',
        config=ParallelConfig(
            max_concurrency=MAX_CONCURRENT_EXTRACTS,
            completion_config=CompletionConfig(tolerated_failure_count=TOLERATED_EXTRACT_FAILURES),
        ),
    )

    payload = build_load_payload(run_date, extracts.get_results(), DATASETS)
    if extracts.completion_reason is CompletionReason.FAILURE_TOLERANCE_EXCEEDED:
        return {'runDate': run_date, 'mode': 'abandoned', 'missing': payload['missing']}

    loaded: dict[str, Any] = context.invoke(
        LOAD_FUNCTION_NAME,
        payload,
        name='handoff',
        config=InvokeConfig(timeout=Duration.from_seconds(HANDOFF_TIMEOUT_SECONDS)),
    )
    return {'runDate': run_date, 'mode': payload['mode'], 'load': loaded}
