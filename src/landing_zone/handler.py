"""A landing-zone trigger: wait for a file drop to settle, then ingest it once.

An upstream feed writes objects into a prefix over several minutes. Starting one
job per object would be wasteful and would read a partial drop, so this waits
until nothing new has landed for a quiet period, writes a manifest of what
arrived, and starts a single ingest job for it.

The handler keeps the shape a Lambda reviewer expects: module-scope clients,
module-scope configuration, and `lambda_handler(event, context)` at module
level. Tests reach every seam with `monkeypatch.setattr` — see tests/conftest.py.
"""

import os
import time
from urllib.parse import unquote_plus

import boto3
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.types import StepContext
from aws_durable_execution_sdk_python.types import WaitForConditionCheckContext
from aws_durable_execution_sdk_python.waits import WaitForConditionConfig
from aws_durable_execution_sdk_python.waits import WaitForConditionDecision

from landing_zone.logic import Manifest
from landing_zone.logic import build_manifest
from landing_zone.logic import is_quiet
from landing_zone.logic import next_poll_delay_seconds
from landing_zone.serdes import DataclassSerDes

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')
lambda_client = boto3.client('lambda')

LANDING_BUCKET = os.environ['LANDING_BUCKET']
LANDING_PREFIX = os.environ.get('LANDING_PREFIX', 'landing/')
MANIFEST_PREFIX = os.environ.get('MANIFEST_PREFIX', 'manifests/')
INGEST_JOB_NAME = os.environ['INGEST_JOB_NAME']
FUNCTION_NAME = os.environ['AWS_LAMBDA_FUNCTION_NAME']
FUNCTION_VERSION = os.environ['AWS_LAMBDA_FUNCTION_VERSION']

QUIET_PERIOD_SECONDS = 300
POLL_DELAYS_SECONDS = (60,)
STEADY_POLL_DELAY_SECONDS = 300


def is_leader(execution_arn: str) -> bool:
    """True when this execution is the oldest RUNNING one for the qualifier.

    Every object in the drop triggers its own invocation, and they must collapse
    into one run. Reserved concurrency cannot express this: it throttles, so the
    extra async invokes retry and land in the DLQ instead of being ignored.
    """
    running = lambda_client.list_durable_executions_by_function(
        FunctionName=FUNCTION_NAME,
        Qualifier=FUNCTION_VERSION,
        Statuses=['RUNNING'],
        ReverseOrder=True,
        MaxItems=1,
    ).get('DurableExecutions', [])
    return not running or running[0]['DurableExecutionArn'] == execution_arn


def list_landed_objects() -> dict[str, dict]:
    """Key -> {size, modified_epoch} for everything under the landing prefix."""
    landed: dict[str, dict] = {}
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=LANDING_BUCKET, Prefix=LANDING_PREFIX):
        for stored_object in page.get('Contents', []):
            key = stored_object['Key']
            if key.endswith('/'):
                continue
            landed[key] = {
                'size': stored_object['Size'],
                'modified_epoch': stored_object['LastModified'].timestamp(),
            }
    return landed


def write_manifest(manifest: Manifest) -> str:
    """Persist the manifest and return its key. The ingest job reads this, not S3."""
    key = f'{MANIFEST_PREFIX}{manifest.run_id}.json'
    body = '\n'.join(manifest.keys).encode()
    s3_client.put_object(Bucket=LANDING_BUCKET, Key=key, Body=body)
    return key


def start_ingest_job(manifest_key: str) -> str:
    response = glue_client.start_job_run(
        JobName=INGEST_JOB_NAME,
        Arguments={'--manifest': f's3://{LANDING_BUCKET}/{manifest_key}'},
    )
    return response['JobRunId']


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    execution_arn = context.execution_context.durable_execution_arn

    def check_leader(step_context: StepContext) -> bool:
        leader = is_leader(execution_arn)
        step_context.logger.info('leader=%s', leader)
        return leader

    if not context.step(check_leader, name='leader'):
        return {'status': 'ignored'}

    triggering_keys = {unquote_plus(record['s3']['object']['key']) for record in event.get('Records', [])}
    if not triggering_keys:
        return {'status': 'no-trigger'}

    def settle_check(_state: dict, check_context: WaitForConditionCheckContext) -> dict:
        """Reads the clock, so it must run inside the step to be replay-stable."""
        landed = list_landed_objects()
        newest = max((o['modified_epoch'] for o in landed.values()), default=None)
        quiet = is_quiet(newest, time.time(), QUIET_PERIOD_SECONDS)
        check_context.logger.info('landed=%d quiet=%s', len(landed), quiet)
        return {'object_count': len(landed), 'quiet': quiet}

    def keep_waiting(state: dict, attempt: int) -> WaitForConditionDecision:
        if state['quiet'] and state['object_count']:
            return WaitForConditionDecision.stop_polling()
        seconds = next_poll_delay_seconds(attempt, POLL_DELAYS_SECONDS, STEADY_POLL_DELAY_SECONDS)
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(seconds))

    context.wait_for_condition(
        check=settle_check,
        config=WaitForConditionConfig(
            wait_strategy=keep_waiting,
            initial_state={'object_count': 0, 'quiet': False},
        ),
        name='settle',
    )

    def freeze_manifest(step_context: StepContext) -> Manifest:
        landed = list_landed_objects()
        manifest = build_manifest(
            run_id=context.execution_context.durable_execution_arn.rsplit('/', 1)[-1],
            objects={key: value['size'] for key, value in landed.items()},
        )
        step_context.logger.info('manifest holds %d object(s)', len(manifest.keys))
        return manifest

    manifest = context.step(
        freeze_manifest,
        name='freeze_manifest',
        config=StepConfig(serdes=DataclassSerDes(Manifest)),
    )

    def publish_and_start(step_context: StepContext) -> str:
        manifest_key = write_manifest(manifest)
        job_run_id = start_ingest_job(manifest_key)
        step_context.logger.info('started %s as %s', INGEST_JOB_NAME, job_run_id)
        return job_run_id

    job_run_id = context.step(publish_and_start, name='publish_and_start')
    return {'status': 'ingested', 'objects': len(manifest.keys), 'jobRunId': job_run_id}
