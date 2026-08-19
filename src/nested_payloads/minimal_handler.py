"""The whole minimal approach as a runnable Lambda.

A nested dataclass with a `StrEnum` field crosses two boundaries here — the
step checkpoint and S3 — and neither needs a `SerDes` subclass, a `StepConfig`,
or any codec machinery. `dict`, `list` and `datetime` are already in the default
codec's supported set and it recurses, so a step returning `to_dict()`
checkpoints natively. `from_dict` rebuilds at the call site.

`nested_payloads.handler` is the same job written with the general codec, for
comparison.
"""

import os

import boto3
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.types import StepContext

from nested_payloads.minimal import Manifest, Status, TrackedFile, load_manifest, save_manifest

s3_client = boto3.client('s3')

LANDING_BUCKET = os.environ['LANDING_BUCKET']
LANDING_PREFIX = os.environ.get('LANDING_PREFIX', 'incoming/')
MANIFEST_PREFIX = os.environ.get('MANIFEST_PREFIX', 'manifests/')


def list_tracked_files() -> list[TrackedFile]:
    tracked: list[TrackedFile] = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=LANDING_BUCKET, Prefix=LANDING_PREFIX):
        for stored in page.get('Contents', []):
            if not stored['Key'].endswith('/'):
                tracked.append(
                    TrackedFile(
                        key=stored['Key'],
                        size=stored['Size'],
                        modified=stored['LastModified'],
                    )
                )
    return tracked


def status_for(files: list[TrackedFile], expected: int) -> Status:
    if not files:
        return Status.EMPTY
    return Status.READY if len(files) >= expected else Status.PARTIAL


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    run_id = context.execution_context.durable_execution_arn.rsplit('/', 1)[-1]
    expected = event.get('expectedFiles', 1)
    previous_key = event.get('previousManifestKey')

    def discover(step_context: StepContext) -> dict:
        files = list_tracked_files()
        manifest = Manifest(status_for(files, expected), LANDING_BUCKET, files)
        step_context.logger.info('found %d file(s), status=%s', len(files), manifest.status)
        return manifest.to_dict()

    manifest = Manifest.from_dict(context.step(discover, name='discover'))

    if previous_key:

        def load_previous(step_context: StepContext) -> dict:
            loaded = load_manifest(s3_client, LANDING_BUCKET, previous_key)
            step_context.logger.info('previous manifest held %d file(s)', len(loaded.files))
            return loaded.to_dict()

        previous = Manifest.from_dict(context.step(load_previous, name='load_previous'))
        merged = {f.key: f for f in previous.files} | {f.key: f for f in manifest.files}
        files = sorted(merged.values(), key=lambda f: f.key)
        manifest = Manifest(status_for(files, expected), manifest.bucket, files)

    def persist(step_context: StepContext) -> str:
        key = save_manifest(s3_client, LANDING_BUCKET, f'{MANIFEST_PREFIX}{run_id}.json', manifest)
        step_context.logger.info('saved %s', key)
        return key

    manifest_key = context.step(persist, name='persist')
    return {
        'status': manifest.status,
        'files': len(manifest.files),
        'manifestKey': manifest_key,
    }
