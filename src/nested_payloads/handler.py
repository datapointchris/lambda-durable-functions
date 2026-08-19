"""A durable function that loads a nested manifest, amends it, and saves it back.

The same `Manifest` crosses two boundaries in one execution: it is checkpointed
as a step's return value, and it is persisted to S3. Both go through
`nested_payloads.codec`, so the checkpointed bytes and the stored bytes agree.
"""

import os
from typing import Any

import boto3
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.types import StepContext

from nested_payloads.models import Manifest, TrackedFile
from nested_payloads.serdes import NestedDataclassSerDes
from nested_payloads.store import ManifestStore

s3_client = boto3.client('s3')

MANIFEST_BUCKET = os.environ['MANIFEST_BUCKET']
MANIFEST_PREFIX = os.environ.get('MANIFEST_PREFIX', 'manifests/')
SOURCE_PREFIX = os.environ.get('SOURCE_PREFIX', 'incoming/')

store = ManifestStore(s3_client, MANIFEST_BUCKET, MANIFEST_PREFIX)

MANIFEST_SERDES = NestedDataclassSerDes(Manifest)


def list_source_files() -> list[TrackedFile]:
    tracked: list[TrackedFile] = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=MANIFEST_BUCKET, Prefix=SOURCE_PREFIX):
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


def merge(previous: Manifest | None, discovered: list[TrackedFile]) -> Manifest:
    """Union the previous manifest's files with what is on the prefix now."""
    by_key: dict[str, TrackedFile] = {f.key: f for f in (previous.files if previous else [])}
    for tracked in discovered:
        by_key[tracked.key] = tracked
    files = sorted(by_key.values(), key=lambda f: f.key)
    return Manifest(status='ready', bucket=MANIFEST_BUCKET, files=files)


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict[str, Any]:
    run_id = context.execution_context.durable_execution_arn.rsplit('/', 1)[-1]
    previous_key = event.get('previousManifestKey')

    def load_previous(step_context: StepContext) -> Manifest | None:
        if not previous_key:
            step_context.logger.info('no previous manifest')
            return None
        loaded = store.load(previous_key)
        step_context.logger.info('loaded %d file(s) from %s', len(loaded.files), previous_key)
        return loaded

    previous = context.step(
        load_previous,
        name='load_previous',
        config=StepConfig(serdes=MANIFEST_SERDES),
    )

    def discover(step_context: StepContext) -> Manifest:
        merged = merge(previous, list_source_files())
        step_context.logger.info('manifest holds %d file(s)', len(merged.files))
        return merged

    manifest = context.step(discover, name='discover', config=StepConfig(serdes=MANIFEST_SERDES))

    def persist(step_context: StepContext) -> str:
        key = store.save(run_id, manifest)
        step_context.logger.info('saved %s', key)
        return key

    manifest_key = context.step(persist, name='persist')
    return {
        'status': manifest.status,
        'files': len(manifest.files),
        'totalBytes': manifest.total_bytes,
        'manifestKey': manifest_key,
    }
