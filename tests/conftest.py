"""Environment and fakes.

The handler reads its configuration and builds its boto3 clients at import time,
which is the conventional Lambda shape. Both have to be satisfied before the
module is imported, so the environment is set here at collection time and the
clients are swapped per test with `monkeypatch.setattr`.
"""

import datetime as dt
import os

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-2')
os.environ.setdefault('LANDING_BUCKET', 'test-landing')
os.environ.setdefault('LANDING_PREFIX', 'landing/')
os.environ.setdefault('MANIFEST_PREFIX', 'manifests/')
os.environ.setdefault('INGEST_JOB_NAME', 'test-ingest-job')
os.environ.setdefault('AWS_LAMBDA_FUNCTION_NAME', 'landing-zone-trigger')
os.environ.setdefault('AWS_LAMBDA_FUNCTION_VERSION', '7')

import pytest  # noqa: E402

from landing_zone import handler as handler_module  # noqa: E402

EPOCH = dt.datetime(2026, 8, 18, 12, 0, tzinfo=dt.UTC)


class FakeS3:
    """Stands in for the S3 client, counting calls so replay can be asserted."""

    def __init__(self, pages: list[dict] | None = None) -> None:
        self.pages = pages if pages is not None else [{}]
        self.paginate_calls = 0
        self.put_objects: list[dict] = []

    def get_paginator(self, _operation: str) -> 'FakeS3':
        return self

    def paginate(self, **_kwargs) -> list[dict]:
        self.paginate_calls += 1
        return self.pages

    def put_object(self, **kwargs) -> dict:
        self.put_objects.append(kwargs)
        return {'ETag': '"fake"'}


class FakeGlue:
    def __init__(self) -> None:
        self.job_runs: list[dict] = []

    def start_job_run(self, **kwargs) -> dict:
        self.job_runs.append(kwargs)
        return {'JobRunId': f'jr_{len(self.job_runs)}'}


class FakeLambda:
    def __init__(self, running_arns: list[str] | None = None) -> None:
        self.running_arns = running_arns or []
        self.list_calls = 0

    def list_durable_executions_by_function(self, **_kwargs) -> dict:
        self.list_calls += 1
        return {'DurableExecutions': [{'DurableExecutionArn': a} for a in self.running_arns]}


def s3_page(entries: dict[str, tuple[int, int]]) -> dict:
    """Build one `list_objects_v2` page from key -> (size, age_in_seconds)."""
    return {
        'Contents': [
            {'Key': key, 'Size': size, 'LastModified': EPOCH - dt.timedelta(seconds=age)}
            for key, (size, age) in entries.items()
        ]
    }


def s3_event(*keys: str) -> dict:
    return {'Records': [{'s3': {'object': {'key': key}}} for key in keys]}


@pytest.fixture
def clients(monkeypatch):
    """Swap every module-scope client, freeze the clock, shrink the delays.

    The handler keeps its conventional shape because the seams are module
    attributes rather than constructor parameters.
    """
    s3, glue, lam = FakeS3(), FakeGlue(), FakeLambda()
    monkeypatch.setattr(handler_module, 's3_client', s3)
    monkeypatch.setattr(handler_module, 'glue_client', glue)
    monkeypatch.setattr(handler_module, 'lambda_client', lam)
    monkeypatch.setattr(handler_module.time, 'time', lambda: EPOCH.timestamp())
    monkeypatch.setattr(handler_module, 'QUIET_PERIOD_SECONDS', 300)
    monkeypatch.setattr(handler_module, 'POLL_DELAYS_SECONDS', (1,))
    monkeypatch.setattr(handler_module, 'STEADY_POLL_DELAY_SECONDS', 1)
    return s3, glue, lam
