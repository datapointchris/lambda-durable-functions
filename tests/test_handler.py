"""Layer 2: orchestration, through the real durable runtime in-process."""

import contextlib
import json

from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

from conftest import s3_event
from conftest import s3_page
from landing_zone import handler as handler_module


def run(event: dict):
    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        return runner.run(input=json.dumps(event), timeout=30)


def payload(result) -> dict:
    """The handler's return value. `result.result` is None on a failed execution."""
    assert result.result is not None, f'execution did not return: {result.error}'
    return json.loads(result.result)


def test_a_non_leader_execution_exits_without_listing_the_bucket(clients, monkeypatch):
    s3, glue, lam = clients
    monkeypatch.setattr(lam, 'running_arns', ['arn:aws:lambda:::an-older-execution'])

    result = run(s3_event('landing/part-0001.csv'))

    assert payload(result) == {'status': 'ignored'}
    assert s3.paginate_calls == 0
    assert glue.job_runs == []


def test_an_event_with_no_records_exits_before_polling(clients):
    s3, glue, _lam = clients

    result = run({'Records': []})

    assert payload(result) == {'status': 'no-trigger'}
    assert s3.paginate_calls == 0
    assert glue.job_runs == []


def test_a_settled_drop_writes_a_manifest_and_starts_one_job(clients):
    s3, glue, _lam = clients
    s3.pages = [s3_page({'landing/part-0001.csv': (120, 600), 'landing/part-0002.csv': (80, 600)})]

    result = run(s3_event('landing/part-0001.csv'))

    assert payload(result) == {'status': 'ingested', 'objects': 2, 'jobRunId': 'jr_1'}
    assert len(glue.job_runs) == 1
    assert glue.job_runs[0]['JobName'] == 'test-ingest-job'


def test_the_manifest_lists_every_landed_key(clients):
    s3, _glue, _lam = clients
    s3.pages = [s3_page({'landing/part-0002.csv': (80, 600), 'landing/part-0001.csv': (120, 600)})]

    run(s3_event('landing/part-0001.csv'))

    body = s3.put_objects[0]['Body'].decode()
    assert body.splitlines() == ['landing/part-0001.csv', 'landing/part-0002.csv']


def test_the_ingest_job_is_pointed_at_the_manifest_not_the_prefix(clients):
    s3, glue, _lam = clients
    s3.pages = [s3_page({'landing/part-0001.csv': (120, 600)})]

    run(s3_event('landing/part-0001.csv'))

    manifest_key = s3.put_objects[0]['Key']
    assert manifest_key.startswith('manifests/')
    assert glue.job_runs[0]['Arguments']['--manifest'].endswith(manifest_key)


def test_the_operation_history_is_leader_settle_freeze_publish(clients):
    s3, _glue, _lam = clients
    s3.pages = [s3_page({'landing/part-0001.csv': (120, 600)})]

    result = run(s3_event('landing/part-0001.csv'))

    assert [op.name for op in result.operations] == [
        'leader',
        'settle',
        'freeze_manifest',
        'publish_and_start',
    ]


def test_a_directory_placeholder_is_not_treated_as_an_object(clients):
    s3, _glue, _lam = clients
    s3.pages = [s3_page({'landing/': (0, 600), 'landing/part-0001.csv': (120, 600)})]

    result = run(s3_event('landing/part-0001.csv'))

    assert payload(result)['objects'] == 1


def test_an_empty_landing_prefix_never_settles(clients):
    """Nothing to ingest is not a finished drop, so it keeps waiting rather than
    ingesting an empty manifest.

    A stuck execution surfaces either as a FAILED result or as a raised
    TimeoutError, depending on whether the execution timeout or the runner's
    wait expires first. Both mean the same thing, so assert on the side effect.
    """
    _s3, glue, _lam = clients

    with (
        contextlib.suppress(TimeoutError),
        DurableFunctionTestRunner(handler_module.lambda_handler) as runner,
    ):
        runner.run(input=json.dumps(s3_event('landing/gone.csv')), timeout=5)

    assert glue.job_runs == []
