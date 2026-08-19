"""The failure mode unique to durable functions: a side effect outside a step.

The handler body re-runs from the top after every suspension. A step body does
not, because a succeeded checkpoint short-circuits it. So the assertion that
matters is a call count on a fake, never a return value.
"""

import json

import pytest
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

from conftest import s3_event, s3_page
from landing_zone import handler as handler_module

SETTLED = {'landing/part-0001.csv': (120, 600), 'landing/part-0002.csv': (80, 600)}


def run_settled(s3):
    s3.pages = [s3_page(SETTLED)]
    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        return runner.run(input=json.dumps(s3_event('landing/part-0001.csv')), timeout=30)


def test_the_ingest_job_starts_exactly_once(clients):
    """A second job run means a side effect escaped its step and replayed."""
    s3, glue, _lam = clients

    run_settled(s3)

    assert len(glue.job_runs) == 1


def test_the_manifest_is_written_exactly_once(clients):
    s3, _glue, _lam = clients

    run_settled(s3)

    assert len(s3.put_objects) == 1


def test_the_leader_check_runs_once_despite_the_replay(clients):
    s3, _glue, lam = clients

    run_settled(s3)

    assert lam.list_calls == 1


def test_the_handler_body_really_does_re_enter(clients, monkeypatch):
    """Guards the tests above: they only prove anything if a replay happened."""
    s3, _glue, _lam = clients
    entries: list[str] = []
    original = handler_module.is_quiet

    def counting(*args, **kwargs):
        entries.append('body')
        return original(*args, **kwargs)

    monkeypatch.setattr(handler_module, 'is_quiet', counting)

    run_settled(s3)

    assert entries, 'the settle check never ran, so nothing was replayed'


@pytest.mark.usefixtures('clients')
def test_a_replayed_step_returns_its_checkpoint_rather_than_re_running(clients):
    s3, _glue, _lam = clients

    result = run_settled(s3)

    # One listing for the settle check, one for freeze_manifest. A third would
    # mean a checkpointed step body executed again on replay.
    assert s3.paginate_calls == 2
    assert result.result is not None
    assert json.loads(result.result)["objects"] == 2
