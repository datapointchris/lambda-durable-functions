"""Three parallel extracts and a chained handoff, at three layers.

Layer 1 is the pure decisions with no SDK. Layer 2 is the orchestration through
the real durable runtime in-process. Layer 3 is replay: a side effect that fires
twice means it escaped its step.
"""

import json

import pytest
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

from conftest_pipeline_chain import RUN_DATE
from conftest_pipeline_chain import RecordingInvokeProcessor
from conftest_pipeline_chain import SchemaMismatch
from conftest_pipeline_chain import Sources
from conftest_pipeline_chain import ThrottlingException
from conftest_pipeline_chain import register_load_function
from conftest_pipeline_chain import swap_sources
from pipeline_chain import handler as handler_module
from pipeline_chain.logic import DATASETS
from pipeline_chain.logic import build_load_payload
from pipeline_chain.logic import extract_summary
from pipeline_chain.logic import is_transient
from pipeline_chain.logic import run_date_from_event
from pipeline_chain.logic import staging_key


@pytest.fixture
def sources(monkeypatch) -> Sources:
    return swap_sources(monkeypatch)


@pytest.fixture
def handoff(monkeypatch) -> RecordingInvokeProcessor:
    return register_load_function(monkeypatch)


def run(run_date: str = RUN_DATE):
    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        return runner.run(input=json.dumps({'runDate': run_date}), timeout=30)


def payload(result) -> dict:
    """The handler's return value. `result.result` is None on a failed execution."""
    assert result.result is not None, f'execution did not return: {result.error}'
    return json.loads(result.result)


def summaries(*datasets: str) -> list[dict]:
    return [extract_summary(name, f'staging/{name}.jsonl', 10) for name in datasets]


def test_every_expected_dataset_present_makes_the_load_full():
    built = build_load_payload(RUN_DATE, summaries(*DATASETS), DATASETS)

    assert built['mode'] == 'full'
    assert built['missing'] == []


def test_a_dataset_that_never_staged_makes_the_load_partial():
    built = build_load_payload(RUN_DATE, summaries('orders', 'clickstream'), DATASETS)

    assert built['mode'] == 'partial'
    assert built['missing'] == ['inventory']
    assert list(built['datasets']) == ['orders', 'clickstream']


def test_the_payload_totals_the_rows_every_branch_staged():
    extracts = [
        extract_summary('orders', 'staging/orders.jsonl', 2),
        extract_summary('clickstream', 'staging/clickstream.jsonl', 3),
    ]

    assert build_load_payload(RUN_DATE, extracts, DATASETS)['rowCount'] == 5


def test_a_throttle_is_transient_and_a_schema_mismatch_is_not():
    assert is_transient('ThrottlingException')
    assert not is_transient('SchemaMismatch')


def test_the_staging_key_partitions_by_dataset_and_run_date():
    assert staging_key('staging/', 'orders', RUN_DATE) == (f'staging/orders/dt={RUN_DATE}/part-0000.jsonl')


def test_a_run_date_that_is_not_a_date_is_rejected():
    with pytest.raises(ValueError, match='runDate'):
        run_date_from_event({'runDate': None})


def test_a_compact_run_date_is_normalised_before_any_key_is_built():
    assert run_date_from_event({'runDate': '20260818'}) == RUN_DATE


@pytest.mark.usefixtures('handoff')
def test_a_malformed_schedule_fails_before_any_source_is_touched(sources: Sources):
    result = run('the-eighteenth')

    assert result.result is None
    assert sources.rds.statements == []
    assert sources.s3.paginate_calls == 0


@pytest.mark.usefixtures('handoff')
def test_all_three_sources_are_extracted_and_the_load_swaps_the_tables(sources: Sources):
    result = payload(run())

    assert sources.rds.statements and sources.s3.paginate_calls and sources.dynamodb.queries
    assert result['mode'] == 'full'
    assert result['load'] == {'loadRunId': f'load-{RUN_DATE}', 'tablesSwapped': True}


@pytest.mark.usefixtures('sources', 'handoff')
def test_the_execution_is_one_parallel_operation_then_one_handoff():
    result = run()

    assert [operation.name for operation in result.operations] == ['extract_sources', 'handoff']


@pytest.mark.usefixtures('sources', 'handoff')
def test_each_branch_is_its_own_context_nested_under_the_parallel_operation():
    """Branch names come from `durable_parallel_branch` and `ParallelBranch`."""
    result = run()

    parallel = result.get_context('extract_sources')

    assert [branch.name for branch in parallel.child_operations] == [
        'extract-orders',
        'extract-clickstream',
        'extract-inventory',
    ]


@pytest.mark.usefixtures('sources', 'handoff')
def test_a_branch_holds_its_own_steps_one_level_further_down():
    result = run()

    parallel = result.get_context('extract_sources')
    orders = parallel.get_context('extract-orders')
    inventory = parallel.get_context('extract-inventory')

    assert [step.name for step in orders.child_operations] == ['read_orders', 'stage_orders']
    assert [step.name for step in inventory.child_operations] == ['snapshot_inventory']


@pytest.mark.usefixtures('handoff')
def test_max_concurrency_holds_the_third_extract_back(sources: Sources):
    run()

    assert sources.probe.peak == handler_module.MAX_CONCURRENT_EXTRACTS


@pytest.mark.usefixtures('handoff')
def test_a_throttled_source_is_retried_inside_its_own_branch(sources: Sources):
    sources.dynamodb.failures.pending = [ThrottlingException('slow down')]

    result = payload(run())

    assert len(sources.dynamodb.queries) == 2
    assert result['mode'] == 'full'


@pytest.mark.usefixtures('handoff')
def test_a_permanent_source_failure_is_not_retried(sources: Sources):
    sources.dynamodb.failures.pending = [SchemaMismatch('snapshot_date is now a number')]

    run()

    assert len(sources.dynamodb.queries) == 1


@pytest.mark.usefixtures('handoff')
def test_one_dead_source_is_tolerated_and_the_load_runs_in_partial_mode(sources: Sources):
    sources.dynamodb.failures.pending = [SchemaMismatch('snapshot_date is now a number')]

    result = payload(run())

    assert result['mode'] == 'partial'
    assert result['load']['tablesSwapped'] is False


def test_the_handoff_names_the_load_function_and_carries_the_staged_keys(sources: Sources, handoff: RecordingInvokeProcessor):
    run()

    function_name, sent = handoff.calls[0]

    assert function_name == handler_module.LOAD_FUNCTION_NAME
    assert sent['runDate'] == RUN_DATE
    assert sent['datasets']['orders'] == staging_key('staging/', 'orders', RUN_DATE)
    assert sent['rowCount'] == len(sources.rds.rows) + len(sources.s3.keys) + len(sources.dynamodb.items)


def test_two_dead_sources_exceed_the_tolerance_and_nothing_is_handed_off(sources: Sources, handoff: RecordingInvokeProcessor):
    """The batch reports the breach rather than raising, so the handler acts on it."""
    sources.dynamodb.failures.pending = [SchemaMismatch('snapshot_date is now a number')]
    sources.rds.failures.pending = [SchemaMismatch('total_cents was dropped')]

    result = payload(run())

    assert result == {'runDate': RUN_DATE, 'mode': 'abandoned', 'missing': ['orders', 'inventory']}
    assert handoff.calls == []


@pytest.mark.usefixtures('handoff')
def test_each_source_is_read_exactly_once_despite_the_replay(sources: Sources):
    """A second read means a side effect escaped its step."""
    sources.dynamodb.failures.pending = [ThrottlingException('slow down')]

    run()

    assert len(sources.rds.statements) == 1
    assert sources.s3.paginate_calls == 1


@pytest.mark.usefixtures('handoff')
def test_every_dataset_is_staged_exactly_once(sources: Sources):
    sources.dynamodb.failures.pending = [ThrottlingException('slow down')]

    run()

    assert sources.s3.staged_keys() == [staging_key('staging/', dataset, RUN_DATE) for dataset in sorted(DATASETS)]


@pytest.mark.usefixtures('handoff')
def test_the_handler_body_really_does_re_enter(sources: Sources, monkeypatch):
    """Guards the two tests above: they prove nothing without a replay."""
    entries: list[str] = []
    original = handler_module.run_date_from_event

    def counting(event: dict) -> str:
        entries.append('body')
        return original(event)

    monkeypatch.setattr(handler_module, 'run_date_from_event', counting)
    sources.dynamodb.failures.pending = [ThrottlingException('slow down')]

    run()

    assert len(entries) > 1, 'the handler body ran once, so nothing was replayed'
