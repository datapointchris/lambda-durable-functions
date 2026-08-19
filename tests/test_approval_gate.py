"""The approval gate, at both layers.

Layer 1 is the pure decisions, called with plain strings. Layer 2 drives the gate
through the real durable runtime: `run_async` starts the execution, `wait_for_callback`
blocks until it has suspended on its token, and only then can the answer be sent.
`runner.run` cannot express any of this — it blocks until the execution finishes,
which for a gate is never.
"""

import json

import pytest
from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from approval_gate.logic import (
    ChangeRequest,
    Decision,
    classify_gate_failure,
    parse_change_request,
    parse_decision,
    review_outcome_item,
)
from conftest_approval_gate import (
    CALLBACK_OPERATION_NAME,
    GATE_CONTEXT_NAME,
    SUBMITTER_STEP_NAME,
    approval,
    audit_row,
    beat_for,
    change_event,
    gate_runner,
    handler_module,
    install_fakes,
    open_gate,
    queued_token,
    reviewer_reads_it,
)

WINDOW_TIMEOUT_MESSAGE = 'Callback timed out: Callback.Timeout'
HEARTBEAT_TIMEOUT_MESSAGE = 'Callback heartbeat timed out: Callback.Heartbeat'


@pytest.fixture
def gate(monkeypatch):
    return install_fakes(monkeypatch)


def payload(result) -> dict:
    """The handler's return value. `result.result` is None on a failed execution."""
    assert result.result is not None, f'execution did not return: {result.error}'
    return json.loads(result.result)


def test_a_request_missing_a_field_the_reviewer_needs_is_refused():
    event = change_event()
    del event['statement']

    with pytest.raises(ValueError, match='statement'):
        parse_change_request(event)


def test_an_answer_naming_no_reviewer_is_not_an_approval():
    decision = parse_decision(json.dumps({'verdict': 'approved'}))

    assert decision.verdict == 'declined'
    assert decision.reason == 'unsigned-decision'


def test_an_empty_answer_is_not_an_approval():
    assert parse_decision(None).reason == 'unsigned-decision'
    assert parse_decision('not json at all').reason == 'unsigned-decision'


def test_a_signed_non_approval_is_recorded_as_a_rejection():
    decision = parse_decision(json.dumps({'verdict': 'rejected', 'reviewer': 'dana@example.com'}))

    assert decision.verdict == 'declined'
    assert decision.reason == 'reviewer-rejected'
    assert decision.reviewer == 'dana@example.com'


def test_a_silent_service_is_told_apart_from_a_lapsed_window():
    assert classify_gate_failure(HEARTBEAT_TIMEOUT_MESSAGE) == 'review-service-silent'
    assert classify_gate_failure(WINDOW_TIMEOUT_MESSAGE) == 'window-lapsed'
    assert classify_gate_failure('needs a backfill plan first') == 'reviewer-rejected'


def test_the_audit_row_carries_only_what_the_decision_supplied():
    request = ChangeRequest('CR-2041', 'warehouse.orders', 'ALTER TABLE ...', 'priya@example.com')

    lapsed = review_outcome_item(request, Decision('declined', reason='window-lapsed'), 1787000000)

    assert 'reviewer' not in lapsed
    assert lapsed['reason'] == {'S': 'window-lapsed'}
    assert lapsed['decidedAt'] == {'N': '1787000000'}


def test_an_approval_applies_the_change_and_records_the_reviewer(gate):
    _sqs, sfn, dynamodb = gate

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        runner.send_callback_success(callback_id, approval(note='ships with the backfill'))
        result = runner.wait_for_result(execution_arn, timeout=30)

    assert payload(result)['status'] == 'approved'
    assert payload(result)['reviewer'] == 'dana@example.com'
    assert sfn.executions[0]['name'] == 'CR-2041'
    assert audit_row(dynamodb)['verdict'] == {'S': 'approved'}
    assert audit_row(dynamodb)['note'] == {'S': 'ships with the backfill'}


def test_the_approval_service_is_handed_the_token_it_answers_with(gate):
    sqs, _sfn, _dynamodb = gate

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        token = queued_token(sqs)
        runner.send_callback_success(callback_id, approval())
        result = runner.wait_for_result(execution_arn, timeout=30)

    assert token == callback_id
    assert payload(result)['status'] == 'approved'


def test_a_rejection_records_the_outcome_and_starts_no_migration(gate):
    _sqs, sfn, dynamodb = gate

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        runner.send_callback_failure(callback_id, ErrorObject.from_message('needs a backfill plan first'))
        result = runner.wait_for_result(execution_arn, timeout=30)

    assert payload(result) == {
        'status': 'declined',
        'changeRequestId': 'CR-2041',
        'reason': 'reviewer-rejected',
    }
    assert sfn.executions == []
    assert audit_row(dynamodb)['verdict'] == {'S': 'declined'}


def test_a_window_that_closes_unanswered_declines_the_change(gate, monkeypatch):
    _sqs, sfn, _dynamodb = gate
    monkeypatch.setattr(handler_module, 'REVIEW_WINDOW_SECONDS', 1)

    with gate_runner() as runner:
        execution_arn, _callback_id = open_gate(runner, change_event())
        result = runner.wait_for_result(execution_arn, timeout=30)

    assert payload(result)['reason'] == 'window-lapsed'
    assert sfn.executions == []


def test_a_review_service_that_stops_beating_declines_before_the_window_closes(gate, monkeypatch):
    """The window is 20s and the run ends in about one, so only the heartbeat can
    have ended it."""
    _sqs, sfn, _dynamodb = gate
    monkeypatch.setattr(handler_module, 'REVIEW_SERVICE_HEARTBEAT_SECONDS', 1)

    with gate_runner() as runner:
        execution_arn, _callback_id = open_gate(runner, change_event())
        result = runner.wait_for_result(execution_arn, timeout=30)

    assert payload(result)['reason'] == 'review-service-silent'
    assert sfn.executions == []


def test_heartbeats_hold_the_gate_open_past_the_heartbeat_timeout(gate, monkeypatch):
    """The same 1s heartbeat as the test above, beaten for well over 1s."""
    _sqs, sfn, _dynamodb = gate
    monkeypatch.setattr(handler_module, 'REVIEW_SERVICE_HEARTBEAT_SECONDS', 1)

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        beats = beat_for(runner, callback_id, seconds=2.4, interval=0.25)
        runner.send_callback_success(callback_id, approval())
        result = runner.wait_for_result(execution_arn, timeout=30)

    assert beats >= 6
    assert payload(result)['status'] == 'approved'
    assert len(sfn.executions) == 1


def test_the_review_request_is_posted_exactly_once_despite_the_replay(gate):
    """A second queued message means the submitter escaped its step and replayed."""
    sqs, sfn, dynamodb = gate

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        reviewer_reads_it()
        runner.send_callback_success(callback_id, approval())
        runner.wait_for_result(execution_arn, timeout=30)

    assert len(sqs.messages) == 1
    assert len(sfn.executions) == 1
    assert len(dynamodb.items) == 1


def test_the_handler_body_really_does_re_enter(gate, monkeypatch):
    """Guards the test above: it only proves anything if a replay happened."""
    _sqs, _sfn, _dynamodb = gate
    entries: list[str] = []
    original = handler_module.parse_change_request

    def counting(event):
        entries.append(event['changeRequestId'])
        return original(event)

    monkeypatch.setattr(handler_module, 'parse_change_request', counting)

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        runner.send_callback_success(callback_id, approval())
        runner.wait_for_result(execution_arn, timeout=30)

    assert len(entries) > 1, 'the body ran once, so nothing was replayed'


def test_the_gate_decomposes_into_a_callback_and_a_submitter_step(gate):
    """`wait_for_callback` is a child context holding two operations, which is why
    the runner is asked for '<name> create callback id' rather than '<name>'."""
    _sqs, _sfn, _dynamodb = gate

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        runner.send_callback_success(callback_id, approval())
        result = runner.wait_for_result(execution_arn, timeout=30)

    assert [op.name for op in result.operations] == [GATE_CONTEXT_NAME, 'apply_change', 'record_outcome']
    gate_context = result.get_context(GATE_CONTEXT_NAME)
    assert [op.name for op in gate_context.child_operations] == [
        CALLBACK_OPERATION_NAME,
        SUBMITTER_STEP_NAME,
    ]
