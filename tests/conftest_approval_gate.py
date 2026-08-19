"""Environment, fakes and callback helpers for the approval gate.

The handler reads its configuration and builds its boto3 clients at import time,
which is the conventional Lambda shape. Both have to be satisfied before the
module is imported, so the environment is set here at import time and the clients
are swapped per test with `monkeypatch.setattr`.
"""

import json
import os
import time

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-2')
os.environ.setdefault('REVIEW_QUEUE_URL', 'https://sqs.us-east-2.amazonaws.com/123456789012/change-review')
os.environ.setdefault('CHANGE_AUDIT_TABLE', 'change-request-audit')
os.environ.setdefault(
    'MIGRATION_STATE_MACHINE_ARN',
    'arn:aws:states:us-east-2:123456789012:stateMachine:schema-migration',
)

from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner  # noqa: E402

from approval_gate import handler as handler_module  # noqa: E402

# `wait_for_callback(name='review')` does not name the callback 'review'. It opens
# a child context under that name and mints the callback inside it, suffixed. The
# runner matches on the operation name, so this is the string it needs.
CALLBACK_OPERATION_NAME = 'review create callback id'

GATE_CONTEXT_NAME = 'review'
SUBMITTER_STEP_NAME = 'review submitter'


class FakeSqs:
    """Stands in for the SQS client, keeping every message so replay can be asserted."""

    def __init__(self) -> None:
        self.messages: list[dict] = []

    def send_message(self, **kwargs) -> dict:
        self.messages.append(kwargs)
        return {'MessageId': f'msg-{len(self.messages)}'}


class FakeStepFunctions:
    def __init__(self) -> None:
        self.executions: list[dict] = []

    def start_execution(self, **kwargs) -> dict:
        self.executions.append(kwargs)
        arn = f'arn:aws:states:us-east-2:123456789012:execution/schema-migration/{kwargs["name"]}'
        return {'executionArn': arn}


class FakeDynamoDb:
    def __init__(self) -> None:
        self.items: list[dict] = []

    def put_item(self, **kwargs) -> dict:
        self.items.append(kwargs)
        return {}


def change_event(request_id: str = 'CR-2041', target_table: str = 'warehouse.orders') -> dict:
    return {
        'changeRequestId': request_id,
        'targetTable': target_table,
        'statement': f'ALTER TABLE {target_table} ADD COLUMN fulfilment_centre VARCHAR',
        'requestedBy': 'priya@example.com',
    }


def approval(reviewer: str = 'dana@example.com', note: str = '') -> bytes:
    """The body the approval service sends back with the token."""
    return json.dumps({'verdict': 'approved', 'reviewer': reviewer, 'note': note}).encode()


def gate_runner() -> DurableFunctionTestRunner:
    """The runner polls the history for the callback id, and its 1s default
    interval is longer than everything else these tests do put together."""
    return DurableFunctionTestRunner(handler_module.lambda_handler, poll_interval=0.05)


def open_gate(runner, event: dict, timeout: int = 10) -> tuple[str, str]:
    """Start the execution and block until it is suspended on its callback."""
    execution_arn = runner.run_async(input=json.dumps(event), timeout=30)
    callback_id = runner.wait_for_callback(execution_arn, name=CALLBACK_OPERATION_NAME, timeout=timeout)
    return execution_arn, callback_id


def queued_token(sqs: FakeSqs) -> str:
    """The token the submitter posted, read back the way the approval service reads it."""
    return json.loads(sqs.messages[0]['MessageBody'])['callbackToken']


def reviewer_reads_it() -> None:
    """A person takes longer than a millisecond to answer.

    The submitter step's SUCCEED is flushed by a background thread, and an answer
    landing inside that window is rejected for a stale checkpoint token — after
    which the replay finds no submitter checkpoint and posts the request again.
    That is at-least-once step semantics behaving as specified, and any scenario
    with a human in it clears the window by orders of magnitude.
    """
    time.sleep(0.3)


def beat_for(runner, callback_id: str, seconds: float, interval: float = 0.4) -> int:
    """Heartbeat the callback for a stretch of wall-clock, returning the beat count."""
    beats = 0
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        time.sleep(interval)
        runner.send_callback_heartbeat(callback_id)
        beats += 1
    return beats


def audit_row(dynamodb: FakeDynamoDb) -> dict:
    return dynamodb.items[0]['Item']


def install_fakes(monkeypatch) -> tuple[FakeSqs, FakeStepFunctions, FakeDynamoDb]:
    """Swap every module-scope client and shrink the two clocks.

    The window is a backstop rather than a subject: a test that means to exercise
    it shortens it further. The heartbeat starts disabled because a zero duration
    is what tells the runtime not to schedule that timer at all.
    """
    sqs, sfn, dynamodb = FakeSqs(), FakeStepFunctions(), FakeDynamoDb()
    monkeypatch.setattr(handler_module, 'sqs_client', sqs)
    monkeypatch.setattr(handler_module, 'sfn_client', sfn)
    monkeypatch.setattr(handler_module, 'dynamodb_client', dynamodb)
    monkeypatch.setattr(handler_module, 'REVIEW_WINDOW_SECONDS', 20)
    monkeypatch.setattr(handler_module, 'REVIEW_SERVICE_HEARTBEAT_SECONDS', 0)
    return sqs, sfn, dynamodb
