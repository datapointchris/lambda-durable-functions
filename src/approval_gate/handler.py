"""A change-request approval gate: post for review, suspend, then apply or decline.

A proposed change to a production table is not applied until a person answers for
it. The gate posts the request to the approval service's queue with a callback
token and suspends. The service answers with that token: success carries the
reviewer's verdict, failure carries their reason for refusing.

Two clocks bound the wait, and they watch different things. `timeout` is the
review window — how long a change may sit unanswered before the gate closes
itself. `heartbeat_timeout` is far shorter and watches the approval service: it
beats while the request is still open in its queue, so a service that dies is
noticed in minutes instead of holding the change open for the whole window.

The handler keeps the shape a Lambda reviewer expects: module-scope clients,
module-scope configuration, and `lambda_handler(event, context)` at module level.
Tests reach every seam with `monkeypatch.setattr` — see tests/conftest_approval_gate.py.
"""

import json
import os
import time

import boto3
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig
from aws_durable_execution_sdk_python.exceptions import CallableRuntimeError
from aws_durable_execution_sdk_python.types import StepContext, WaitForCallbackContext

from approval_gate.logic import (
    APPROVED,
    DECLINED,
    ChangeRequest,
    Decision,
    build_review_message,
    classify_gate_failure,
    parse_change_request,
    parse_decision,
    review_outcome_item,
)

sqs_client = boto3.client('sqs')
sfn_client = boto3.client('stepfunctions')
dynamodb_client = boto3.client('dynamodb')

REVIEW_QUEUE_URL = os.environ['REVIEW_QUEUE_URL']
CHANGE_AUDIT_TABLE = os.environ['CHANGE_AUDIT_TABLE']
MIGRATION_STATE_MACHINE_ARN = os.environ['MIGRATION_STATE_MACHINE_ARN']

REVIEW_WINDOW_SECONDS = 4 * 60 * 60
REVIEW_SERVICE_HEARTBEAT_SECONDS = 15 * 60


def post_review_request(message: dict) -> str:
    """Hand the request, and the token that answers it, to the approval service."""
    response = sqs_client.send_message(QueueUrl=REVIEW_QUEUE_URL, MessageBody=json.dumps(message))
    return response['MessageId']


def start_migration(request: ChangeRequest) -> str:
    """Run the approved statement. The request id names the execution, so a step
    replayed under at-least-once semantics collides instead of migrating twice."""
    response = sfn_client.start_execution(
        stateMachineArn=MIGRATION_STATE_MACHINE_ARN,
        name=request.request_id,
        input=json.dumps({'targetTable': request.target_table, 'statement': request.statement}),
    )
    return response['executionArn']


def record_outcome(item: dict) -> None:
    dynamodb_client.put_item(TableName=CHANGE_AUDIT_TABLE, Item=item)


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    change_request = parse_change_request(event)

    def request_review(callback_token: str, callback_context: WaitForCallbackContext) -> None:
        message_id = post_review_request(build_review_message(change_request, callback_token))
        callback_context.logger.info('review of %s queued as %s', change_request.request_id, message_id)

    try:
        answer = context.wait_for_callback(
            request_review,
            name='review',
            config=WaitForCallbackConfig(
                timeout=Duration.from_seconds(REVIEW_WINDOW_SECONDS),
                heartbeat_timeout=Duration.from_seconds(REVIEW_SERVICE_HEARTBEAT_SECONDS),
            ),
        )
    except CallableRuntimeError as gate_failure:
        decision = Decision(verdict=DECLINED, reason=classify_gate_failure(gate_failure.message))
    else:
        decision = parse_decision(answer)

    def write_audit_row(step_context: StepContext) -> None:
        record_outcome(review_outcome_item(change_request, decision, time.time()))
        step_context.logger.info('%s recorded as %s', change_request.request_id, decision.verdict)

    if decision.verdict != APPROVED:
        context.step(write_audit_row, name='record_outcome')
        return {
            'status': DECLINED,
            'changeRequestId': change_request.request_id,
            'reason': decision.reason,
        }

    def apply_change(step_context: StepContext) -> str:
        execution_arn = start_migration(change_request)
        step_context.logger.info('migration of %s started: %s', change_request.target_table, execution_arn)
        return execution_arn

    migration_execution_arn = context.step(apply_change, name='apply_change')
    context.step(write_audit_row, name='record_outcome')
    return {
        'status': APPROVED,
        'changeRequestId': change_request.request_id,
        'reviewer': decision.reviewer,
        'migrationExecutionArn': migration_execution_arn,
    }
