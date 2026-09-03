"""A checkout saga: hold the stock, charge the card, buy the label, refund if the stage fails.

Three side effects have three different safety properties, so they get three
different step configurations.

`reserve_stock` is keyed by order id, so running it twice writes the same row.
It keeps the AT_LEAST_ONCE default: the SDK checkpoints its start without
waiting, and an invocation lost mid-step simply runs it again.

`charge_card` has no such property. AT_MOST_ONCE_PER_RETRY makes the SDK
checkpoint the start synchronously, so an invocation lost mid-charge is
recognized on replay and the body is not re-entered. The step raises
StepInterruptedError into its retry strategy instead, which declines to retry
it — see `should_retry_charge`.

`charge_card` and `buy_label` sit in one child context, so the stage fails as a
unit and its FAIL checkpoint is what the parent sees on every later replay. The
parent catches that and compensates: it asks the processor whether the money
moved, refunds it if it did, and releases the stock either way.

The handler keeps the shape a Lambda reviewer expects: module-scope clients,
module-scope configuration, and `lambda_handler(event, context)` at module
level. Tests reach every seam with `monkeypatch.setattr`.
"""

import json
import os

import boto3
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.config import StepSemantics
from aws_durable_execution_sdk_python.exceptions import CallableRuntimeError
from aws_durable_execution_sdk_python.exceptions import StepInterruptedError
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.types import StepContext

from order_saga.logic import charge_idempotency_key
from order_saga.logic import order_total_cents
from order_saga.logic import parse_order
from order_saga.logic import reserved_quantities
from order_saga.logic import should_retry_charge

lambda_client = boto3.client('lambda')
dynamodb_client = boto3.client('dynamodb')

RESERVATION_TABLE = os.environ['RESERVATION_TABLE']
PAYMENTS_FUNCTION_NAME = os.environ['PAYMENTS_FUNCTION_NAME']
SHIPPING_FUNCTION_NAME = os.environ['SHIPPING_FUNCTION_NAME']
SHIPPING_SERVICE_LEVEL = os.environ.get('SHIPPING_SERVICE_LEVEL', 'ground')

CHARGE_SEMANTICS = StepSemantics.AT_MOST_ONCE_PER_RETRY
CHARGE_MAX_ATTEMPTS = 3
CHARGE_RETRY_DELAY_SECONDS = 5
LABEL_MAX_ATTEMPTS = 3
LABEL_RETRY_DELAY_SECONDS = 5


class ServiceCallFailed(RuntimeError):
    """A downstream service answered with an error."""


def call_service(function_name: str, action: str, body: dict) -> dict:
    response = lambda_client.invoke(
        FunctionName=function_name,
        Payload=json.dumps({'action': action, **body}).encode(),
    )
    answer = json.loads(response['Payload'].read())
    if 'errorMessage' in answer:
        raise ServiceCallFailed(f'{function_name} {action}: {answer["errorMessage"]}')
    return answer


class InventoryClient:
    """Holds stock against an order. The row is keyed by order id, so a second reserve is a no-op."""

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name

    def reserve(self, order_id: str, quantities: dict[str, int]) -> str:
        reservation_id = f'rsv-{order_id}'
        dynamodb_client.put_item(
            TableName=self.table_name,
            Item={
                'reservationId': {'S': reservation_id},
                'orderId': {'S': order_id},
                'quantities': {'M': {sku: {'N': str(n)} for sku, n in quantities.items()}},
            },
        )
        return reservation_id

    def release(self, reservation_id: str) -> None:
        dynamodb_client.delete_item(
            TableName=self.table_name,
            Key={'reservationId': {'S': reservation_id}},
        )


class PaymentsClient:
    def __init__(self, function_name: str) -> None:
        self.function_name = function_name

    def charge(self, idempotency_key: str, amount_cents: int, currency: str) -> str:
        answer = call_service(
            self.function_name,
            'charge',
            {'idempotencyKey': idempotency_key, 'amountCents': amount_cents, 'currency': currency},
        )
        return answer['chargeId']

    def refund_by_key(self, idempotency_key: str) -> str | None:
        """Refund whatever the processor holds under this key, and return None when it holds nothing.

        Compensation cannot see inside the failed stage, so it asks the
        processor rather than assuming either way.
        """
        found = call_service(self.function_name, 'findCharge', {'idempotencyKey': idempotency_key})
        charge_id = found.get('chargeId')
        if charge_id is None:
            return None
        return call_service(self.function_name, 'refund', {'chargeId': charge_id})['refundId']


class ShippingClient:
    def __init__(self, function_name: str) -> None:
        self.function_name = function_name

    def create_label(self, order_id: str, service_level: str) -> str:
        answer = call_service(
            self.function_name,
            'createLabel',
            {'orderId': order_id, 'serviceLevel': service_level},
        )
        return answer['trackingNumber']


inventory_client = InventoryClient(RESERVATION_TABLE)
payments_client = PaymentsClient(PAYMENTS_FUNCTION_NAME)
shipping_client = ShippingClient(SHIPPING_FUNCTION_NAME)


def charge_retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
    if should_retry_charge(
        interrupted=isinstance(error, StepInterruptedError),
        attempts_made=attempts_made,
        max_attempts=CHARGE_MAX_ATTEMPTS,
    ):
        return RetryDecision.retry(Duration.from_seconds(CHARGE_RETRY_DELAY_SECONDS))
    return RetryDecision.no_retry()


def label_retry_strategy(_error: Exception, attempts_made: int) -> RetryDecision:
    if attempts_made >= LABEL_MAX_ATTEMPTS:
        return RetryDecision.no_retry()
    return RetryDecision.retry(Duration.from_seconds(LABEL_RETRY_DELAY_SECONDS))


@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    order = parse_order(event)
    idempotency_key = charge_idempotency_key(order)

    def reserve_stock(step_context: StepContext) -> str:
        reservation_id = inventory_client.reserve(order.order_id, reserved_quantities(order))
        step_context.logger.info('reserved %s for %s', reservation_id, order.order_id)
        return reservation_id

    reservation_id = context.step(reserve_stock, name='reserve_stock')

    def fulfill(stage: DurableContext) -> dict:
        def charge_card(step_context: StepContext) -> str:
            charge_id = payments_client.charge(idempotency_key, order_total_cents(order), order.currency)
            step_context.logger.info('charged %s', charge_id)
            return charge_id

        charge_id = stage.step(
            charge_card,
            name='charge_card',
            config=StepConfig(
                step_semantics=CHARGE_SEMANTICS,
                retry_strategy=charge_retry_strategy,
            ),
        )

        def buy_label(step_context: StepContext) -> str:
            tracking_number = shipping_client.create_label(order.order_id, SHIPPING_SERVICE_LEVEL)
            step_context.logger.info('label %s', tracking_number)
            return tracking_number

        tracking_number = stage.step(
            buy_label,
            name='buy_label',
            config=StepConfig(retry_strategy=label_retry_strategy),
        )
        return {'chargeId': charge_id, 'trackingNumber': tracking_number}

    try:
        fulfillment = context.run_in_child_context(fulfill, name='fulfillment')
    # StepInterruptedError is deliberately not caught. It asks Lambda to retry the
    # invocation, and the stage's FAIL checkpoint reaches this handler as a
    # CallableRuntimeError on the replay that follows.
    except CallableRuntimeError as stage_failure:
        reason = str(stage_failure)

        def refund_charge(step_context: StepContext) -> str | None:
            refund_id = payments_client.refund_by_key(idempotency_key)
            step_context.logger.info('refund=%s after %s', refund_id, reason)
            return refund_id

        refund_id = context.step(refund_charge, name='refund_charge')

        def release_stock(step_context: StepContext) -> None:
            inventory_client.release(reservation_id)
            step_context.logger.info('released %s', reservation_id)

        context.step(release_stock, name='release_stock')
        return {
            'status': 'compensated',
            'orderId': order.order_id,
            'reason': reason,
            'refundId': refund_id,
        }

    return {
        'status': 'placed',
        'orderId': order.order_id,
        'reservationId': reservation_id,
        **fulfillment,
    }
