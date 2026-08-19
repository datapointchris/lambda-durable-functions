"""The checkout saga: pure decisions, then orchestration through the real runtime."""

import json

import pytest
from aws_durable_execution_sdk_python.config import StepSemantics
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

from conftest_order_saga import carrier_outage, checkout_event, install_fakes
from order_saga import handler as handler_module
from order_saga.logic import (
    charge_idempotency_key,
    order_total_cents,
    parse_order,
    reserved_quantities,
    should_retry_charge,
)


@pytest.fixture
def saga_clients(monkeypatch):
    return install_fakes(monkeypatch)


def run(event: dict):
    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        return runner.run(input=json.dumps(event), timeout=30)


def payload(result) -> dict:
    """The handler's return value. `result.result` is None on a failed execution."""
    assert result.result is not None, f'execution did not return: {result.error}'
    return json.loads(result.result)


def test_two_lines_of_the_same_sku_become_one_reservation():
    order = parse_order(checkout_event(('SKU-MUG-350', 2, 900), ('SKU-MUG-350', 3, 900)))

    assert reserved_quantities(order) == {'SKU-MUG-350': 5}


def test_the_order_total_is_the_sum_of_every_line():
    order = parse_order(checkout_event(('SKU-KETTLE-1L', 1, 4999), ('SKU-FILTER-6PK', 2, 1250)))

    assert order_total_cents(order) == 7499


def test_the_idempotency_key_depends_only_on_the_order():
    """A key that varied per attempt would turn the processor's own dedupe off."""
    first = parse_order(checkout_event(('SKU-MUG-350', 1, 900), order_id='ord-9001'))
    second = parse_order(checkout_event(('SKU-KETTLE-1L', 4, 4999), order_id='ord-9001'))

    assert charge_idempotency_key(first) == charge_idempotency_key(second)


def test_an_interrupted_charge_is_not_retried_even_on_the_first_attempt():
    assert not should_retry_charge(interrupted=True, attempts_made=1, max_attempts=3)


def test_a_charge_that_failed_outright_is_retried_up_to_the_attempt_limit():
    decisions = [
        should_retry_charge(interrupted=False, attempts_made=attempt, max_attempts=3) for attempt in (1, 2, 3)
    ]

    assert decisions == [True, True, False]


def test_a_clean_checkout_reserves_charges_and_labels_once(saga_clients):
    payments, inventory, shipping = saga_clients

    result = run(checkout_event())

    assert payload(result)['status'] == 'placed'
    assert len(inventory.reservations) == 1
    assert len(payments.charge_attempts) == 1
    assert len(shipping.labels) == 1


def test_the_charge_is_the_basket_total(saga_clients):
    payments, _inventory, _shipping = saga_clients

    run(checkout_event())

    assert payments.charge_attempts[0]['amountCents'] == 7499


@pytest.mark.usefixtures('saga_clients')
def test_the_fulfilment_stage_groups_the_charge_and_the_label():
    """The stage is one operation to the parent, which is what lets it fail as a unit."""
    result = run(checkout_event())

    stage = result.get_context('fulfilment')
    assert [op.name for op in result.operations] == ['reserve_stock', 'fulfilment']
    assert [op.name for op in stage.child_operations] == ['charge_card', 'buy_label']


def test_a_carrier_outage_refunds_the_charge_and_releases_the_stock(saga_clients, monkeypatch):
    payments, inventory, shipping = saga_clients
    shipping.fail_next_label = carrier_outage()
    monkeypatch.setattr(handler_module, 'LABEL_MAX_ATTEMPTS', 1)

    result = run(checkout_event())

    body = payload(result)
    assert body['status'] == 'compensated'
    assert 'carrier api unavailable' in body['reason']
    assert payments.refunds == ['ch_1']
    assert inventory.releases == ['rsv-ord-4417']


def test_a_retried_label_does_not_charge_the_card_again(saga_clients):
    """A step retry replays the whole stage body. The charge returns its checkpoint instead."""
    payments, _inventory, shipping = saga_clients
    shipping.fail_next_label = carrier_outage()

    result = run(checkout_event())

    stage = result.get_context('fulfilment')
    assert stage.get_step('charge_card').attempt == 1
    assert stage.get_step('buy_label').attempt == 2
    assert payload(result)['status'] == 'placed'
    assert len(payments.charge_attempts) == 1
    assert len(shipping.labels) == 1


def test_an_interrupted_charge_runs_its_body_exactly_once(saga_clients):
    """The whole point of AT_MOST_ONCE_PER_RETRY.

    The processor took the money and the invocation died before the step could
    checkpoint. The next invocation finds a STARTED checkpoint and refuses to
    re-enter the body, so the card is charged once rather than twice.
    """
    payments, _inventory, _shipping = saga_clients
    payments.interrupt_next_charge = True

    run(checkout_event())

    assert len(payments.charge_attempts) == 1


def test_an_interrupted_charge_is_refunded_rather_than_re_attempted(saga_clients):
    payments, inventory, shipping = saga_clients
    payments.interrupt_next_charge = True

    result = run(checkout_event())

    body = payload(result)
    assert body['status'] == 'compensated'
    assert body['refundId'] == 'rf_1'
    assert payments.refunds == ['ch_1']
    assert inventory.releases == ['rsv-ord-4417']
    assert shipping.labels == []


def test_the_default_semantics_charge_the_card_twice(saga_clients, monkeypatch):
    """Why the charge step is configured at all.

    The same interruption, with the AT_LEAST_ONCE default, re-enters the step
    body on replay. A processor that did not dedupe on the idempotency key would
    take the money a second time.
    """
    payments, _inventory, _shipping = saga_clients
    monkeypatch.setattr(handler_module, 'CHARGE_SEMANTICS', StepSemantics.AT_LEAST_ONCE_PER_RETRY)
    payments.interrupt_next_charge = True

    result = run(checkout_event())

    assert payload(result)['status'] == 'placed'
    assert len(payments.charge_attempts) == 2


def test_the_reservation_is_written_once_despite_the_replays(saga_clients):
    """A step that already succeeded returns its checkpoint, however often the body re-enters."""
    payments, inventory, _shipping = saga_clients
    payments.interrupt_next_charge = True

    run(checkout_event())

    assert len(inventory.reservations) == 1


def test_the_handler_body_really_does_re_enter(saga_clients, monkeypatch):
    """Guards the two tests above: they only prove anything if a replay happened."""
    payments, _inventory, _shipping = saga_clients
    entries: list[str] = []
    original = handler_module.parse_order

    def counting(event: dict):
        entries.append(event['orderId'])
        return original(event)

    monkeypatch.setattr(handler_module, 'parse_order', counting)
    payments.interrupt_next_charge = True

    run(checkout_event())

    assert len(entries) > 1, 'the interrupted charge never replayed the handler body'


def test_a_retried_charge_reuses_the_idempotency_key(saga_clients):
    """A charge that failed outright never reached the processor, so retrying it is safe."""
    payments, _inventory, shipping = saga_clients
    payments.fail_next_charge = handler_module.ServiceCallFailed('test-payments charge: gateway 503')

    result = run(checkout_event())

    assert payload(result)['status'] == 'placed'
    assert [attempt['key'] for attempt in payments.charge_attempts] == [
        'checkout-ord-4417',
        'checkout-ord-4417',
    ]
    assert len(shipping.labels) == 1
