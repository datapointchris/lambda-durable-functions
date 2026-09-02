"""Fakes and wiring for the checkout saga.

The handler reads its configuration and builds its clients at import time, which
is the conventional Lambda shape. Both have to be satisfied before the module is
imported, so the environment is set here and the clients are swapped per test
with `monkeypatch.setattr`.

`FakePayments.interrupt_next_charge` is the interesting one. It records the
charge and then raises `TimedSuspendExecution`, which the SDK treats as a
suspension rather than a step failure: the step keeps its STARTED checkpoint and
nothing checkpoints an outcome. That is what a Lambda invocation dying mid-step
looks like to the next invocation. Testing 1.2.1 has no fault injection, so this
is the lever that reaches the AT_MOST_ONCE_PER_RETRY branch.
"""

import os

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-2')
os.environ.setdefault('RESERVATION_TABLE', 'test-reservations')
os.environ.setdefault('PAYMENTS_FUNCTION_NAME', 'test-payments')
os.environ.setdefault('SHIPPING_FUNCTION_NAME', 'test-shipping')
os.environ.setdefault('SHIPPING_SERVICE_LEVEL', 'ground')

from aws_durable_execution_sdk_python.exceptions import TimedSuspendExecution  # noqa: E402
from aws_durable_execution_sdk_python_testing.executor import Executor  # noqa: E402

from order_saga import handler as handler_module  # noqa: E402

BASKET = (('SKU-KETTLE-1L', 1, 4999), ('SKU-FILTER-6PK', 2, 1250))
INTERRUPT_RESUME_SECONDS = 1


class FakePayments:
    """The payment processor, holding one charge per idempotency key."""

    def __init__(self) -> None:
        self.charge_attempts: list[dict] = []
        self.charges: dict[str, str] = {}
        self.refunds: list[str] = []
        self.interrupt_next_charge = False
        self.fail_next_charge: Exception | None = None

    def charge(self, idempotency_key: str, amount_cents: int, currency: str) -> str:
        self.charge_attempts.append({'key': idempotency_key, 'amountCents': amount_cents, 'currency': currency})
        if self.fail_next_charge is not None:
            error, self.fail_next_charge = self.fail_next_charge, None
            raise error
        charge_id = self.charges.setdefault(idempotency_key, f'ch_{len(self.charges) + 1}')
        if self.interrupt_next_charge:
            self.interrupt_next_charge = False
            raise TimedSuspendExecution.from_delay('invocation lost after the processor took the money', INTERRUPT_RESUME_SECONDS)
        return charge_id

    def refund_by_key(self, idempotency_key: str) -> str | None:
        charge_id = self.charges.get(idempotency_key)
        if charge_id is None:
            return None
        self.refunds.append(charge_id)
        return f'rf_{len(self.refunds)}'


class FakeInventory:
    def __init__(self) -> None:
        self.reservations: list[tuple[str, dict[str, int]]] = []
        self.releases: list[str] = []

    def reserve(self, order_id: str, quantities: dict[str, int]) -> str:
        self.reservations.append((order_id, dict(quantities)))
        return f'rsv-{order_id}'

    def release(self, reservation_id: str) -> None:
        self.releases.append(reservation_id)


class FakeShipping:
    def __init__(self) -> None:
        self.labels: list[tuple[str, str]] = []
        self.fail_next_label: Exception | None = None

    def create_label(self, order_id: str, service_level: str) -> str:
        if self.fail_next_label is not None:
            error, self.fail_next_label = self.fail_next_label, None
            raise error
        self.labels.append((order_id, service_level))
        return f'1Z{len(self.labels):09d}'


def checkout_event(*lines: tuple[str, int, int], order_id: str = 'ord-4417') -> dict:
    """Build a checkout payload from (sku, quantity, unit_price_cents) triples."""
    return {
        'orderId': order_id,
        'customerId': 'cus-8802',
        'currency': 'USD',
        'lines': [
            {'sku': sku, 'quantity': quantity, 'unitPriceCents': unit_price_cents} for sku, quantity, unit_price_cents in (lines or BASKET)
        ],
    }


def carrier_outage() -> Exception:
    return handler_module.ServiceCallFailed('test-shipping createLabel: carrier api unavailable')


def install_fakes(monkeypatch) -> tuple[FakePayments, FakeInventory, FakeShipping]:
    """Swap every module-scope client and shrink every delay on the path to one second.

    The handler keeps its conventional shape because the seams are module
    attributes rather than constructor parameters.

    `Executor.RETRY_BACKOFF_SECONDS` is the harness's own delay before it
    re-invokes after an InvocationError, which is how a StepInterruptedError
    reaches its replay. There is no time skipping in testing 1.2.1, so its
    default of 5 is 5 seconds of real test time.
    """
    monkeypatch.setattr(Executor, 'RETRY_BACKOFF_SECONDS', 1)
    payments, inventory, shipping = FakePayments(), FakeInventory(), FakeShipping()
    monkeypatch.setattr(handler_module, 'payments_client', payments)
    monkeypatch.setattr(handler_module, 'inventory_client', inventory)
    monkeypatch.setattr(handler_module, 'shipping_client', shipping)
    monkeypatch.setattr(handler_module, 'CHARGE_RETRY_DELAY_SECONDS', 1)
    monkeypatch.setattr(handler_module, 'LABEL_RETRY_DELAY_SECONDS', 1)
    return payments, inventory, shipping
