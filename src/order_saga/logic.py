"""Pure decisions. No SDK types, no clients, no context.

Everything here is callable from a plain pytest test with plain dicts.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderLine:
    """One line of a basket. The same sku may appear on several lines."""

    sku: str
    quantity: int
    unit_price_cents: int


@dataclass(frozen=True)
class Order:
    """A checkout request, parsed from the event and never re-read after that."""

    order_id: str
    customer_id: str
    currency: str
    lines: tuple[OrderLine, ...]


def parse_order(event: dict) -> Order:
    """Read the checkout payload. Deterministic, so the handler body may call it on every replay."""
    lines = tuple(
        OrderLine(
            sku=line['sku'],
            quantity=int(line['quantity']),
            unit_price_cents=int(line['unitPriceCents']),
        )
        for line in event['lines']
    )
    return Order(
        order_id=event['orderId'],
        customer_id=event['customerId'],
        currency=event.get('currency', 'USD'),
        lines=lines,
    )


def order_total_cents(order: Order) -> int:
    return sum(line.quantity * line.unit_price_cents for line in order.lines)


def reserved_quantities(order: Order) -> dict[str, int]:
    """Stock to hold per sku. Two lines of the same sku are one reservation of the sum."""
    quantities: dict[str, int] = {}
    for line in order.lines:
        quantities[line.sku] = quantities.get(line.sku, 0) + line.quantity
    return quantities


def charge_idempotency_key(order: Order) -> str:
    """The processor's dedupe key. Derived from the order alone, never from the attempt.

    A key that varies per attempt turns the processor's guard off exactly when a
    retry needs it.
    """
    return f'checkout-{order.order_id}'


def should_retry_charge(*, interrupted: bool, attempts_made: int, max_attempts: int) -> bool:
    """An interrupted charge may already have reached the processor, so it is never retried.

    The saga cannot tell a lost response from a lost request. Retrying resolves
    that ambiguity in the customer's disfavor; failing the stage sends it to
    compensation, which asks the processor what actually happened.
    """
    if interrupted:
        return False
    return attempts_made < max_attempts
