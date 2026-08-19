"""Environment, fakes, and the one harness gap this example runs into.

The handler reads its configuration and builds its boto3 clients at import time,
which is the conventional Lambda shape. Both have to be satisfied before the
module is imported, so the environment is set here and the clients are swapped
per test with `monkeypatch.setattr`.

Testing 1.2.1 ships no checkpoint processor for `OperationType.CHAINED_INVOKE`:
`OperationTransformer._DEFAULT_PROCESSORS` has entries for STEP, WAIT, CONTEXT,
CALLBACK and EXECUTION only, and anything else raises
`InvalidParameterValueException: Checkpoint for OperationType.CHAINED_INVOKE is
not implemented yet`. That kills the whole execution, so `context.invoke` cannot
be driven at all until the slot is filled. `RecordingInvokeProcessor` fills it
for the duration of a test.
"""

import contextlib
import json
import os
import threading
import time
from collections.abc import Callable, Iterator

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-2')
os.environ.setdefault('STAGING_BUCKET', 'test-analytics-staging')
os.environ.setdefault('STAGING_PREFIX', 'staging/')
os.environ.setdefault('ORDERS_CLUSTER_ARN', 'arn:aws:rds:us-east-2:123456789012:cluster:commerce')
os.environ.setdefault('ORDERS_SECRET_ARN', 'arn:aws:secretsmanager:us-east-2:123456789012:secret:c')
os.environ.setdefault('ORDERS_DATABASE', 'commerce')
os.environ.setdefault('CLICKSTREAM_BUCKET', 'test-clickstream')
os.environ.setdefault('CLICKSTREAM_PREFIX', 'events/')
os.environ.setdefault('INVENTORY_TABLE', 'inventory-snapshots')
os.environ.setdefault('LOAD_FUNCTION_NAME', 'warehouse-load')

from aws_durable_execution_sdk_python.lambda_service import (  # noqa: E402
    ChainedInvokeDetails,
    Operation,
    OperationStatus,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.serdes import DEFAULT_JSON_SERDES, SerDesContext  # noqa: E402
from aws_durable_execution_sdk_python_testing.checkpoint.transformer import (  # noqa: E402
    OperationTransformer,
)

from pipeline_chain import handler as handler_module  # noqa: E402

RUN_DATE = '2026-08-18'

ORDER_ROWS = [
    {'order_id': 'ord-9001', 'customer_id': 'cus-41', 'total_cents': 12900},
    {'order_id': 'ord-9002', 'customer_id': 'cus-77', 'total_cents': 4550},
]
CLICKSTREAM_KEYS = [
    f'events/dt={RUN_DATE}/collector-a.jsonl',
    f'events/dt={RUN_DATE}/collector-b.jsonl',
    f'events/dt={RUN_DATE}/collector-c.jsonl',
]
INVENTORY_ITEMS = [
    {'sku': {'S': 'SKU-1'}, 'on_hand': {'N': '18'}},
    {'sku': {'S': 'SKU-2'}, 'on_hand': {'N': '4'}},
]


class ThrottlingException(Exception):
    """Named to match the botocore error `is_transient` recognises."""


class SchemaMismatch(Exception):
    """A source failure that repeating cannot fix."""


class ConcurrencyProbe:
    """Records the most extracts that were ever in flight at the same time.

    Branches run on a ThreadPoolExecutor sized by `max_concurrency`, so the peak
    is the only direct evidence the cap is doing anything.
    """

    def __init__(self, dwell_seconds: float = 0.15) -> None:
        self.dwell_seconds = dwell_seconds
        self.peak = 0
        self._in_flight = 0
        self._lock = threading.Lock()

    @contextlib.contextmanager
    def in_flight(self) -> Iterator[None]:
        with self._lock:
            self._in_flight += 1
            self.peak = max(self.peak, self._in_flight)
        try:
            time.sleep(self.dwell_seconds)
            yield
        finally:
            with self._lock:
                self._in_flight -= 1


class FailureSchedule:
    """Errors a fake source raises, one per call, until the list runs out."""

    def __init__(self) -> None:
        self.pending: list[Exception] = []
        self._lock = threading.Lock()

    def raise_if_due(self) -> None:
        with self._lock:
            error = self.pending.pop(0) if self.pending else None
        if error is not None:
            raise error


class FakeS3:
    """Lists the clickstream prefix and collects every staged write."""

    def __init__(self, probe: ConcurrencyProbe) -> None:
        self.probe = probe
        self.keys = list(CLICKSTREAM_KEYS)
        self.failures = FailureSchedule()
        self.paginate_calls = 0
        self.put_objects: list[dict] = []
        self._lock = threading.Lock()

    def get_paginator(self, _operation: str) -> 'FakeS3':
        return self

    def paginate(self, **_kwargs) -> list[dict]:
        with self.probe.in_flight():
            with self._lock:
                self.paginate_calls += 1
            self.failures.raise_if_due()
            return [{'Contents': [{'Key': key} for key in self.keys]}]

    def put_object(self, **kwargs) -> dict:
        with self._lock:
            self.put_objects.append(kwargs)
        return {'ETag': '"fake"'}

    def staged_keys(self) -> list[str]:
        return sorted(written['Key'] for written in self.put_objects)


class FakeRdsData:
    """Stands in for the Aurora Data API."""

    def __init__(self, probe: ConcurrencyProbe) -> None:
        self.probe = probe
        self.rows = list(ORDER_ROWS)
        self.failures = FailureSchedule()
        self.statements: list[dict] = []
        self._lock = threading.Lock()

    def execute_statement(self, **kwargs) -> dict:
        with self.probe.in_flight():
            with self._lock:
                self.statements.append(kwargs)
            self.failures.raise_if_due()
            return {'formattedRecords': json.dumps(self.rows)}


class FakeDynamoDb:
    """Stands in for the inventory snapshot table."""

    def __init__(self, probe: ConcurrencyProbe) -> None:
        self.probe = probe
        self.items = list(INVENTORY_ITEMS)
        self.failures = FailureSchedule()
        self.queries: list[dict] = []
        self._lock = threading.Lock()

    def query(self, **kwargs) -> dict:
        with self.probe.in_flight():
            with self._lock:
                self.queries.append(kwargs)
            self.failures.raise_if_due()
            return {'Items': self.items}


class Sources:
    """The three swapped clients plus the probe they share."""

    def __init__(self, probe: ConcurrencyProbe) -> None:
        self.probe = probe
        self.s3 = FakeS3(probe)
        self.rds = FakeRdsData(probe)
        self.dynamodb = FakeDynamoDb(probe)


def load_warehouse(payload: dict) -> dict:
    """Stands in for the second durable function."""
    return {
        'loadRunId': f'load-{payload["runDate"]}',
        'tablesSwapped': payload['mode'] == 'full',
    }


class RecordingInvokeProcessor:
    """Completes a chained invoke inside its own START checkpoint.

    The SDK checks the operation's status again after the synchronous START
    checkpoint returns, precisely so an invoke that finished immediately costs no
    suspension. Marking the operation SUCCEEDED there is therefore a path the SDK
    already supports, and the parent resumes with the result rather than waiting
    for a service that the in-process harness does not have.

    START is the only action to handle: `OperationUpdate.create_invoke_start` is
    the only chained-invoke checkpoint the SDK writes, and every terminal one
    comes from the service. The scheduler hooks the transformer passes are
    ignored for the same reason nothing else is needed — no later wake-up.
    """

    def __init__(self, downstream: Callable[[dict], object]) -> None:
        self.downstream = downstream
        self.calls: list[tuple[str, dict]] = []

    def process(self, update: OperationUpdate, **_harness_arguments: object) -> Operation:
        serdes_context = SerDesContext()
        payload = DEFAULT_JSON_SERDES.deserialize(update.payload or 'null', serdes_context)
        options = update.chained_invoke_options
        self.calls.append((options.function_name if options else '', payload))
        return Operation(
            operation_id=update.operation_id,
            operation_type=OperationType.CHAINED_INVOKE,
            status=OperationStatus.SUCCEEDED,
            parent_id=update.parent_id,
            name=update.name,
            sub_type=update.sub_type,
            chained_invoke_details=ChainedInvokeDetails(
                result=DEFAULT_JSON_SERDES.serialize(self.downstream(payload), serdes_context)
            ),
        )


def swap_sources(monkeypatch) -> Sources:
    """Swap every module-scope client and shrink the retry delay."""
    swapped = Sources(ConcurrencyProbe())
    monkeypatch.setattr(handler_module, 's3_client', swapped.s3)
    monkeypatch.setattr(handler_module, 'rds_data_client', swapped.rds)
    monkeypatch.setattr(handler_module, 'dynamodb_client', swapped.dynamodb)
    monkeypatch.setattr(handler_module, 'EXTRACT_RETRY_DELAY_SECONDS', 1)
    return swapped


def register_load_function(monkeypatch) -> RecordingInvokeProcessor:
    """Give the harness a chained-invoke processor and record what it receives."""
    processor = RecordingInvokeProcessor(load_warehouse)
    monkeypatch.setitem(
        OperationTransformer._DEFAULT_PROCESSORS, OperationType.CHAINED_INVOKE, processor
    )
    return processor
