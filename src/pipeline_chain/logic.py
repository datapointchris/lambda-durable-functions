"""Pure decisions. No SDK types, no clients, no context.

Everything here is callable from a plain pytest test with plain dicts.
"""

import datetime as dt
from collections.abc import Sequence

DATASETS: tuple[str, ...] = ('orders', 'clickstream', 'inventory')

TRANSIENT_ERROR_NAMES = frozenset(
    {
        'InternalServerError',
        'ProvisionedThroughputExceededException',
        'RequestTimeout',
        'ServiceUnavailable',
        'SlowDown',
        'ThrottlingException',
    }
)


def is_transient(error_name: str) -> bool:
    """True when the source refused this attempt rather than this request.

    A schema mismatch or a missing partition fails identically on every attempt,
    so retrying one only delays the two datasets that did arrive.
    """
    return error_name in TRANSIENT_ERROR_NAMES


def run_date_from_event(event: dict) -> str:
    """The partition every extract is scoped to, from the schedule's input.

    Validated before the branches start, so a malformed schedule costs nothing
    rather than three source reads.
    """
    raw = event.get('runDate')
    if not isinstance(raw, str):
        msg = f'runDate must be an ISO date, got {raw!r}'
        raise ValueError(msg)
    return dt.date.fromisoformat(raw).isoformat()


def staging_key(prefix: str, dataset: str, run_date: str) -> str:
    """Where one dataset's extract lands for the load function to read."""
    return f'{prefix}{dataset}/dt={run_date}/part-0000.jsonl'


def extract_summary(dataset: str, key: str, row_count: int) -> dict:
    """What a branch returns.

    Plain JSON types only: a BatchResult item is serialised with the default
    codec, which rejects a dataclass.
    """
    return {'dataset': dataset, 'stagingKey': key, 'rowCount': row_count}


def build_load_payload(run_date: str, extracts: Sequence[dict], expected: Sequence[str]) -> dict:
    """The payload the warehouse-load function is invoked with.

    `mode` is what the load branches on. A full refresh swaps the published
    tables; a partial one writes only the datasets that arrived and leaves the
    rest of yesterday's snapshot standing.
    """
    staged = {extract['dataset']: extract['stagingKey'] for extract in extracts}
    missing = [dataset for dataset in expected if dataset not in staged]
    return {
        'runDate': run_date,
        'mode': 'partial' if missing else 'full',
        'missing': missing,
        'datasets': {dataset: staged[dataset] for dataset in expected if dataset in staged},
        'rowCount': sum(extract['rowCount'] for extract in extracts),
    }
