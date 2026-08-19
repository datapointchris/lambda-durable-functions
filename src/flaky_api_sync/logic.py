"""Pure decisions. No SDK types, no clients, no context.

Everything here is callable from a plain pytest test with plain dicts.
"""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass

RETRYABLE_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})
URL_EXPIRY_STATUS_CODES = frozenset({403, 410})


class PartnerApiError(Exception):
    """An HTTP failure from the billing partner, carrying what a retry decision needs.

    The status code has to survive the call rather than being flattened into a
    message, because it is the only thing that separates "come back later" from
    "this request will never work".
    """

    def __init__(self, status_code: int, message: str, retry_after_seconds: int | None = None) -> None:
        super().__init__(f'{status_code} {message}')
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


class DownloadUrlExpired(PartnerApiError):
    """The export's signed URL is no longer accepted.

    Permanent for that URL and recoverable by minting another one, so it is
    neither a step retry nor a failure — it is a restart of the whole block.
    """

    def __init__(self, status_code: int) -> None:
        super().__init__(status_code, 'signed download url rejected')


@dataclass(frozen=True)
class RetryLimits:
    """The budget a retry decision is allowed to spend."""

    max_attempts: int
    base_seconds: int
    cap_seconds: int


@dataclass(frozen=True)
class RetryPlan:
    should_retry: bool
    delay_seconds: int


NO_RETRY = RetryPlan(should_retry=False, delay_seconds=0)


def is_retryable_status(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS_CODES


def is_url_expiry_status(status_code: int) -> bool:
    return status_code in URL_EXPIRY_STATUS_CODES


def parse_retry_after(header_value: str | None) -> int | None:
    """Seconds from a `Retry-After` header, or None when there is nothing usable.

    Only the delta-seconds form is read. The HTTP-date form needs the current
    clock, and a step body that reads the clock returns a different answer on
    every replay.
    """
    if header_value is None:
        return None
    try:
        seconds = int(header_value.strip())
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


def backoff_delay_seconds(attempts_made: int, base_seconds: int, cap_seconds: int) -> int:
    """Doubling delay for `attempts_made`, which the SDK numbers from 1."""
    return min(base_seconds * 2 ** (attempts_made - 1), cap_seconds)


def plan_step_retry(error: Exception, attempts_made: int, limits: RetryLimits) -> RetryPlan:
    """Back off on a throttle or a 5xx; stop dead on anything else.

    A 4xx is the partner rejecting the request itself, so an identical second
    request fails identically. Retrying it spends the whole budget to arrive at
    the same failure several minutes later.
    """
    if not isinstance(error, PartnerApiError) or not is_retryable_status(error.status_code):
        return NO_RETRY
    if attempts_made >= limits.max_attempts:
        return NO_RETRY
    if error.retry_after_seconds is not None:
        return RetryPlan(should_retry=True, delay_seconds=max(error.retry_after_seconds, 1))
    delay = backoff_delay_seconds(attempts_made, limits.base_seconds, limits.cap_seconds)
    return RetryPlan(should_retry=True, delay_seconds=delay)


def failing_type_name(error: Exception) -> str:
    """The original class name of a failure that crossed a step boundary.

    A step re-raises its error as `CallableRuntimeError`, which keeps the class
    name on `error_type` and nothing else of the original — no attributes, and
    no `isinstance` relationship to the class that was raised.
    """
    return getattr(error, 'error_type', None) or type(error).__name__


def plan_block_retry(error: Exception, attempts_made: int, limits: RetryLimits) -> RetryPlan:
    """Restart the download block, and only for an expired URL.

    Everything else has already been through the step's own retry policy, so a
    second pass over the same block would repeat a decision that was made.
    """
    if failing_type_name(error) != DownloadUrlExpired.__name__:
        return NO_RETRY
    if attempts_made >= limits.max_attempts:
        return NO_RETRY
    delay = backoff_delay_seconds(attempts_made, limits.base_seconds, limits.cap_seconds)
    return RetryPlan(should_retry=True, delay_seconds=delay)


def export_is_running(export: dict) -> bool:
    """True while the partner is still building the export.

    Reads only the status just observed. Poll state is not threaded between
    attempts, so a stop condition computed from accumulated counters never fires.
    """
    return export['status'] == 'RUNNING'


def to_write_request(record: dict) -> dict:
    """One partner record as a DynamoDB BatchWriteItem entry."""
    return {
        'PutRequest': {
            'Item': {
                'subscriberId': {'S': record['id']},
                'planCode': {'S': record['plan']},
                'subscriptionStatus': {'S': record['status']},
                'mrrCents': {'N': str(record['mrr_cents'])},
            }
        }
    }


def chunked(records: Sequence[dict], size: int) -> Iterator[list[dict]]:
    """Split a listing into batches BatchWriteItem will accept."""
    for start in range(0, len(records), size):
        yield list(records[start : start + size])
