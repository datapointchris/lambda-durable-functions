"""Pure decisions. No SDK types, no clients, no context.

Everything here is callable from a plain pytest test with plain dicts.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Manifest:
    """The set of objects one ingest job will read."""

    run_id: str
    keys: tuple[str, ...]
    total_bytes: int


def is_quiet(newest_epoch: float | None, now_epoch: float, quiet_seconds: int) -> bool:
    """True when nothing has landed recently enough to expect more.

    Settle detection reads the newest object's timestamp rather than comparing
    successive listings, so a poll attempt needs no memory of the one before it.
    An empty prefix is never quiet — there is nothing to ingest yet.
    """
    if newest_epoch is None:
        return False
    return (now_epoch - newest_epoch) >= quiet_seconds


def build_manifest(run_id: str, objects: dict[str, int]) -> Manifest:
    """Freeze a listing into the manifest an ingest job will consume."""
    keys = tuple(sorted(objects))
    return Manifest(run_id=run_id, keys=keys, total_bytes=sum(objects.values()))


def next_poll_delay_seconds(attempt: int, schedule: tuple[int, ...], steady: int) -> int:
    """Delay before poll `attempt`, which the SDK numbers from 1.

    Indexing `schedule` with `attempt` rather than `attempt - 1` silently skips
    element 0 and falls through to `steady`.
    """
    index = attempt - 1
    return schedule[index] if index < len(schedule) else steady
