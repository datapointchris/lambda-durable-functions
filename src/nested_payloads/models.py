"""A nested payload of the shape a real step returns.

`Manifest` holds a list of `TrackedFile`, so it is not flat. That is the case
the SDK's default codec cannot carry and a top-level-only reconstruction gets
wrong: the manifest comes back as a `Manifest` whose `files` are plain dicts.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TrackedFile:
    key: str
    size: int
    modified: datetime


@dataclass(frozen=True)
class Manifest:
    status: str
    bucket: str
    files: list[TrackedFile]
    note: str | None = None

    @property
    def total_bytes(self) -> int:
        return sum(f.size for f in self.files)
