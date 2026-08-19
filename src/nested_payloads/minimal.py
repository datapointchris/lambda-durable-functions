"""The small way, for when the codec costs more than it saves.

`codec.py` plus `serdes.py` is around 200 lines. For two dataclasses in one
Lambda that is a worse trade than converting by hand where it happens.

No `SerDes` subclass appears here, and none is needed. `dict`, `list` and
`datetime` are all in the default codec's supported set and it recurses, so a
step returning a nested dict checkpoints natively. The dataclass is rebuilt at
the call site.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class Status(StrEnum):
    """A StrEnum checkpoints as a plain string and comes back as one.

    Nothing has to be configured for it to serialize — it is a `str` subclass, so
    the default codec takes its primitive fast path and `json.dumps` emits the
    value. It comes back a plain `str`, which still compares equal and still
    matches, so the downgrade goes unnoticed until something calls `.name` or
    checks `isinstance`.

    `to_dict` therefore passes the field through rather than reading `.value`,
    which would raise on a `Manifest` built with a bare string. `from_dict` is
    where it is normalised, and that call validates: an unknown value raises
    `ValueError` instead of travelling on.
    """

    READY = 'ready'
    PARTIAL = 'partial'
    EMPTY = 'empty'


@dataclass(frozen=True)
class TrackedFile:
    key: str
    size: int
    modified: datetime
    status: Status = Status.READY

    def to_dict(self) -> dict:
        return {
            'key': self.key,
            'size': self.size,
            'modified': self.modified.isoformat(),
            'status': self.status,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> 'TrackedFile':
        return cls(
            payload['key'],
            payload['size'],
            datetime.fromisoformat(payload['modified']),
            Status(payload['status']),
        )


@dataclass(frozen=True)
class Manifest:
    status: Status
    bucket: str
    files: list[TrackedFile]

    def to_dict(self) -> dict:
        return {
            'status': self.status,
            'bucket': self.bucket,
            'files': [f.to_dict() for f in self.files],
        }

    @classmethod
    def from_dict(cls, payload: dict) -> 'Manifest':
        return cls(
            Status(payload['status']),
            payload['bucket'],
            [TrackedFile.from_dict(f) for f in payload['files']],
        )


def save_manifest(s3_client, bucket: str, key: str, manifest: Manifest) -> str:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest.to_dict(), indent=2).encode(),
        ContentType='application/json',
    )
    return key


def load_manifest(s3_client, bucket: str, key: str) -> Manifest:
    body = s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()
    return Manifest.from_dict(json.loads(body))
