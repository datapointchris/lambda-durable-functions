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


@dataclass(frozen=True)
class TrackedFile:
    key: str
    size: int
    modified: datetime

    def to_dict(self) -> dict:
        return {'key': self.key, 'size': self.size, 'modified': self.modified.isoformat()}

    @classmethod
    def from_dict(cls, payload: dict) -> 'TrackedFile':
        return cls(payload['key'], payload['size'], datetime.fromisoformat(payload['modified']))


@dataclass(frozen=True)
class Manifest:
    status: str
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
            payload['status'],
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
