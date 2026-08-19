"""Fakes for the nested-payload example."""

import datetime as dt
import json
import os

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-2')
os.environ.setdefault('MANIFEST_BUCKET', 'test-lake')
os.environ.setdefault('LANDING_BUCKET', 'test-lake')
os.environ.setdefault('MANIFEST_PREFIX', 'manifests/')
os.environ.setdefault('SOURCE_PREFIX', 'incoming/')

EPOCH = dt.datetime(2026, 8, 19, 12, 0, tzinfo=dt.UTC)


class FakeS3:
    """Enough of the S3 client for list, put and get, with call counts."""

    def __init__(self, pages: list[dict] | None = None) -> None:
        self.pages = pages if pages is not None else [{}]
        self.objects: dict[str, bytes] = {}
        self.paginate_calls = 0
        self.put_calls = 0
        self.get_calls = 0

    def get_paginator(self, _operation: str) -> 'FakeS3':
        return self

    def paginate(self, **_kwargs) -> list[dict]:
        self.paginate_calls += 1
        return self.pages

    def put_object(self, **kwargs) -> dict:
        self.put_calls += 1
        self.objects[kwargs['Key']] = kwargs['Body']
        return {'ETag': '"fake"'}

    def get_object(self, **kwargs) -> dict:
        self.get_calls += 1
        return {'Body': _Body(self.objects[kwargs['Key']])}

    def stored_json(self, key: str) -> dict:
        return json.loads(self.objects[key])


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


def s3_page(entries: dict[str, tuple[int, int]]) -> dict:
    """One list_objects_v2 page from key -> (size, age_in_seconds)."""
    return {
        'Contents': [
            {'Key': key, 'Size': size, 'LastModified': EPOCH - dt.timedelta(seconds=age)}
            for key, (size, age) in entries.items()
        ]
    }


def install_fakes(monkeypatch, pages: list[dict] | None = None) -> FakeS3:
    from nested_payloads import handler as handler_module
    from nested_payloads.store import ManifestStore

    fake = FakeS3(pages)
    monkeypatch.setattr(handler_module, 's3_client', fake)
    monkeypatch.setattr(
        handler_module,
        'store',
        ManifestStore(fake, handler_module.MANIFEST_BUCKET, handler_module.MANIFEST_PREFIX),
    )
    return fake
