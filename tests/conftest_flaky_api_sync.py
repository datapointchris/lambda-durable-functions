"""Fakes and fixtures for the flaky-partner sync.

The handler reads its configuration and builds both its DynamoDB client and its
partner client at import time, which is the conventional Lambda shape. The
environment is therefore set before the import and the clients are swapped per
test with `monkeypatch.setattr`.
"""

import os
from collections import defaultdict

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-2')
os.environ.setdefault('PARTNER_API_URL', 'https://api.partner-billing.test')
os.environ.setdefault('PARTNER_API_TOKEN', 'test-token')
os.environ.setdefault('SUBSCRIBER_TABLE', 'test-subscribers')

from flaky_api_sync import handler as handler_module  # noqa: E402
from flaky_api_sync.logic import DownloadUrlExpired, PartnerApiError  # noqa: E402

EXPORT_ID = 'exp_7742'
DOWNLOAD_URL = 'https://cdn.partner-billing.test/exp_7742.json?signature=abc'

SUBSCRIBERS = [
    {'id': 'sub_1001', 'plan': 'growth-annual', 'status': 'active', 'mrr_cents': 24900},
    {'id': 'sub_1002', 'plan': 'starter-monthly', 'status': 'past_due', 'mrr_cents': 2900},
    {'id': 'sub_1003', 'plan': 'scale-annual', 'status': 'active', 'mrr_cents': 99000},
]


def export_running() -> dict:
    return {'status': 'RUNNING', 'recordCount': 0}


def export_ready(record_count: int) -> dict:
    return {'status': 'READY', 'recordCount': record_count}


def export_failed() -> dict:
    return {'status': 'FAILED', 'recordCount': 0}


def throttled(retry_after_seconds: int = 1) -> PartnerApiError:
    return PartnerApiError(429, 'rate limit exceeded', retry_after_seconds)


def unavailable() -> PartnerApiError:
    return PartnerApiError(503, 'service unavailable')


def forbidden() -> PartnerApiError:
    return PartnerApiError(403, 'token lacks the exports scope')


def url_expired() -> DownloadUrlExpired:
    return DownloadUrlExpired(410)


class FakePartnerBillingApi:
    """A scripted partner, one queue per endpoint.

    A queue entry is either the value to return or the exception to raise, so a
    test can spell out "throttle, throttle, then succeed". The last entry repeats
    once the queue runs out, which is how an endpoint that never recovers is
    written.
    """

    def __init__(self) -> None:
        self.request_export_script: list = [EXPORT_ID]
        self.get_export_script: list = [export_running(), export_ready(len(SUBSCRIBERS))]
        self.mint_download_url_script: list = [DOWNLOAD_URL]
        self.download_records_script: list = [SUBSCRIBERS]
        self.calls: defaultdict[str, int] = defaultdict(int)
        self.requested_since: str | None = None
        self.downloaded_urls: list[str] = []

    def _next(self, endpoint: str, script: list):
        self.calls[endpoint] += 1
        entry = script[min(self.calls[endpoint] - 1, len(script) - 1)]
        if isinstance(entry, Exception):
            raise entry
        return entry

    def request_export(self, since: str) -> str:
        self.requested_since = since
        return self._next('request_export', self.request_export_script)

    def get_export(self, _export_id: str) -> dict:
        return self._next('get_export', self.get_export_script)

    def mint_download_url(self, _export_id: str) -> str:
        return self._next('mint_download_url', self.mint_download_url_script)

    def download_records(self, download_url: str) -> list[dict]:
        self.downloaded_urls.append(download_url)
        return self._next('download_records', self.download_records_script)


class FakeDynamoDb:
    def __init__(self) -> None:
        self.batches: list[dict] = []

    def batch_write_item(self, **kwargs) -> dict:
        self.batches.append(kwargs)
        return {'UnprocessedItems': {}}


def install_partner_fakes(monkeypatch) -> tuple[FakePartnerBillingApi, FakeDynamoDb]:
    """Swap both module-scope clients and shrink every modeled delay to a second.

    The harness has no clock skipping, so a retry delay is real wall-clock time.
    Durations are module constants precisely so a test can hold them at one second.
    """
    api, dynamo = FakePartnerBillingApi(), FakeDynamoDb()
    monkeypatch.setattr(handler_module, 'partner_api', api)
    monkeypatch.setattr(handler_module, 'dynamodb_client', dynamo)
    monkeypatch.setattr(handler_module, 'REQUEST_MAX_ATTEMPTS', 3)
    monkeypatch.setattr(handler_module, 'REQUEST_BASE_DELAY_SECONDS', 1)
    monkeypatch.setattr(handler_module, 'REQUEST_MAX_DELAY_SECONDS', 1)
    monkeypatch.setattr(handler_module, 'POLL_MAX_ATTEMPTS', 4)
    monkeypatch.setattr(handler_module, 'POLL_INITIAL_DELAY_SECONDS', 1)
    monkeypatch.setattr(handler_module, 'POLL_MAX_DELAY_SECONDS', 1)
    monkeypatch.setattr(handler_module, 'DOWNLOAD_MAX_ATTEMPTS', 2)
    monkeypatch.setattr(handler_module, 'DOWNLOAD_BASE_DELAY_SECONDS', 1)
    monkeypatch.setattr(handler_module, 'DOWNLOAD_MAX_DELAY_SECONDS', 1)
    return api, dynamo
