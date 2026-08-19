"""Environment and fakes for the batch scoring example.

The handler reads its configuration and builds its boto3 clients at import time,
which is the conventional Lambda shape. Both have to be satisfied before the
module is imported, so the environment is set here and the handler is imported
and re-exported from here — a test module importing the handler directly would
be sorted above this import and find the environment empty.
"""

import io
import json
import os
import threading
import time

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-2')
os.environ.setdefault('FEATURE_BUCKET', 'test-feature-store')
os.environ.setdefault('FEATURE_PREFIX', 'features/')
os.environ.setdefault('SCORE_BUCKET', 'test-model-scores')
os.environ.setdefault('SCORE_PREFIX', 'scores/')
os.environ.setdefault('SCORING_ENDPOINT_NAME', 'credit-risk-v3')

from batch_scoring import handler as handler_module  # noqa: E402

APPLICATIONS_PER_BATCH = 3
MAX_CONCURRENT_BATCHES = 2
TOLERATED_BATCH_FAILURES = 1
RETRY_DELAY_SECONDS = 1
MAX_ATTEMPTS = 2
ENDPOINT_LATENCY_SECONDS = 0.05

COMPLETE_FEATURES = {
    'annualIncome': 82_000,
    'creditScore': 710,
    'debtToIncome': 0.31,
    'employmentMonths': 46,
}


class EndpointError(Exception):
    """Shaped like botocore's ClientError text, which is what the retry strategy matches on."""


class FakeS3:
    """An in-memory bucket. Every write is kept so replay can be asserted on counts."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_objects: list[dict] = []
        self.get_calls: list[str] = []

    def get_object(self, **kwargs) -> dict:
        self.get_calls.append(kwargs['Key'])
        return {'Body': io.BytesIO(self.objects[kwargs['Key']])}

    def put_object(self, **kwargs) -> dict:
        self.put_objects.append(kwargs)
        self.objects[kwargs['Key']] = kwargs['Body']
        return {'ETag': '"fake"'}

    def keys_under(self, prefix: str) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))


class FakeScoringEndpoint:
    """Stands in for `sagemaker-runtime`, and measures how many batches overlapped.

    `permanent` maps an application id to the error code raised on every call that
    carries it; `transient` maps one to a number of calls that raise a throttle
    before succeeding. The map runs iterations on real threads, so the concurrency
    counter is what proves `max_concurrency` is honoured.
    """

    def __init__(
        self,
        permanent: dict[str, str] | None = None,
        transient: dict[str, int] | None = None,
    ) -> None:
        self.permanent = dict(permanent or {})
        self.transient = dict(transient or {})
        self.invocations: list[list[str]] = []
        self.peak_concurrency = 0
        self._in_flight = 0
        self._lock = threading.Lock()

    def invoke_endpoint(self, **kwargs) -> dict:
        application_ids = [row['applicationId'] for row in json.loads(kwargs['Body'])['instances']]
        with self._lock:
            self.invocations.append(application_ids)
            self._in_flight += 1
            self.peak_concurrency = max(self.peak_concurrency, self._in_flight)
        try:
            time.sleep(ENDPOINT_LATENCY_SECONDS)
            self._raise_scripted_failure(application_ids)
            probabilities = [scripted_probability(a) for a in application_ids]
            body = json.dumps({'probabilities': probabilities}).encode()
            return {'Body': io.BytesIO(body)}
        finally:
            with self._lock:
                self._in_flight -= 1

    def _raise_scripted_failure(self, application_ids: list[str]) -> None:
        for application_id in application_ids:
            code = self.permanent.get(application_id)
            if code:
                raise EndpointError(self._client_error_text(code))
            if self.transient.get(application_id, 0) > 0:
                self.transient[application_id] -= 1
                raise EndpointError(self._client_error_text('ThrottlingException'))

    @staticmethod
    def _client_error_text(code: str) -> str:
        return f'An error occurred ({code}) when calling the InvokeEndpoint operation: rejected'


def scripted_probability(application_id: str) -> float:
    """`APP-0072` scores 0.72, so a test reads the risk band straight off the id."""
    return int(application_id.rsplit('-', 1)[-1]) / 100


def application_ids(*suffixes: int) -> list[str]:
    return [f'APP-{suffix:04d}' for suffix in suffixes]


def seed_features(s3: FakeS3, ids: list[str], incomplete: tuple[str, ...] = ()) -> None:
    """Write one feature row per application. An incomplete row has a null credit score."""
    for application_id in ids:
        row = {'applicationId': application_id, **COMPLETE_FEATURES}
        if application_id in incomplete:
            row['creditScore'] = None
        key = f'{handler_module.FEATURE_PREFIX}{application_id}.json'
        s3.objects[key] = json.dumps(row).encode()


def scoring_event(ids: list[str], run_id: str = 'run-2026-08-19') -> dict:
    return {'runId': run_id, 'applicationIds': ids}


def install_fakes(monkeypatch) -> tuple[FakeS3, FakeScoringEndpoint]:
    """Swap both module-scope clients and shrink every duration to test scale.

    The handler keeps its conventional shape because the seams are module
    attributes rather than constructor parameters. A plain factory rather than a
    fixture: pytest resolves fixtures by name in the test module, and a fixture
    imported for that alone reads as an unused import.
    """
    s3, endpoint = FakeS3(), FakeScoringEndpoint()
    monkeypatch.setattr(handler_module, 's3_client', s3)
    monkeypatch.setattr(handler_module, 'sagemaker_runtime_client', endpoint)
    monkeypatch.setattr(handler_module, 'APPLICATIONS_PER_BATCH', APPLICATIONS_PER_BATCH)
    monkeypatch.setattr(handler_module, 'MAX_CONCURRENT_BATCHES', MAX_CONCURRENT_BATCHES)
    monkeypatch.setattr(handler_module, 'TOLERATED_BATCH_FAILURES', TOLERATED_BATCH_FAILURES)
    monkeypatch.setattr(handler_module, 'SCORING_MAX_ATTEMPTS', MAX_ATTEMPTS)
    monkeypatch.setattr(handler_module, 'SCORING_RETRY_DELAY_SECONDS', RETRY_DELAY_SECONDS)
    return s3, endpoint
