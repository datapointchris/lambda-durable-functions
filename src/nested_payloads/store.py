"""Load and save manifests in S3 using the same codec the checkpoint uses.

The stored object is plain JSON. The SDK's own `ExtendedTypeSerDes` produces a
tagged envelope — `{"t":"m","v":{...}}` — which is an implementation detail of
the SDK version, so persisting it would make every downstream reader depend on
the SDK's internal format. A Glue job, Athena, or a person opening the object
gets ordinary JSON instead.
"""

import json
from typing import Any

from nested_payloads.codec import structure, unstructure
from nested_payloads.models import Manifest


class ManifestStore:
    """S3 persistence for a nested dataclass.

    The client is a parameter so tests pass a fake. The handler holds one of
    these at module scope, built from a module-scope boto3 client.
    """

    def __init__(self, s3_client: Any, bucket: str, prefix: str = 'manifests/') -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix

    def key_for(self, run_id: str) -> str:
        return f'{self._prefix}{run_id}.json'

    def save(self, run_id: str, manifest: Manifest) -> str:
        key = self.key_for(run_id)
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=json.dumps(unstructure(manifest), indent=2).encode(),
            ContentType='application/json',
        )
        return key

    def load(self, key: str) -> Manifest:
        body = self._s3.get_object(Bucket=self._bucket, Key=key)['Body'].read()
        return structure(json.loads(body), Manifest)
