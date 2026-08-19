"""Round-tripping a nested dataclass through a checkpoint and through S3."""

import json
from dataclasses import asdict
from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python.exceptions import SerDesError
from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes, SerDesContext
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

from conftest_nested_payloads import install_fakes, s3_page
from nested_payloads.codec import structure, unstructure
from nested_payloads.models import Manifest, TrackedFile
from nested_payloads.serdes import ExplicitSerDes, FlatDataclassSerDes, NestedDataclassSerDes
from nested_payloads.store import ManifestStore

NESTED = Manifest(
    status='ready',
    bucket='test-lake',
    files=[
        TrackedFile('incoming/a.csv', 120, datetime(2026, 8, 19, 9, 0, tzinfo=UTC)),
        TrackedFile('incoming/b.csv', 80, datetime(2026, 8, 19, 9, 5, tzinfo=UTC)),
    ],
)


@pytest.fixture
def ctx() -> SerDesContext:
    return SerDesContext()


@pytest.fixture
def fakes(monkeypatch):
    return install_fakes(monkeypatch, [s3_page({'incoming/a.csv': (120, 600)})])


# --- the failure this exists to solve ---


def test_the_default_codec_refuses_a_dataclass(ctx):
    with pytest.raises(SerDesError, match='Unsupported type'):
        ExtendedTypeSerDes().serialize(NESTED, ctx)


def test_asdict_alone_returns_dicts_where_the_nested_type_belongs(ctx):
    """The obvious first attempt. It fails quietly rather than raising."""
    flat = FlatDataclassSerDes(Manifest)
    restored = flat.deserialize(flat.serialize(NESTED, ctx), ctx)

    assert isinstance(restored, Manifest)
    assert not isinstance(restored.files[0], TrackedFile)
    assert isinstance(restored.files[0], dict)
    with pytest.raises(AttributeError):
        _ = restored.files[0].key


# --- the recursive codec ---


def test_the_recursive_serdes_restores_the_whole_tree(ctx):
    serdes = NestedDataclassSerDes(Manifest)
    restored = serdes.deserialize(serdes.serialize(NESTED, ctx), ctx)

    assert restored == NESTED
    assert isinstance(restored.files[0], TrackedFile)
    assert isinstance(restored.files[0].modified, datetime)


def test_an_optional_field_survives_as_none(ctx):
    serdes = NestedDataclassSerDes(Manifest)
    assert serdes.deserialize(serdes.serialize(NESTED, ctx), ctx).note is None


def test_an_optional_field_survives_when_set(ctx):
    serdes = NestedDataclassSerDes(Manifest)
    with_note = Manifest('ready', 'test-lake', list(NESTED.files), note='late arrival')
    assert serdes.deserialize(serdes.serialize(with_note, ctx), ctx).note == 'late arrival'


def test_an_empty_file_list_round_trips(ctx):
    serdes = NestedDataclassSerDes(Manifest)
    empty = Manifest('empty', 'test-lake', [])
    assert serdes.deserialize(serdes.serialize(empty, ctx), ctx) == empty


def test_the_wire_form_is_plain_json_not_the_sdk_envelope(ctx):
    """The stored bytes must not carry the SDK's tagged format."""
    payload = json.loads(NestedDataclassSerDes(Manifest).serialize(NESTED, ctx))

    assert set(payload) == {'status', 'bucket', 'files', 'note'}
    assert payload['files'][0]['key'] == 'incoming/a.csv'
    assert payload['files'][0]['modified'] == '2026-08-19T09:00:00+00:00'
    assert 't' not in payload and 'v' not in payload


def test_structure_is_the_inverse_of_unstructure():
    assert structure(unstructure(NESTED), Manifest) == NESTED


def test_unstructure_emits_only_json_types():
    json.dumps(unstructure(NESTED))


# --- the explicit codec pair ---


class VersionedManifest(Manifest):
    """A type that owns its wire format, for when the format is a contract."""

    def to_payload(self) -> dict:
        return {
            'v': 2,
            'state': self.status,
            'files': [asdict(f) | {'modified': f.modified.isoformat()} for f in self.files],
        }

    @classmethod
    def from_payload(cls, payload: dict) -> 'VersionedManifest':
        return cls(
            status=payload['state'],
            bucket='test-lake',
            files=[
                TrackedFile(f['key'], f['size'], datetime.fromisoformat(f['modified']))
                for f in payload['files']
            ],
        )


def test_the_explicit_serdes_uses_the_types_own_format(ctx):
    serdes = ExplicitSerDes(VersionedManifest)
    original = VersionedManifest('ready', 'test-lake', list(NESTED.files))
    wire = json.loads(serdes.serialize(original, ctx))

    assert wire['v'] == 2
    assert wire['state'] == 'ready'
    assert serdes.deserialize(json.dumps(wire), ctx).files == list(NESTED.files)


# --- the S3 store, sharing the codec ---


def test_the_store_writes_readable_json(fakes):
    store = ManifestStore(fakes, 'test-lake')

    key = store.save('run-1', NESTED)

    stored = fakes.stored_json(key)
    assert stored['files'][0]['key'] == 'incoming/a.csv'
    assert stored['status'] == 'ready'


def test_the_store_round_trips_the_nested_type(fakes):
    store = ManifestStore(fakes, 'test-lake')

    key = store.save('run-1', NESTED)

    assert store.load(key) == NESTED


def test_stored_bytes_and_checkpoint_bytes_agree(fakes, ctx):
    """One codec, so a manifest means the same thing in both places."""
    store = ManifestStore(fakes, 'test-lake')
    key = store.save('run-1', NESTED)

    stored = fakes.stored_json(key)
    checkpointed = json.loads(NestedDataclassSerDes(Manifest).serialize(NESTED, ctx))

    assert stored == checkpointed


# --- through the real runtime ---


def test_the_handler_checkpoints_and_persists_the_same_manifest(fakes):
    from nested_payloads import handler as handler_module

    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        result = runner.run(input=json.dumps({}), timeout=30)

    assert result.result is not None
    payload = json.loads(result.result)
    assert payload['files'] == 1
    assert payload['totalBytes'] == 120

    restored = ManifestStore(fakes, 'test-lake').load(payload['manifestKey'])
    assert isinstance(restored.files[0], TrackedFile)


def test_a_previous_manifest_is_loaded_and_merged(fakes):
    from nested_payloads import handler as handler_module

    previous_key = ManifestStore(fakes, 'test-lake').save('run-0', NESTED)

    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        result = runner.run(input=json.dumps({'previousManifestKey': previous_key}), timeout=30)

    assert result.result is not None
    # a.csv is in both the previous manifest and the listing, b.csv only in the previous
    assert json.loads(result.result)['files'] == 2


def test_the_manifest_is_written_once_despite_replay(fakes):
    from nested_payloads import handler as handler_module

    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        runner.run(input=json.dumps({}), timeout=30)

    assert fakes.put_calls == 1


# --- cattrs, when the shapes outgrow the recursive codec ---

cattrs = pytest.importorskip('cattrs', reason='cattrs is an optional extra')


def test_the_cattrs_serdes_round_trips_the_nested_type(ctx):
    from nested_payloads.serdes import cattrs_serdes

    serdes = cattrs_serdes(Manifest)
    restored = serdes.deserialize(serdes.serialize(NESTED, ctx), ctx)

    assert restored == NESTED
    assert isinstance(restored.files[0], TrackedFile)
    assert isinstance(restored.files[0].modified, datetime)


def test_cattrs_produces_the_same_wire_form_as_the_recursive_codec(ctx):
    """Both emit plain JSON, so the store can read either."""
    from nested_payloads.serdes import cattrs_serdes

    assert json.loads(cattrs_serdes(Manifest).serialize(NESTED, ctx)) == json.loads(
        NestedDataclassSerDes(Manifest).serialize(NESTED, ctx)
    )
