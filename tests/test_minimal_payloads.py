"""The small approach, proved against the same nested shape."""

import json
from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.exceptions import SerDesError
from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes, SerDesContext
from aws_durable_execution_sdk_python.types import StepContext
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

from conftest_nested_payloads import FakeS3
from nested_payloads.minimal import Manifest, Status, TrackedFile, load_manifest, save_manifest

NESTED = Manifest(
    status=Status.READY,
    bucket='test-lake',
    files=[
        TrackedFile('incoming/a.csv', 120, datetime(2026, 8, 19, 9, 0, tzinfo=UTC)),
        TrackedFile('incoming/b.csv', 80, datetime(2026, 8, 19, 9, 5, tzinfo=UTC)),
    ],
)


def test_two_methods_per_type_round_trip_the_whole_tree():
    assert Manifest.from_dict(NESTED.to_dict()) == NESTED


def test_the_nested_type_is_restored_not_left_as_a_dict():
    assert isinstance(Manifest.from_dict(NESTED.to_dict()).files[0], TrackedFile)


def test_a_nested_dict_needs_no_custom_serdes_at_all():
    """The reason no SerDes subclass appears in minimal.py."""
    codec, ctx = ExtendedTypeSerDes(), SerDesContext()

    restored = codec.deserialize(codec.serialize(NESTED.to_dict(), ctx), ctx)

    assert Manifest.from_dict(restored) == NESTED


def test_the_default_codec_even_keeps_a_raw_datetime_inside_a_dict():
    """So to_dict may skip isoformat if the payload never reaches S3."""
    codec, ctx = ExtendedTypeSerDes(), SerDesContext()
    payload = {'files': [{'key': 'a.csv', 'modified': datetime(2026, 8, 19, 9, 0, tzinfo=UTC)}]}

    restored = codec.deserialize(codec.serialize(payload, ctx), ctx)

    assert isinstance(restored['files'][0]['modified'], datetime)


def test_s3_round_trip_uses_the_same_two_methods():
    fake = FakeS3()

    save_manifest(fake, 'test-lake', 'manifests/run-1.json', NESTED)

    assert load_manifest(fake, 'test-lake', 'manifests/run-1.json') == NESTED


def test_the_stored_object_is_readable_json():
    fake = FakeS3()

    save_manifest(fake, 'test-lake', 'manifests/run-1.json', NESTED)

    stored = json.loads(fake.objects['manifests/run-1.json'])
    assert stored['files'][0]['key'] == 'incoming/a.csv'
    assert stored['files'][0]['modified'] == '2026-08-19T09:00:00+00:00'


@pytest.fixture
def fake_s3():
    return FakeS3()


def test_a_handler_using_only_dicts_needs_no_step_config(fake_s3):
    """No StepConfig, no serdes argument, no SerDes subclass anywhere."""
    discovered = [TrackedFile('incoming/a.csv', 120, datetime(2026, 8, 19, 9, 0, tzinfo=UTC))]

    @durable_execution
    def lambda_handler(_event: dict, context: DurableContext) -> dict:
        def discover(step_context: StepContext) -> dict:
            manifest = Manifest(Status.READY, 'test-lake', discovered)
            step_context.logger.info('found %d file(s)', len(manifest.files))
            return manifest.to_dict()

        manifest = Manifest.from_dict(context.step(discover, name='discover'))

        def persist(step_context: StepContext) -> str:
            key = save_manifest(fake_s3, 'test-lake', 'manifests/run-1.json', manifest)
            step_context.logger.info('saved %s', key)
            return key

        return {'files': len(manifest.files), 'key': context.step(persist, name='persist')}

    with DurableFunctionTestRunner(lambda_handler) as runner:
        result = runner.run(input='{}', timeout=30)

    assert result.result is not None
    assert json.loads(result.result)['files'] == 1
    assert load_manifest(fake_s3, 'test-lake', 'manifests/run-1.json').files[0].key == 'incoming/a.csv'
    assert fake_s3.put_calls == 1


# --- StrEnum ---


def test_a_strenum_serializes_with_no_configuration():
    """It is a str subclass, so the codec takes its primitive fast path."""
    codec, ctx = ExtendedTypeSerDes(), SerDesContext()

    assert codec.serialize(Status.READY, ctx) == '"ready"'


def test_a_strenum_comes_back_as_a_plain_str():
    codec, ctx = ExtendedTypeSerDes(), SerDesContext()

    restored = codec.deserialize(codec.serialize(Status.READY, ctx), ctx)

    assert type(restored) is str
    assert not isinstance(restored, Status)


def test_a_downgraded_strenum_still_compares_and_matches():
    """Which is why the downgrade goes unnoticed until something checks the type."""
    codec, ctx = ExtendedTypeSerDes(), SerDesContext()
    restored = codec.deserialize(codec.serialize(Status.READY, ctx), ctx)

    assert restored == Status.READY
    assert restored in {Status.READY}
    assert {Status.READY: 1}[restored] == 1
    match restored:
        case Status.READY:
            pass
        case _:
            pytest.fail('match/case did not match the member')


def test_a_downgraded_strenum_loses_isinstance_and_name():
    codec, ctx = ExtendedTypeSerDes(), SerDesContext()
    restored = codec.deserialize(codec.serialize(Status.READY, ctx), ctx)

    assert not isinstance(restored, Status)
    with pytest.raises(AttributeError):
        _ = restored.name


def test_a_plain_enum_is_a_hard_failure_not_a_downgrade():
    """Unlike StrEnum, an Enum that is not str- or int-backed raises."""
    from enum import Enum

    class Mode(Enum):
        FULL = 'full'

    with pytest.raises(SerDesError, match='Unsupported type'):
        ExtendedTypeSerDes().serialize(Mode.FULL, SerDesContext())


def test_from_dict_restores_the_member_and_validates():
    restored = Manifest.from_dict(NESTED.to_dict())

    assert isinstance(restored.status, Status)
    assert restored.status.name == 'READY'
    assert isinstance(restored.files[0].status, Status)


def test_an_unknown_status_is_rejected_on_rebuild():
    payload = NESTED.to_dict() | {'status': 'bogus'}

    with pytest.raises(ValueError, match='not a valid Status'):
        Manifest.from_dict(payload)


# --- the same approach as a real handler module ---


@pytest.fixture
def minimal_handler(monkeypatch):
    """Swap the module-scope client, exactly as the other examples do."""
    import datetime as dt

    from nested_payloads import minimal_handler as handler_module

    fake = FakeS3(
        [
            {
                'Contents': [
                    {
                        'Key': 'incoming/a.csv',
                        'Size': 120,
                        'LastModified': dt.datetime(2026, 8, 19, 9, 0, tzinfo=dt.UTC),
                    }
                ]
            }
        ]
    )
    monkeypatch.setattr(handler_module, 's3_client', fake)
    return handler_module, fake


def test_the_minimal_handler_needs_no_step_config_anywhere(minimal_handler):
    handler_module, fake = minimal_handler

    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        result = runner.run(input=json.dumps({'expectedFiles': 1}), timeout=30)

    assert result.result is not None
    payload = json.loads(result.result)
    assert payload['files'] == 1
    assert payload['status'] == Status.READY


def test_the_strenum_survives_the_round_trip_through_s3(minimal_handler):
    handler_module, fake = minimal_handler

    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        result = runner.run(input=json.dumps({'expectedFiles': 1}), timeout=30)

    assert result.result is not None
    key = json.loads(result.result)['manifestKey']
    restored = load_manifest(fake, handler_module.LANDING_BUCKET, key)

    assert isinstance(restored.status, Status)
    assert restored.status is Status.READY
    assert isinstance(restored.files[0].status, Status)


def test_a_partial_drop_is_reported_as_partial(minimal_handler):
    handler_module, _fake = minimal_handler

    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        result = runner.run(input=json.dumps({'expectedFiles': 3}), timeout=30)

    assert result.result is not None
    assert json.loads(result.result)['status'] == Status.PARTIAL


def test_the_manifest_is_written_once_despite_replay(minimal_handler):
    handler_module, fake = minimal_handler

    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        runner.run(input=json.dumps({'expectedFiles': 1}), timeout=30)

    assert fake.put_calls == 1
