"""The small approach, proved against the same nested shape."""

import json
from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes, SerDesContext
from aws_durable_execution_sdk_python.types import StepContext
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

from conftest_nested_payloads import FakeS3
from nested_payloads.minimal import Manifest, TrackedFile, load_manifest, save_manifest

NESTED = Manifest(
    status='ready',
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
            manifest = Manifest('ready', 'test-lake', discovered)
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
