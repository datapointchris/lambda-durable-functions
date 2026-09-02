"""The default codec's type set, and what the custom SerDes adds."""

from dataclasses import dataclass
from datetime import datetime

import pytest
from aws_durable_execution_sdk_python.exceptions import SerDesError
from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes
from aws_durable_execution_sdk_python.serdes import SerDesContext

from landing_zone.logic import Manifest
from landing_zone.serdes import DataclassSerDes


@dataclass
class Stamped:
    at: datetime
    label: str


@pytest.fixture
def ctx() -> SerDesContext:
    return SerDesContext()


def test_the_default_codec_rejects_a_dataclass(ctx):
    with pytest.raises(SerDesError, match='Unsupported type'):
        ExtendedTypeSerDes().serialize(Manifest('run-1', ('a',), 10), ctx)


def test_a_manifest_round_trips_through_the_custom_serdes(ctx):
    serdes = DataclassSerDes(Manifest)
    original = Manifest(run_id='run-1', keys=('landing/a.csv', 'landing/b.csv'), total_bytes=30)
    assert serdes.deserialize(serdes.serialize(original, ctx), ctx) == original


def test_the_tuple_field_survives_as_a_tuple(ctx):
    serdes = DataclassSerDes(Manifest)
    original = Manifest('run-1', ('landing/a.csv',), 10)
    assert isinstance(serdes.deserialize(serdes.serialize(original, ctx), ctx).keys, tuple)


def test_a_datetime_field_is_preserved_rather_than_stringified(ctx):
    serdes = DataclassSerDes(Stamped)
    original = Stamped(at=datetime(2026, 1, 1, 12, 30), label='x')
    assert isinstance(serdes.deserialize(serdes.serialize(original, ctx), ctx).at, datetime)


@pytest.mark.parametrize(
    'value',
    [None, 'x', 1, 1.5, True, b'bytes', (1, 2), [1, 2], {'k': 'v'}, datetime(2026, 1, 1)],
)
def test_the_default_codec_carries_its_documented_types(value, ctx):
    codec = ExtendedTypeSerDes()
    assert codec.deserialize(codec.serialize(value, ctx), ctx) == value
