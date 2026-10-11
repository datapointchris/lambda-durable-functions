"""The cattrs codec, when the shapes outgrow the recursive codec."""

import datetime as dt
import json

import pytest

pytest.importorskip('cattrs', reason='cattrs is an optional extra')

from aws_durable_execution_sdk_python.serdes import SerDesContext

from conftest_nested_payloads import NESTED
from nested_payloads import cattrs_serdes
from nested_payloads.models import Manifest
from nested_payloads.models import TrackedFile
from nested_payloads.serdes import NestedDataclassSerDes


@pytest.fixture
def ctx() -> SerDesContext:
    return SerDesContext()


def test_the_cattrs_serdes_round_trips_the_nested_type(ctx):
    serdes = cattrs_serdes.for_class(Manifest)
    restored = serdes.deserialize(serdes.serialize(NESTED, ctx), ctx)

    assert restored == NESTED
    assert isinstance(restored.files[0], TrackedFile)
    assert isinstance(restored.files[0].modified, dt.datetime)


def test_cattrs_produces_the_same_wire_form_as_the_recursive_codec(ctx):
    """Both emit plain JSON, so the store can read either."""
    cattrs_wire = cattrs_serdes.for_class(Manifest).serialize(NESTED, ctx)
    codec_wire = NestedDataclassSerDes(Manifest).serialize(NESTED, ctx)

    assert json.loads(cattrs_wire) == json.loads(codec_wire)
