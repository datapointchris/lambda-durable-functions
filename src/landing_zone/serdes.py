"""Checkpoint serialization for types the SDK's default codec does not carry.

`ExtendedTypeSerDes` carries the closed set of types the `TypeTag` enum in the
SDK's `serdes.py` names, listed in docs/sdk-internals.md. Anything else raises
`SerDesError: Unsupported type`, dataclasses included.
"""

from dataclasses import asdict
from typing import Any

from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes
from aws_durable_execution_sdk_python.serdes import SerDes
from aws_durable_execution_sdk_python.serdes import SerDesContext


class DataclassSerDes(SerDes):
    """Round-trip a flat dataclass through the SDK's own extended-type codec.

    Delegating to `ExtendedTypeSerDes` rather than `json.dumps` is what keeps a
    `datetime` field a `datetime` on the way back instead of a string.

    Fields that are themselves dataclasses come back as plain dicts, because
    `asdict` flattens them going out and the constructor call only rebuilds the
    top level. Nested types need their own reconstruction here.

    `nested_payloads.serdes.FlatDataclassSerDes` carries the same body on
    purpose. Each example is a self-contained Lambda that imports nothing from
    another, so it can be read on its own.
    """

    def __init__(self, cls: type) -> None:
        self._cls = cls
        self._inner: ExtendedTypeSerDes = ExtendedTypeSerDes()

    def serialize(self, value: Any, serdes_context: SerDesContext) -> str:
        return self._inner.serialize(asdict(value), serdes_context)

    def deserialize(self, data: str, serdes_context: SerDesContext) -> Any:
        return self._cls(**self._inner.deserialize(data, serdes_context))
