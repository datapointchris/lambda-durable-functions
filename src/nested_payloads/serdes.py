"""Four ways to checkpoint a nested dataclass, and what each costs.

The SDK's default codec carries a closed set of types. A dataclass is not among
them, so a step returning one raises `SerDesError: Unsupported type` after the
body has already run. These are the options, in the order worth trying them.
"""

import json
from dataclasses import asdict
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from aws_durable_execution_sdk_python.exceptions import SerDesError
from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes, SerDes, SerDesContext

from nested_payloads.codec import structure, unstructure


@runtime_checkable
class SelfDescribing(Protocol):
    """A type that declares its own wire format."""

    def to_payload(self) -> dict: ...

    @classmethod
    def from_payload(cls, payload: dict) -> Any: ...


class JsonSerDes(SerDes):
    """Plain-JSON `SerDes` with the SDK's error surface wired in.

    A serdes failure otherwise arrives with no indication of which operation
    produced it, which is expensive to chase — the step body has already run and
    the traceback points at the codec rather than at the caller.
    """

    def _to_payload(self, value: Any) -> Any:
        raise NotImplementedError

    def _from_payload(self, payload: Any) -> Any:
        raise NotImplementedError

    def serialize(self, value: Any, serdes_context: SerDesContext) -> str:
        try:
            return json.dumps(self._to_payload(value), separators=(',', ':'))
        except (TypeError, ValueError) as exc:
            msg = (
                f'cannot serialize {type(value).__name__} for operation {serdes_context.operation_id}: {exc}'
            )
            raise SerDesError(msg) from exc

    def deserialize(self, data: str, serdes_context: SerDesContext) -> Any:
        try:
            return self._from_payload(json.loads(data))
        except (TypeError, ValueError, KeyError) as exc:
            msg = f'cannot deserialize for operation {serdes_context.operation_id}: {exc}'
            raise SerDesError(msg) from exc


class NestedDataclassSerDes(JsonSerDes):
    """Type-hint driven, recursive, no dependencies. The default choice.

    Shares `codec.unstructure`/`codec.structure` with the S3 store, so a
    checkpointed manifest and a stored one are the same bytes.
    """

    def __init__(self, cls: type) -> None:
        self._cls = cls

    def _to_payload(self, value: Any) -> Any:
        return unstructure(value)

    def _from_payload(self, payload: Any) -> Any:
        return structure(payload, self._cls)


class ExplicitSerDes(JsonSerDes):
    """Delegates to codec methods the type declares itself.

    More code per type, and the only option when the wire format is a contract
    someone else depends on — field renames, versioning, computed fields.
    """

    def __init__(self, cls: type[SelfDescribing]) -> None:
        self._cls = cls

    def _to_payload(self, value: SelfDescribing) -> Any:
        return value.to_payload()

    def _from_payload(self, payload: Any) -> Any:
        return self._cls.from_payload(payload)


class FlatDataclassSerDes(SerDes):
    """`asdict` out, constructor in. Correct only when nothing nested.

    Kept because it is the obvious first attempt and it fails quietly: a nested
    field comes back as a plain dict, and the failure surfaces later as an
    AttributeError somewhere else entirely.
    """

    def __init__(self, cls: type) -> None:
        self._cls = cls
        self._inner: ExtendedTypeSerDes = ExtendedTypeSerDes()

    def serialize(self, value: Any, serdes_context: SerDesContext) -> str:
        return self._inner.serialize(asdict(value), serdes_context)

    def deserialize(self, data: str, serdes_context: SerDesContext) -> Any:
        return self._cls(**self._inner.deserialize(data, serdes_context))


def cattrs_serdes(cls: type) -> SerDes:
    """A `cattrs` converter, for when the shapes outgrow the recursive codec.

    Worth the two extra packages in the bundle (`cattrs`, `attrs`) once you need
    discriminated unions, per-field hooks, or validation on the way in. Import is
    local so nothing here requires it.

    `datetime` needs hooks registered explicitly. cattrs leaves it as a datetime
    object, and `json.dumps` then raises `TypeError: Object of type datetime is
    not JSON serializable` — at the checkpoint, after the step body has run.
    """
    import cattrs

    converter = cattrs.Converter()
    converter.register_unstructure_hook(datetime, lambda value: value.isoformat())
    converter.register_structure_hook(datetime, lambda value, _type: datetime.fromisoformat(value))

    class _CattrsSerDes(JsonSerDes):
        def _to_payload(self, value: Any) -> Any:
            return converter.unstructure(value)

        def _from_payload(self, payload: Any) -> Any:
            return converter.structure(payload, cls)

    return _CattrsSerDes()
