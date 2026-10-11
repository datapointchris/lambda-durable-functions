"""A `cattrs` converter as a checkpoint codec, for when the shapes outgrow the recursive codec.

The one module that imports the `cattrs` extra, so nothing else in the package requires it.
Worth the two extra packages in the bundle (`cattrs`, `attrs`) once you need discriminated
unions, per-field hooks, or validation on the way in.
"""

import datetime as dt
from typing import Any

import cattrs
from aws_durable_execution_sdk_python.serdes import SerDes

from nested_payloads.serdes import JsonSerDes


def for_class(cls: type) -> SerDes:
    """A codec that structures and unstructures `cls` through one converter.

    `datetime` needs hooks registered explicitly. cattrs leaves it as a datetime
    object, and `json.dumps` then raises `TypeError: Object of type datetime is
    not JSON serializable` — at the checkpoint, after the step body has run.
    """
    converter = cattrs.Converter()
    converter.register_unstructure_hook(dt.datetime, lambda value: value.isoformat())
    converter.register_structure_hook(dt.datetime, lambda value, _type: dt.datetime.fromisoformat(value))

    class _CattrsSerDes(JsonSerDes):
        def _to_payload(self, value: Any) -> Any:
            return converter.unstructure(value)

        def _from_payload(self, payload: Any) -> Any:
            return converter.structure(payload, cls)

    return _CattrsSerDes()
