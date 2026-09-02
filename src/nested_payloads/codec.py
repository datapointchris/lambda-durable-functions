"""One conversion between dataclasses and plain JSON-safe values.

Two boundaries need it and they must agree: the SDK checkpoints a step's return
value, and S3 stores the same object for a downstream job to read. Writing the
conversion twice is how the two drift.

`unstructure` produces only types `json.dumps` accepts, so the S3 object is
ordinary readable JSON rather than the SDK's tagged envelope. `structure`
rebuilds the dataclass from the field type hints, recursing through lists,
dicts, tuples, optionals and nested dataclasses.
"""

from dataclasses import fields
from dataclasses import is_dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from types import UnionType
from typing import Any
from typing import Union
from typing import get_args
from typing import get_origin
from typing import get_type_hints
from uuid import UUID


def unstructure(value: Any) -> Any:
    """Dataclass tree -> plain JSON-safe values."""
    if is_dataclass(value) and not isinstance(value, type):
        return {f.name: unstructure(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, list | tuple | set | frozenset):
        return [unstructure(v) for v in value]
    if isinstance(value, dict):
        return {k: unstructure(v) for k, v in value.items()}
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, UUID | Decimal):
        return str(value)
    return value


def structure(value: Any, target: Any) -> Any:
    """Plain values -> dataclass tree, driven by `target`'s type hints."""
    origin = get_origin(target)

    if origin in (Union, UnionType):
        candidates = [a for a in get_args(target) if a is not type(None)]
        if value is None:
            return None
        return structure(value, candidates[0]) if len(candidates) == 1 else value

    if isinstance(target, type) and is_dataclass(target) and isinstance(value, dict):
        hints = get_type_hints(target)
        return target(**{f.name: structure(value[f.name], hints[f.name]) for f in fields(target) if f.name in value})

    if origin in (list, set, frozenset):
        (arg,) = get_args(target)
        return origin(structure(v, arg) for v in value)

    if origin is tuple:
        args = get_args(target)
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(structure(v, args[0]) for v in value)
        return tuple(structure(v, a) for v, a in zip(value, args, strict=False))

    if origin is dict:
        key_type, value_type = get_args(target)
        return {structure(k, key_type): structure(v, value_type) for k, v in value.items()}

    if target is datetime and isinstance(value, str):
        return datetime.fromisoformat(value)
    if target is date and isinstance(value, str):
        return date.fromisoformat(value)
    if target is UUID and isinstance(value, str):
        return UUID(value)
    if target is Decimal and isinstance(value, str):
        return Decimal(value)

    return value
