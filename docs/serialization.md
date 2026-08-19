# Serialization

How to get a nested dataclass through a checkpoint, and how to store the same object in S3 without
writing the conversion twice.

Measured against `aws-durable-execution-sdk-python` **1.7.0** on 2026-08-19. Working code and its
tests are `src/nested_payloads/` and `tests/test_nested_payloads.py`.

## The default codec carries a closed set of types

`ExtendedTypeSerDes` is what the SDK uses when a step has no `serdes` configured. Its `TypeTag`
enum is the complete list:

| Carried |
|---|
| `None` `str` `int` `float` `bool` `bytes` `UUID` `Decimal` `datetime` `date` `tuple` `list` `dict` `BatchResult` |

A dataclass is not among them:

```text
SerDesError: Unsupported type: <class 'Manifest'>
```

!!! danger "The failure lands after the work is done"
    Serialization happens when the step **succeeds**, not when it starts. The body has already run
    its side effect by the time the checkpoint is rejected. A step that charges a card and returns a
    dataclass charges the card, then fails.

## The obvious first attempt is wrong, and it fails quietly

`asdict` on the way out and the constructor on the way in looks like it works. For a flat type it
does. For `Manifest(status, bucket, files=[TrackedFile])` it does not, because `asdict` flattens the
whole tree and the constructor only rebuilds the top level:

```python
restored = flat.deserialize(flat.serialize(manifest, ctx), ctx)

isinstance(restored, Manifest)            # True
isinstance(restored.files[0], TrackedFile)  # False — it is a dict
restored.files[0].key                     # AttributeError
```

That is `FlatDataclassSerDes` in the repo, kept and tested precisely because it is the trap. Nothing
raises at the boundary. The `AttributeError` surfaces later, somewhere else, on a replay.

## Start here: return a dict and you need no `SerDes` at all

`dict`, `list`, `tuple` and `datetime` are all in the supported set, and the codec recurses. So a
step returning a **nested dict** checkpoints natively — no `StepConfig`, no `serdes=`, no subclass.
The whole problem exists only because the step returns a dataclass.

```python
def discover(step_context: StepContext) -> dict:
    manifest = Manifest('ready', bucket, files)
    step_context.logger.info('found %d file(s)', len(manifest.files))
    return manifest.to_dict()

manifest = Manifest.from_dict(context.step(discover, name='discover'))
```

Two methods per type is the entire cost:

```python
@dataclass(frozen=True)
class TrackedFile:
    key: str
    size: int
    modified: datetime

    def to_dict(self) -> dict:
        return {'key': self.key, 'size': self.size, 'modified': self.modified.isoformat()}

    @classmethod
    def from_dict(cls, payload: dict) -> 'TrackedFile':
        return cls(payload['key'], payload['size'], datetime.fromisoformat(payload['modified']))


@dataclass(frozen=True)
class Manifest:
    status: str
    bucket: str
    files: list[TrackedFile]

    def to_dict(self) -> dict:
        return {'status': self.status, 'bucket': self.bucket,
                'files': [f.to_dict() for f in self.files]}

    @classmethod
    def from_dict(cls, payload: dict) -> 'Manifest':
        return cls(payload['status'], payload['bucket'],
                   [TrackedFile.from_dict(f) for f in payload['files']])
```

The same two methods serve S3, so there is still only one conversion:

```python
def save_manifest(s3_client, bucket, key, manifest) -> str:
    s3_client.put_object(Bucket=bucket, Key=key,
                         Body=json.dumps(manifest.to_dict(), indent=2).encode())
    return key

def load_manifest(s3_client, bucket, key) -> Manifest:
    return Manifest.from_dict(json.loads(s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()))
```

!!! note "`isoformat` is only for S3"
    The checkpoint keeps a raw `datetime` inside a dict perfectly well — measured. `json.dumps`
    cannot, so `to_dict` converts. If a payload never reaches S3, `to_dict` can leave the `datetime`
    alone.

That is `src/nested_payloads/minimal.py`, 21 lines of conversion across two types. Everything below
is what to reach for when that stops being enough.

## Sizing the alternative honestly

| | Lines | Buys you |
|---|---|---|
| Two methods per type | ~21 | Works today, obvious, nothing to learn |
| `codec.py` + `serdes.py` + `store.py` | ~256 | Never write a conversion again, at any depth |

At two dataclasses the machinery loses. It starts winning somewhere around five or six types, or
when the shapes change often enough that hand-written methods drift from their fields. Below that,
write the methods.

## One codec, two boundaries

A manifest usually crosses two boundaries in the same execution. The SDK checkpoints it as a step's
return value, and S3 stores it for a downstream job. Those must agree, so the conversion is written
once:

```text
          Manifest(status, bucket, files=[TrackedFile])
                          │
              codec.unstructure  ── plain JSON-safe values ──┐
                          │                                  │
        ┌─────────────────┴──────────────────┐               │
        ▼                                    ▼               │
NestedDataclassSerDes                  ManifestStore         │
  StepConfig(serdes=…)                   put_object          │
  checkpoint bytes                       S3 object           │
        │                                    │               │
        └────────── codec.structure ─────────┘ ◀─────────────┘
                          │
          Manifest(status, bucket, files=[TrackedFile])
```

!!! warning "Never persist the SDK's envelope to S3"
    `ExtendedTypeSerDes` emits a tagged format — `{"t":"m","v":{...}}` — which is an implementation
    detail of the SDK version. Writing that to S3 makes every downstream reader depend on the SDK's
    internals. Store plain JSON; a Glue job, Athena, or a person opening the object can read it.

The repo asserts the two agree:

```python
def test_stored_bytes_and_checkpoint_bytes_agree(fakes, ctx):
    key = store.save('run-1', NESTED)
    assert fakes.stored_json(key) == json.loads(NestedDataclassSerDes(Manifest).serialize(NESTED, ctx))
```

## Five approaches, and when each is right

| Approach | Reach for it when | Cost |
|---|---|---|
| `to_dict` / `from_dict` | **Default.** A handful of types in one Lambda | ~21 lines, no config |
| `NestedDataclassSerDes` | Many types, or shapes that change often | ~256 lines, no dependencies |
| `ExplicitSerDes` | The wire format is a contract — versioning, renames, computed fields | Two methods per type |
| `cattrs` | Discriminated unions, per-field hooks, validation on the way in | Two packages in the bundle |
| No dataclass at the boundary | The type is only used inside pure logic | Nothing, but you lose the type at the seam |

### `NestedDataclassSerDes` — type-hint driven, recursive

`structure` reads the target's type hints and rebuilds the tree. It handles nested dataclasses,
`list` / `set` / `frozenset` / `tuple` / `dict`, `X | None`, and `datetime` / `date` / `UUID` /
`Decimal` from their string forms.

```python
manifest = context.step(
    discover,
    name='discover',
    config=StepConfig(serdes=NestedDataclassSerDes(Manifest)),
)
```

Verified: the tree round-trips equal, `files[0]` is a `TrackedFile`, and `modified` is a `datetime`
rather than the ISO string it was on the wire.

### `ExplicitSerDes` — the type owns its format

When the JSON is a contract someone else reads, generating it from field names is fragile — renaming
a field silently changes the format. Declare it:

```python
def to_payload(self) -> dict:
    return {'v': 2, 'state': self.status, 'files': [...]}

@classmethod
def from_payload(cls, payload: dict) -> 'VersionedManifest':
    ...
```

A version tag in the payload is what lets `from_payload` read last month's objects.

### `cattrs` — when the shapes outgrow the codec

```python
converter = cattrs.Converter()
converter.register_unstructure_hook(datetime, lambda value: value.isoformat())
converter.register_structure_hook(datetime, lambda value, _type: datetime.fromisoformat(value))
```

!!! warning "cattrs does not handle `datetime` out of the box"
    Without those two hooks, `unstructure` leaves a `datetime` object and `json.dumps` raises
    `TypeError: Object of type datetime is not JSON serializable` — at the checkpoint, after the
    step body has run. Measured while writing this page.

With the hooks registered, cattrs emits the same wire form as the recursive codec, which the repo
asserts, so the store reads either.

### Keeping dataclasses off the boundary

The fourth option is to not cross it. Steps return and accept plain dicts; the dataclass exists only
inside pure logic, constructed on the way in and flattened on the way out. No serdes, no config, no
machinery — and no type at the seam, so nothing catches a field that quietly changed shape. Worth it
for a type used in one place, not for one that travels.

## Large payloads

A checkpoint counts against the batcher's 750 KB ceiling, and every step's return value is retained
for the function's `retention_period`. A manifest of a few thousand files will exceed that.

Store the payload in S3 and checkpoint the **key**, which is what
`src/nested_payloads/handler.py` does with its `persist` step. The SDK also ships
`aws_durable_execution_sdk_python.filesystem_serdes` for offloading large values; the repo does not
exercise it, so nothing here is measured about it.

## Where to go next

- [Steps](steps.md) — `StepConfig`, and what a checkpoint costs
- [Testing](testing.md) — asserting on a step's raw `.result` envelope
- [SDK Internals](sdk-internals.md) — the `TypeTag` enum, read from the shipped source
- [Reference](reference.md) — the `SerDes` surface
- [`serdes.py` in the SDK](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/serdes.py)
- [cattrs documentation](https://catt.rs/en/stable/)
