# SDK internals

What the Python durable execution SDK actually does, read from the shipped source rather than the
documentation. Every claim here was measured against the versions named below; re-measure before
trusting any of it against a newer release.

**Measured 2026-08-18** against `aws-durable-execution-sdk-python` **1.7.0** and
`aws-durable-execution-sdk-python-testing` **1.2.1**, both from PyPI.

```bash
uv run --with aws-durable-execution-sdk-python python -c \
  "import aws_durable_execution_sdk_python as s; print(s.__file__)"
```

## `@durable_step` curries — it does not inject an ambient context

There is no contextvar and no registry. The decorator captures your arguments and returns a
closure of one parameter. `context.py`:

```python
def durable_step(
    func: Callable[Concatenate[StepContext, Params], T],
) -> Callable[Params, Callable[[StepContext], T]]:
    def wrapper(*args, **kwargs):
        def function_with_arguments(context: StepContext):
            return func(context, *args, **kwargs)
        function_with_arguments._original_name = func.__name__
        return function_with_arguments
    return wrapper
```

So a decorated call runs nothing. `context.step` is what invokes the closure with a `StepContext`
and checkpoints the result:

Given `@durable_step def fetch_quota(step_context, account_id: str) -> int`:

```text
fetch_quota                          →  (account_id: str) -> ((StepContext) -> int)
fetch_quota("acct-42")               →  (StepContext) -> int
context.step(fetch_quota("acct-42")) →  int
```

A bare `fetch_quota(account_id)` with no `context.step(...)` around it is a silent no-op. The body
never executes, nothing is checkpointed, and you hold a function object where a value belongs.

## The lambda form is equivalent, and a nested `def` is better than both

`context.step` accepts any `Callable[[StepContext], T]`. Only that callable needs a parameter — the
function it calls needs nothing:

```python
context.step(lambda _: fetch_quota(account_id), name='fetch_quota')
```

A lambda cannot hold two statements, so anything that also logs uses a nested `def` in the handler.
`step_context` arrives as its only parameter with nothing to thread:

```python
def upload_report(step_context: StepContext):
    s3_client.upload_file(local_path, BUCKET, key)
    step_context.logger.info('uploaded %s', key)

context.step(upload_report, name='upload_report')
```

## The step name is metadata; replay identity is ordinal

`_resolve_step_name` reads a private attribute that only the decorator sets:

```python
return name or getattr(func, '_original_name', None)
```

It never reads `__name__`, despite a docstring claiming otherwise. Hand `context.step` an ordinary
named function and the step is unnamed.

Identity is a counter, not the name:

```python
def _create_step_id(self) -> str:
    new_counter: int = self._step_counter.increment()
    return self._create_step_id_for_logical_step(new_counter)   # blake2b of the counter
```

The name travels beside it in `OperationIdentifier(name=...)` and nothing in the SDK compares it.

**Do not design around that.** The SDK's own docstring says the id format is "subject to change
without notice", and AWS's step-design guidance states names are part of a step's deterministic
identity. Keep names static and unique, per the guidance, and treat the ordinal behavior as an
implementation detail you happen to have read.

## The default serializer carries a closed set of types

`serdes.py` `TypeTag`:

```text
None  str  int  float  bool  bytes  UUID  Decimal  datetime  date  tuple  list  dict  BatchResult
```

Anything else raises. A dataclass returned from a step fails at checkpoint time, after the body has
already run:

```yaml
SerDesError: Unsupported type: <class 'Manifest'>
```

`StepConfig(serdes=...)` is the supported fix. Delegate to `ExtendedTypeSerDes` inside rather than
`json.dumps`, or a `datetime` field comes back a string. See `src/landing_zone/serdes.py`, and
`tests/test_serdes.py` for the round-trip proof.

Nested dataclasses need explicit reconstruction — `asdict` flattens them going out, and a
constructor call only rebuilds the top level.

## A step costs two checkpoints, and the success one blocks

Under the default `AT_LEAST_ONCE_PER_RETRY`, `operation/step.py`:

```python
# START — queued, does not block
is_sync = (config.step_semantics is AT_MOST_ONCE_PER_RETRY)   # False by default
self.state.create_checkpoint(operation_update=start_operation, is_sync=is_sync)

# SUCCEED — is_sync=True by default, blocks the handler
self.state.create_checkpoint(operation_update=success_operation)
```

```bash
step body returns
       │
       ▼
create_checkpoint(SUCCEED, is_sync=True)  ── handler blocks ──┐
       │                                                      │
       ▼  queued                                              │
background batcher waits up to 100ms for a companion op       │
       │  limits: 750KB · 1.0s · 250 operations               │
       ▼                                                      │
lambda:CheckpointDurableExecution ── network round trip ──────┘
```

A lone sequential step pays that ~100ms idle wait before the flush, because the batcher polls for a
second operation and breaks on `queue.Empty`. The return value is then serialized into durable
state and retained for the function's `retention_period`.

So a step is for a side effect or a non-deterministic value. Pure computation on data already in
memory does not need one, and AWS says so directly: *"each extra step is an unnecessary
checkpoint."* Never give a log line its own step.

## `context.logger` is replay-aware; the root logger is what defeats it

One predicate decides it, `logger.py`:

```python
def _should_log(self) -> bool:
    return not self._is_replaying()
```

The root context starts in `REPLAY` when the execution has prior operations
(`execution.py`), and `_replay_aware` flips to `NEW` at the replay boundary:

```python
elif self.is_replaying() and not self._next_operation_exists():
    self._set_replay_status_new()
```

A `context.logger.info` positioned past the last checkpointed operation therefore runs in `NEW` and
prints on every pass. That is the usual cause of "my logs repeat".

The SDK wraps `logging.getLogger()` — the **root** logger. A module-level
`logger = logging.getLogger(__name__)` is a different object with no replay awareness and prints
every time.

A log inside a step body needs none of this. A succeeded checkpoint short-circuits the body, so the
code holding the log is never reached on replay.

## `wait_for_condition` threads state through the checkpoint

`check(state, check_context) -> state` runs as a durable step and its return is checkpointed.
`wait_strategy(state, attempt) -> WaitForConditionDecision` reads what the check just returned.
Order per cycle is check, then strategy, then suspend.

```text
initial_state ─► check ─► wait_strategy ─┬─ stop ─► return final state
      ▲                                  └─ continue(delay) ─► suspend ─┐
      └───────────────────────────────────────────────────────────────┘
```

`attempt` is 1-based and the first check runs immediately, before any wait. Index a delay schedule
with `attempt - 1` or element 0 is skipped silently.

State is restored from the checkpoint only when it is truthy:

```python
if checkpointed_result.is_started_or_ready() and checkpointed_result.result:
    current_state = deserialize(...)
else:
    current_state = self.config.initial_state
```

A check that returns `None` or another falsy value therefore restarts from `initial_state` every
attempt. Upstream issue #600 identifies exactly this and proposes keying on attempt count instead.
Keep the polling state a non-empty dict.

The local test runner drops this state entirely — see [testing.md](testing.md).
