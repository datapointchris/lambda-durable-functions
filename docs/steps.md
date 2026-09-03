# Steps

A step is the unit of durability. `context.step` runs a callable once, checkpoints what it returned,
and on every later replay hands back the checkpoint instead of running the callable again. Every
side effect in a durable handler belongs inside one.

This page is the complete guide to that one method: which of the three spellings to write, what to
log inside it, what it costs, how to configure it, and how to name it. [concepts.md](concepts.md)
covers replay itself, [waits.md](waits.md) covers the operations that suspend, and
[sdk-internals.md](sdk-internals.md) carries the shipped-source proof behind the claims below.

Measured against `aws-durable-execution-sdk-python` **1.7.0** and
`aws-durable-execution-sdk-python-testing` **1.2.1**, both from PyPI. Every sample is code from this
repository, and the test suite behind it passes. Re-measure before trusting any of it against a
newer release.

Upstream reading for this page:

- [Lambda durable functions guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
- [Durable execution SDK developer guide](https://docs.aws.amazon.com/durable-execution/)
- [aws/aws-durable-execution-sdk-python](https://github.com/aws/aws-durable-execution-sdk-python)

## Three spellings, and the nested `def` is the one to write

`context.step` accepts any callable of one parameter:

```python
def step(
    self,
    func: Callable[[StepContext], T],
    name: str | None = None,
    config: StepConfig | None = None,
) -> T: ...
```

Only that callable takes a parameter. The function it calls takes whatever arguments you like. Three
constructions satisfy the signature, and they are not equally good.

| Spelling | Statements | Gets `step_context` | Self-naming | Silent no-op |
| --- | --- | --- | --- | --- |
| `@durable_step` closure | many | yes, first parameter | yes | yes |
| `lambda _: ...` | one expression | discarded by convention | no | no |
| Nested `def` | many | yes, only parameter | no | no |

*Silent no-op* is the `@durable_step` failure below: forget `context.step` and nothing runs, and
nothing raises.

### `@durable_step` curries — a decorated call runs nothing

The decorator captures your arguments and returns a closure of one parameter. There is no
contextvar, no registry, and no ambient context. Source: [`context.py`][sdk-context].

Given `@durable_step def fetch_quota(step_context: StepContext, account_id: str) -> int`, the type
transforms twice before you get a value:

```text
fetch_quota                          →  (account_id: str) -> ((StepContext) -> int)
fetch_quota('acct-42')               →  (StepContext) -> int
context.step(fetch_quota('acct-42')) →  int
```

Measured on SDK 1.7.0: calling `fetch_quota('acct-42')` returns a `function`, the body
does not run, and the returned object carries `_original_name == 'fetch_quota'`.

!!! danger "A decorated call with no `context.step` around it is a silent no-op"

    The body never executes, nothing is checkpointed, and you are holding a function object where a
    value belongs. Nothing raises. The failure surfaces later as a wrong result or a side effect
    that never happened, far from the line that caused it.

    ```python
    quota = fetch_quota(account_id)                     # a closure, not an int
    quota = context.step(fetch_quota(account_id))       # an int
    ```

### The lambda form is equivalent and caps out at one expression

`context.step` does not care where the callable came from. Given an ordinary undecorated
`read_quota(account_id: str) -> int`:

```python
quota = context.step(lambda _: read_quota(account_id), name='read_quota')
```

The parameter is discarded, so the lambda cannot log, and a lambda cannot hold a second statement to
log from. It also carries no `_original_name`, so the `name=` argument is not optional — see
[Names are metadata that must stay static](#names-are-metadata-that-must-stay-static).

### A nested `def` is what this repo uses

Every step in this repository is a nested `def` inside the handler body. Two reasons, and the first
is the one that decides it.

**A step that logs needs two statements.** The work and the log line are two statements, so a lambda
cannot express them. Reaching for `@durable_step` to get the second statement drags in the currying
and the silent no-op above.

**`step_context` arrives as the only parameter, with nothing to thread.** The nested `def` closes
over the handler's locals directly, so there are no arguments to bind and nothing for the decorator
to curry. From `src/order_saga/handler.py`:

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    order = parse_order(event)

    def reserve_stock(step_context: StepContext) -> str:
        reservation_id = inventory_client.reserve(order.order_id, reserved_quantities(order))
        step_context.logger.info('reserved %s for %s', reservation_id, order.order_id)
        return reservation_id

    reservation_id = context.step(reserve_stock, name='reserve_stock')
```

`order` is a handler local. A `@durable_step` version would take it as a decorated parameter, curry
it, and read no better for it.

!!! note "The nested `def` is glue, and it is deliberately not unit tested"

    Each one calls a tested pure function, calls a client, and logs. It holds no branching, so
    there is nothing in it to test that the pure function does not already cover.
    [testing.md](testing.md) has the three-layer split that makes this work.

Use `@durable_step` when the same step body is genuinely shared between handlers and needs
arguments bound at each call site. That is the case it was built for, and it is rare.

## `step_context.logger` is the logger that cannot repeat

Three loggers are reachable from inside a handler, and they behave differently on replay.

| Logger | Suppressed while replaying | Repeats on replay |
| --- | --- | --- |
| `step_context.logger` | never needs to be | no — a succeeded body is not re-entered |
| `context.logger` | yes, until the replay boundary | yes, past the last checkpointed operation |
| `logging.getLogger(__name__)` | no | yes, every pass |

### A log inside a step body cannot repeat, because the body is short-circuited

A succeeded checkpoint returns its deserialized result without calling the function. The line
holding the log is never reached, so no replay can print it twice. This is the same mechanism that
makes a side effect safe inside a step, applied to the log line beside it.

`step_context.logger` also tags each record with the operation's identity. `Logger.from_log_info`
sets `executionArn`, `operationName`, `operationId`, `parentId` and `attempt` as `extra` fields, so
a retried step's second log line is distinguishable from its first by the `attempt` field alone.

A step body does run again in two cases: a retry, and an at-least-once step resumed after a lost
invocation. Both print. `_replay_aware` flips the context to `NEW` **before** any step whose body is
about to run real work, so `step_context.logger` is never suppressed for work that actually happens.

### `context.logger` suppresses only while the context is replaying

One predicate decides it, and it reads the context's replay status rather than the execution's. Past
the last checkpointed operation the context is `NEW`, so a `context.logger.info` positioned there
prints on every invocation. That is the usual cause of "my logs repeat".

Keep handler-level logging to things that are true of the whole execution, and put anything
per-operation inside the step that performs it. The traced mechanism is in
[sdk-internals.md](sdk-internals.md).

!!! warning "The root-logger trap"

    The SDK wraps `logging.getLogger()` — the **root** logger. A module-level
    `logger = logging.getLogger(__name__)` is a different object entirely, with no replay awareness
    and no operation metadata. It prints on every pass of every replay, and no configuration of the
    SDK changes that. `context.set_logger(...)` replaces the wrapped logger if you need a
    structured one.

## A step costs two checkpoints and a network round trip

Each `context.step` writes two operations to durable state, and the second one blocks the handler.
The sequencing is in [`operation/step.py`][sdk-step].

| Stage | Blocking | What it costs |
| --- | --- | --- |
| START, `AT_LEAST_ONCE_PER_RETRY` | no, queued | nothing on the critical path |
| START, `AT_MOST_ONCE_PER_RETRY` | yes | one batcher wait plus one round trip |
| Body | — | your own work |
| Serialize the return value | yes | the result is written into durable state |
| SUCCEED | yes | one batcher wait plus one round trip |

The batcher in [`state.py`][sdk-state] waits for a companion operation before flushing. Its limits
are 750KB, a 1.0-second window, and 250 operations, and it polls in 100ms slices and breaks on an
empty slice. A lone sequential step therefore pays roughly 100ms of idle wait before its flush, then
the `lambda:CheckpointDurableExecution` round trip. [sdk-internals.md](sdk-internals.md) has the
traced path and the source.

The return value is serialized into durable state and retained for the function's
`retention_period`. A step that returns a large object pays for that object twice — once in
checkpoint bytes, once again in every replay's deserialization.

Two rules fall out of the cost, and AWS states the first one directly in the
[durable functions best practices](https://docs.aws.amazon.com/lambda/latest/dg/durable-best-practices.html):
*"each extra step is an unnecessary checkpoint."*

**Pure computation does not belong in a step.** Data already in memory can be recomputed on replay
for free. Every pure function in this repo — `order_total_cents`, `build_manifest`, `plan_step_retry`
— is called from the handler body or from inside a step, never wrapped in one of its own.

**A log line never gets its own step.** Log from inside the step that did the work.

What does belong in a step:

- a side effect that must not repeat — a write, a charge, a job start, a message
- a non-deterministic read — a clock, a random value, a listing, an API response
- anything whose result later code branches on, so the branch is stable across replays

The settle check in `src/landing_zone/handler.py` is the second case. It reads the clock, so it runs
inside a durable operation to be replay-stable:

```python
    def settle_check(_state: dict, check_context: WaitForConditionCheckContext) -> dict:
        """Reads the clock, so it must run inside the step to be replay-stable."""
        landed = list_landed_objects()
        newest = max((o['modified_epoch'] for o in landed.values()), default=None)
        quiet = is_quiet(newest, time.time(), QUIET_PERIOD_SECONDS)
        check_context.logger.info('landed=%d quiet=%s', len(landed), quiet)
        return {'object_count': len(landed), 'quiet': quiet}
```

`is_quiet` is pure and takes the clock reading as an argument, so it is unit tested with plain
values. `wait_for_condition` is covered in [waits.md](waits.md).

## `StepConfig` in full

`StepConfig` is a frozen dataclass of three fields, from [`config.py`][sdk-config]:

```python
@dataclass(frozen=True)
class StepConfig:
    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    step_semantics: StepSemantics = StepSemantics.AT_LEAST_ONCE_PER_RETRY
    serdes: SerDes | None = None
```

Passing no config at all is `StepConfig()`, which is not the same as passing nothing meaningful —
the defaults are real policy. [reference.md](reference.md) has the full config surface.

### `retry_strategy` — the default retries everything six times

A retry strategy is a plain callable of `(error, attempts_made)` returning a `RetryDecision`. It is
called with the checkpointed attempt plus one, so the first failure sees `attempts_made == 1` and a
budget of `max_attempts=N` yields exactly N executions of the body.

```python
RetryDecision.retry(Duration.from_seconds(5))   # should_retry=True
RetryDecision.no_retry()                        # should_retry=False
```

!!! warning "No `retry_strategy` means `RetryPresets.default()`, which retries every exception"

    Six attempts, 5s initial delay, doubling to a 60s cap, full jitter. The retryable-error filter
    defaults to the pattern `.*`, so a permanently poisoned step spends the whole budget — minutes
    of real wall clock — before the execution can fail. In a local test that is minutes of a test
    run, which is why every step in this repo whose failure path is exercised carries an explicit
    strategy built from module constants a test can shrink.

The presets in [`retries.py`][sdk-retries]:

| Preset | Attempts | Initial delay | Cap | Backoff | Jitter |
| --- | --- | --- | --- | --- | --- |
| `RetryPresets.none()` | 1 | — | — | — | — |
| `RetryPresets.default()` | 6 | 5s | 60s | ×2 | full |
| `RetryPresets.transient()` | 3 | 5s | 5min | ×2 | half |
| `RetryPresets.resource_availability()` | 5 | 5s | 5min | ×2 | full |
| `RetryPresets.critical()` | 10 | 1s | 60s | ×1.5 | none |
| `RetryPresets.linear()` | 6 | 1s, +1s each | 5min | linear | none |
| `RetryPresets.fixed(interval)` | 5 | interval | interval | ×1 | none |

`create_retry_strategy(RetryStrategyConfig(...))` builds a custom one. Its `retryable_errors` takes
strings or compiled patterns matched against `str(error)`, and `retryable_error_types` takes
exception classes matched by `isinstance`. The match-all default applies only when both are `None`.

Writing the decision as a pure function and mapping it onto the SDK type at the boundary keeps the
policy unit testable. From `src/flaky_api_sync/logic.py` and `src/flaky_api_sync/handler.py`:

```python
def plan_step_retry(error: Exception, attempts_made: int, limits: RetryLimits) -> RetryPlan:
    """Back off on a throttle or a 5xx; stop dead on anything else.

    A 4xx is the partner rejecting the request itself, so an identical second
    request fails identically. Retrying it spends the whole budget to arrive at
    the same failure several minutes later.
    """
    if not isinstance(error, PartnerApiError) or not is_retryable_status(error.status_code):
        return NO_RETRY
    if attempts_made >= limits.max_attempts:
        return NO_RETRY
    if error.retry_after_seconds is not None:
        return RetryPlan(should_retry=True, delay_seconds=max(error.retry_after_seconds, 1))
    delay = backoff_delay_seconds(attempts_made, limits.base_seconds, limits.cap_seconds)
    return RetryPlan(should_retry=True, delay_seconds=delay)


def to_retry_decision(plan: RetryPlan) -> RetryDecision:
    if not plan.should_retry:
        return RetryDecision.no_retry()
    return RetryDecision.retry(Duration.from_seconds(plan.delay_seconds))


def partner_retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
    """The `StepConfig` retry policy for every call to the partner."""
    limits = RetryLimits(REQUEST_MAX_ATTEMPTS, REQUEST_BASE_DELAY_SECONDS, REQUEST_MAX_DELAY_SECONDS)
    return to_retry_decision(plan_step_retry(error, attempts_made, limits))
```

The classification is what the tests assert on. From `tests/test_flaky_api_sync.py`:

```python
def test_a_permanent_error_is_not_retried(partner):
    """The whole point of classifying: one attempt, then the execution fails."""
    api, dynamo = partner
    api.request_export_script = [forbidden()]

    result = run({'changedSince': CHANGED_SINCE})

    assert result.status is InvocationStatus.FAILED
    assert result.get_step('request_export').attempt == 1
    assert api.calls['request_export'] == 1
    assert dynamo.batches == []


def test_a_retryable_error_still_stops_at_the_attempt_budget(partner):
    api, dynamo = partner
    api.request_export_script = [unavailable()]

    result = run({'changedSince': CHANGED_SINCE})

    assert result.status is InvocationStatus.FAILED
    assert result.get_step('request_export').attempt == 3
    assert dynamo.batches == []
```

The budget in that second test is 3 rather than the handler's `REQUEST_MAX_ATTEMPTS = 5`, because
`tests/conftest_flaky_api_sync.py` monkeypatches the module constants down before the run. That is
the shrinking the warning above asks for, and it is the reason the file's retry tests cost seconds
instead of minutes.

!!! note "`StepOperation.attempt` counts executions, not retries"

    The testing harness increments it on RETRY, SUCCEED and FAIL alike, so a step that succeeds
    first time reports `attempt == 1`, and two retries followed by success reports `3`.

Three more facts about retries worth knowing before you set a delay:

- A delay below one second is clamped up to one second by the executor, with a warning logged. A
  zero-second retry still costs a real second.
- A retry does not sleep in place. It checkpoints RETRY and suspends the whole execution, so the
  handler body re-enters from the top on the next invocation.
- `with_retry` retries a *block* of durable operations rather than one step, re-running everything in
  the block from the beginning. `src/flaky_api_sync/handler.py` uses it to re-mint an expired
  download URL and start the download over, which a step-level retry cannot express.

### `step_semantics` — the two behave identically until an invocation dies

| | `AT_LEAST_ONCE_PER_RETRY` (default) | `AT_MOST_ONCE_PER_RETRY` |
| --- | --- | --- |
| START checkpoint | queued, non-blocking | synchronous, blocks the body |
| Body raises an exception | body re-runs on the retry | body re-runs on the retry |
| Invocation dies mid-body | body runs again | body is not re-entered |
| On that replay | ordinary execution | `StepInterruptedError` into the retry strategy |
| Cost | one blocking checkpoint per step | two blocking checkpoints per step |

!!! danger "An example that only demonstrates a failing body demonstrates nothing about semantics"

    The two settings are indistinguishable when a step body raises an ordinary exception. Both
    re-run it. They diverge only when the invocation dies mid-step, leaving a STARTED checkpoint
    behind: at-most-once refuses to re-enter the body and raises `StepInterruptedError` into the
    retry strategy, while at-least-once simply executes it again.

**Use the at-least-once default when the side effect is safe to repeat.** A keyed write, a
deterministic PUT, a read. The START checkpoint is queued rather than blocking, so it is the cheaper
of the two. `reserve_stock` above is keyed by order id, so a second reserve writes the same row.

**Use at-most-once when repeating the side effect is worse than failing.** From
`src/order_saga/handler.py`, the charge step, whose start is checkpointed synchronously so the next
invocation can recognize an interrupted charge. `CHARGE_SEMANTICS` is a module constant holding
`StepSemantics.AT_MOST_ONCE_PER_RETRY`, so a test can flip it:

```python
        charge_id = stage.step(
            charge_card,
            name='charge_card',
            config=StepConfig(
                step_semantics=CHARGE_SEMANTICS,
                retry_strategy=charge_retry_strategy,
            ),
        )
```

The retry strategy is where at-most-once actually pays off. `StepInterruptedError` arrives as the
`error` argument, and the strategy declines to retry it:

```python
def should_retry_charge(*, interrupted: bool, attempts_made: int, max_attempts: int) -> bool:
    """An interrupted charge may already have reached the processor, so it is never retried.

    The saga cannot tell a lost response from a lost request. Retrying resolves
    that ambiguity in the customer's disfavor; failing the stage sends it to
    compensation, which asks the processor what actually happened.
    """
    if interrupted:
        return False
    return attempts_made < max_attempts


def charge_retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
    if should_retry_charge(
        interrupted=isinstance(error, StepInterruptedError),
        attempts_made=attempts_made,
        max_attempts=CHARGE_MAX_ATTEMPTS,
    ):
        return RetryDecision.retry(Duration.from_seconds(CHARGE_RETRY_DELAY_SECONDS))
    return RetryDecision.no_retry()
```

The paired tests are the proof, and the second one is what makes the first non-vacuous. From
`tests/test_order_saga.py`:

```python
def test_an_interrupted_charge_runs_its_body_exactly_once(saga_clients):
    """The whole point of AT_MOST_ONCE_PER_RETRY."""
    payments, _inventory, _shipping = saga_clients
    payments.interrupt_next_charge = True

    run(checkout_event())

    assert len(payments.charge_attempts) == 1


def test_the_default_semantics_charge_the_card_twice(saga_clients, monkeypatch):
    """Why the charge step is configured at all."""
    payments, _inventory, _shipping = saga_clients
    monkeypatch.setattr(handler_module, 'CHARGE_SEMANTICS', StepSemantics.AT_LEAST_ONCE_PER_RETRY)
    payments.interrupt_next_charge = True

    result = run(checkout_event())

    assert payload(result)['status'] == 'placed'
    assert len(payments.charge_attempts) == 2
```

!!! note "How a lost invocation is simulated, since testing 1.2.1 has no fault injection"

    `interrupt_next_charge` makes the payments fake raise `TimedSuspendExecution` from inside the
    step body, after recording the charge. It works because that exception derives from
    `BaseException`: `StepOperationExecutor.execute` catches `except Exception` and misses it, and
    `state.wrap_user_function` re-raises `SuspendExecution` explicitly. The step is left with a
    STARTED checkpoint and no result, which is exactly the state a dead invocation leaves behind.

!!! warning "At-most-once is not exactly-once"

    It guarantees the body is not re-entered after an interruption. It tells you nothing about
    whether the side effect completed before the invocation died. The money may or may not have
    moved. Resolving that is compensation's job, and in this saga compensation asks the processor
    rather than assuming either way — `refund_by_key` looks the charge up by idempotency key and
    returns `None` when the processor holds nothing.

`StepInterruptedError` is an `InvocationError`, so it is re-raised unchanged rather than converted,
and it reaches Lambda as a request to retry the invocation. A parent catching stage failures must
catch `CallableRuntimeError` only, and let the interruption through.

### `serdes` — the default codec carries a closed set of types

With `serdes=None` the step falls back to `EXTENDED_TYPES_SERDES` in [`serdes.py`][sdk-serdes],
which carries exactly these types and no others:

```text
None  str  int  float  bool  bytes  UUID  Decimal  datetime  date  tuple  list  dict  BatchResult
```

A dataclass is not among them:

```yaml
SerDesError: Unsupported type: <class 'Manifest'>
```

!!! danger "A serialization failure is fatal and arrives after the body has already run"

    `serialize` is called on the return value *after* `wrapped_user_func` returns. Its failure is
    wrapped in an `ExecutionError`, and `StepOperationExecutor.execute` re-raises `ExecutionError`
    without consulting the retry strategy. So the side effect happened, the step failed, no retry
    was attempted, and the execution ends. Return a supported type or attach a `SerDes`.

`StepConfig(serdes=...)` is the supported fix. The same codec deserializes the checkpoint on every
replay, so it has to round-trip. From `src/landing_zone/handler.py`:

```python
    manifest = context.step(
        freeze_manifest,
        name='freeze_manifest',
        config=StepConfig(serdes=DataclassSerDes(Manifest)),
    )
```

And the implementation, from `src/landing_zone/serdes.py`. It delegates to the SDK's own extended
codec rather than `json.dumps`, which is what keeps a `datetime` field a `datetime` on the way back:

```python
class DataclassSerDes(SerDes):
    def __init__(self, cls: type) -> None:
        self._cls = cls
        self._inner: ExtendedTypeSerDes = ExtendedTypeSerDes()

    def serialize(self, value: Any, serdes_context: SerDesContext) -> str:
        return self._inner.serialize(asdict(value), serdes_context)

    def deserialize(self, data: str, serdes_context: SerDesContext) -> Any:
        return self._cls(**self._inner.deserialize(data, serdes_context))
```

Nested dataclasses need explicit reconstruction. `asdict` flattens them on the way out and the
constructor call only rebuilds the top level, so an inner dataclass comes back as a plain dict.
`tests/test_serdes.py` has the round-trip proof.

!!! note "Checkpoint payloads are capped at 256KB"

    `CHECKPOINT_SIZE_LIMIT_BYTES` in [`constants.py`][sdk-constants] is 256 × 1024. Beyond it,
    child contexts switch to replaying children and `FileSystemSerDes` overflows to a file. A step
    returning a large collection is a step returning a key to that collection instead.

## Names are metadata that must stay static

The `name` argument is metadata attached to the operation. Replay identity is a per-context ordinal
counter, and nothing in the SDK compares names — see [sdk-internals.md](sdk-internals.md) for the
source. Do not design around that. AWS's step-design guidance treats a name as part of a step's
deterministic identity, and the SDK's own docstring calls the id format subject to change.

Names are load-bearing for three readers even though the runtime ignores them:

- The test harness addresses operations by name. `result.get_step('request_export')` is how every
  assertion in this repo reaches a step.
- CloudWatch sees them. `step_context.logger` puts the name in every record as `operationName`.
- A human reading an execution history sees them, and nothing else identifies the operation.

### An unnamed step is unaddressable, and nothing warns you

Name resolution is one line, from [`context.py`][sdk-context]:

```python
return name or getattr(func, '_original_name', None)
```

Only `@durable_step` sets `_original_name`. `__name__` is never read, despite a docstring claiming
otherwise. Measured on SDK 1.7.0:

| Callable | `name=` omitted | Resolved name |
| --- | --- | --- |
| `@durable_step` closure | yes | `'fetch_quota'` |
| Plain named `def` | yes | `None` |
| `lambda _: ...` | yes | `None` |
| Any of the three | `name='charge_card'` | `'charge_card'` |

So a nested `def` handed to `context.step` without `name=` produces an unnamed operation. Every
`context.step` call in this repository passes `name=` explicitly.

### A dynamic name breaks the readers, silently

!!! warning "Never build a step name from an item id, a timestamp, or a UUID"

    The runtime does not object, so nothing fails at the call site. The damage lands on the readers:
    `result.get_step('...')` in a test cannot be written against a name that changes per run, and a
    CloudWatch query grouping by `operationName` sees one group per execution.

    For per-item names inside a fan-out, `MapConfig.item_namer` is the supported hook — see
    [fan-out.md](fan-out.md).

Duplicate names are the same class of problem. `get_operation_by_name` scans in execution order and
returns the first match, so two steps sharing a name make one of them unreachable from a test, with
no error raised. Keep names unique within a context.

!!! note "`get_step` searches one level only"

    It scans the operations of the result or context it is called on. A step nested inside a child
    context, a map iteration, or a parallel branch raises `DurableFunctionsTestError` from the
    top-level result. Reach it through `result.get_context(name).get_step(name)`, or with
    `get_all_operations()`, which recurses and returns operations in reverse order.
    [testing.md](testing.md) and [fan-out.md](fan-out.md) cover the nesting.

## Where to go next

| Page | Answers |
| --- | --- |
| [index.md](index.md) | What this site covers, and the example the whole of it is built on |
| [concepts.md](concepts.md) | What replay is, and why the handler body re-enters |
| [waits.md](waits.md) | `wait`, `wait_for_condition`, and `wait_for_callback` |
| [fan-out.md](fan-out.md) | `map`, `parallel`, and reading a `BatchResult` |
| [testing.md](testing.md) | Driving a handler through `DurableFunctionTestRunner` |
| [reference.md](reference.md) | Every config dataclass and its fields |
| [sdk-internals.md](sdk-internals.md) | The shipped source behind the claims on this page |
| [typing-and-tooling.md](typing-and-tooling.md) | basedpyright, ruff, and the context parameter |

Upstream, on how to shape a step rather than how to write one:

- [Durable execution best practices](https://docs.aws.amazon.com/durable-execution/patterns/best-practices/)
- [Lambda best practices](https://docs.aws.amazon.com/lambda/latest/dg/durable-best-practices.html)

[sdk-context]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/context.py
[sdk-config]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/config.py
[sdk-retries]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/retries.py
[sdk-serdes]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/serdes.py
[sdk-step]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/operation/step.py
[sdk-state]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/state.py
