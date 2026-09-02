# Testing

This page is how a durable Lambda gets a test suite that proves something. It covers why a durable
handler needs a different assertion from an ordinary one, which of the four runners a given test
needs, the three layers a suite splits into, everything a result object will tell you, how to drive
a callback from a test, and the harness limits that shape every test in this repository.

It is for someone who already has a handler and wants it under test without changing its shape.

Every fact here was measured on **2026-08-19** against `aws-durable-execution-sdk-python` **1.7.0**
and `aws-durable-execution-sdk-python-testing` **1.2.1**. Every snippet is quoted from a test in
this repository that runs. The whole suite is **147 passed, 1 xfailed in 128.95s**, measured the
same day.

Background reading, none of it repeated here and all of it listed on the [index](index.md):
[concepts.md](concepts.md) for what replay is,
[steps.md](steps.md) for step semantics and retry configuration, [waits.md](waits.md) for waits and
callbacks, [fan-out.md](fan-out.md) for `map` and `parallel`, [reference.md](reference.md) for the
API surface, [sdk-internals.md](sdk-internals.md) for what the SDK does underneath, and
[typing-and-tooling.md](typing-and-tooling.md) for the type-checker findings. The official material
is the [Lambda durable functions
guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html), the [SDK developer
guide](https://docs.aws.amazon.com/durable-execution/), and the [SDK
source](https://github.com/aws/aws-durable-execution-sdk-python).

## The assertion that matters is a call count, not a return value

An ordinary Lambda handler runs once. A durable handler runs once **per invocation**, and one
execution spans many invocations. Each suspension ends an invocation, and the next one calls
`lambda_handler(event, context)` again from the top with the same event. The SDK replays the
operations it finds in the checkpoint history — a succeeded `context.step` returns its checkpointed
result without executing its body — but the handler code *between* those operations has no
checkpoint and simply runs again.

```text
invocation 1   lambda_handler(event, context)
               ├─ context.step(check_leader)          executes → checkpoint SUCCEEDED
               ├─ triggering_keys = {...}             executes  (plain handler code)
               └─ context.wait_for_condition(settle)  executes → checkpoint RETRY, then suspend
                                                                 ▲ the invocation ends here
invocation 2   lambda_handler(event, context)         ← same event, from the top
               ├─ context.step(check_leader)          replayed → returns the checkpoint, body skipped
               ├─ triggering_keys = {...}             executes AGAIN
               └─ context.wait_for_condition(settle)  executes → poll 2, suspend again
```

So a test asserting only on the returned payload passes whether the card was charged once or three
times. The failure mode unique to durable functions is a side effect sitting in handler code rather
than inside a step, and the only thing that catches it is a **count on a fake**.

`tests/test_replay.py` asserts exactly that, and its module docstring says why:

```python
"""The failure mode unique to durable functions: a side effect outside a step.

The handler body re-runs from the top after every suspension. A step body does
not, because a succeeded checkpoint short-circuits it. So the assertion that
matters is a call count on a fake, never a return value.
"""
```

```python
def test_the_ingest_job_starts_exactly_once(clients):
    """A second job run means a side effect escaped its step and replayed."""
    s3, glue, _lam = clients

    run_settled(s3)

    assert len(glue.job_runs) == 1


def test_a_replayed_step_returns_its_checkpoint_rather_than_re_running(clients):
    s3, _glue, _lam = clients

    result = run_settled(s3)

    # One listing for the settle check, one for freeze_manifest. A third would
    # mean a checkpointed step body executed again on replay.
    assert s3.paginate_calls == 2
    assert result.result is not None
    assert json.loads(result.result)["objects"] == 2
```

!!! danger "A once-only assertion passes for the wrong reason when no replay happened"

    `assert len(glue.job_runs) == 1` is also true of an execution that never suspended. Every
    example in this repository therefore carries a guard test that proves the body re-entered.
    Without it the whole replay section is decoration. The guard is
    [described in full below](#the-guard-test-is-what-makes-a-once-only-assertion-mean-anything).

## Four runners ship, and one of them covers almost everything

`aws_durable_execution_sdk_python_testing` exports four runners. All of them produce the same
`DurableFunctionTestResult`, so assertions written against one work against another.

| Runner | Drives | AWS | Reach for it when |
| --- | --- | --- | --- |
| `DurableFunctionTestRunner` | The real runtime, in-process | No | Always, unless a row below applies |
| `DurableChildContextTestRunner` | One `@durable_with_child_context` block | No | A block stands on its own |
| `DurableFunctionCloudTestRunner` | A deployed function, over boto3 | Yes | The harness cannot model it |
| `WebRunner` | An HTTP server over the local service | No | Something outside pytest invokes it |

### `DurableFunctionTestRunner` is the default and runs the real runtime

It is not a mock. The constructor wires a scheduler, an in-memory execution store, a checkpoint
processor, an in-memory service client, an in-process invoker and an executor. Your handler runs
against the genuine SDK, which suspends and resumes it exactly as the service would.

```python
def run(event: dict):
    with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
        return runner.run(input=json.dumps(event), timeout=30)
```

`input` is a **string**, not a dict — `json.dumps` is not optional. The context manager stops the
scheduler on exit; a runner you build without `with` needs `runner.close()`.

The constructor takes one keyword worth knowing:

```python
DurableFunctionTestRunner(handler, poll_interval: float = 1.0)
```

`poll_interval` is what `wait_for_callback` polls the execution history with. It costs a full
second per poll by default, which dominates any callback test whose real work is milliseconds. The
approval-gate suite builds its runner with `poll_interval=0.05` and cut roughly 1.3s of overhead
per run:

```python
def gate_runner() -> DurableFunctionTestRunner:
    """The runner polls the history for the callback id, and its 1s default
    interval is longer than everything else these tests do put together."""
    return DurableFunctionTestRunner(handler_module.lambda_handler, poll_interval=0.05)
```

### `DurableChildContextTestRunner` tests one block in isolation

It is a five-line subclass. It wraps the decorated block in a throwaway `@durable_execution`
handler, then behaves like the runner above:

```python
class DurableChildContextTestRunner(DurableFunctionTestRunner):
    """Test a durable block, annotated with @durable_with_child_context, in isolation."""

    def __init__(self, context_function, *args, **kwargs):
        @durable_execution
        def handler(event, context: DurableContext):
            return context_function(*args, **kwargs)(context)

        super().__init__(handler)
```

The arguments you pass to the runner are the arguments the block takes, because
`durable_with_child_context` curries: calling the decorated function returns a
`Callable[[DurableContext], T]` rather than running anything. No example here uses it — the child
contexts in this repository are closures over handler state rather than free functions, so they
have nothing to be tested in isolation *from*. Reach for it when a block is a top-level function
with its own arguments.

### `DurableFunctionCloudTestRunner` runs against deployed AWS

```python
DurableFunctionCloudTestRunner(
    function_name: str,
    region: str = 'us-west-2',
    lambda_endpoint: str | None = None,
    poll_interval: float = 1.0,
)
```

`run()` calls `lambda_client.invoke(InvocationType='RequestResponse')`, reads the
`DurableExecutionArn` off the response, then polls `GetDurableExecution` and
`GetDurableExecutionHistory` until the execution finishes.
`DurableFunctionTestResult.from_execution_history` converts those two responses into the same
result object the local runner returns, so the same assertions apply.

!!! warning "Nothing on this site is verified against real Lambda"

    This workspace has no AWS account. Every measurement here is from the local harness. Where the
    harness and the service could differ, the page says so rather than guessing.

### `WebRunner` puts the local service behind HTTP

`WebRunner(WebRunnerConfig(...))` starts an HTTP server that speaks the durable-execution API, so
something other than pytest can invoke the function — a `curl`, a SAM local container, an
integration harness in another language. The package installs it as a console script:

```bash
dex-local-runner start-server --host localhost --port 5000 --store-type sqlite
dex-local-runner invoke --function-name my-function --input '{"runDate": "2026-08-19"}'
dex-local-runner get-durable-execution-history --durable-execution-arn <arn>
```

`--store-type` takes `memory` (the default), `filesystem` or `sqlite`. Only the last two survive a
restart, which is the reason to prefer them here — an execution suspended for ten minutes outlives
the server process.

## Three layers, and only the outermost needs a runner

```text
# src/<example>/                                          tested by
# ──────────────────────────────────────────────────────────────────────
logic.py     is_quiet · build_manifest                    layer 1: plain pytest
             next_poll_delay_seconds · risk_band
             classify_gate_failure · plan_step_retry
#       ▲ no SDK import, no clients, no context
#       │ called by
handler.py   list_landed_objects · write_manifest         layer 2: a fake client
             start_ingest_job · is_leader
#       ▲ module-scope client, swapped with monkeypatch
#       │ called by
handler.py   lambda_handler                               layer 3: a runner
               def settle_check(...) · freeze_manifest     glue only, no branching
               def publish_and_start(...)
```

Most assertions belong in layer one. `tests/test_logic.py` runs in hundredths of a second with no
fakes and no runtime, and every example in this repository puts its real decisions there:

```python
"""Layer 1: pure logic, no SDK, no runner, no fakes."""

def test_a_drop_is_quiet_once_nothing_has_landed_for_the_period():
    assert is_quiet(NOW - 300, NOW, quiet_seconds=300) is True


def test_a_recent_arrival_keeps_the_drop_open():
    assert is_quiet(NOW - 299, NOW, quiet_seconds=300) is False


def test_an_empty_prefix_is_never_quiet():
    assert is_quiet(None, NOW, quiet_seconds=300) is False
```

The rule that keeps layer one large: **a decision the handler makes is a function in `logic.py`**.
The approval gate cannot tell a reviewer's rejection from a lapsed review window from a dead
approval service by exception type, because all three arrive as one `CallableRuntimeError`. That
classification is a pure function, so all three outcomes are settled in three calls and no runtime:

```python
def test_a_silent_service_is_told_apart_from_a_lapsed_window():
    assert classify_gate_failure(HEARTBEAT_TIMEOUT_MESSAGE) == 'review-service-silent'
    assert classify_gate_failure(WINDOW_TIMEOUT_MESSAGE) == 'window-lapsed'
    assert classify_gate_failure('needs a backfill plan first') == 'reviewer-rejected'
```

### The nested `def` is deliberately not unit tested

A closure inside the handler cannot be imported, so it cannot be reached directly. That is fine
exactly while it holds no logic — it calls a tested function and logs. A branch appearing inside one
is the signal to lift that branch into `logic.py`, not to make the closure reachable.

```python
def score(step_context: StepContext) -> dict:
    rows = load_feature_rows(batch)
    scorable, rejected = partition_scorable(rows)
    probabilities = invoke_scoring_endpoint(scorable)
    step_context.logger.info(
        'batch %d: %d scorable, %d rejected', index, len(scorable), len(rejected)
    )
    return {'scored': score_rows(scorable, probabilities), 'rejected': list(rejected)}
```

Four calls and a log line, no branching. `partition_scorable` and `score_rows` are tested directly
at layer one; `load_feature_rows` and `invoke_scoring_endpoint` are client wrappers, asserted at
layer two through the fake's own call log.

## The handler never changes shape to become testable

The handler keeps the shape any Lambda reviewer expects. Module-scope clients, module-scope
configuration, `lambda_handler(event, context)` at module level. No factory, no dependency-injection
container, no `create_handler(clients)` wrapper. Testability comes from `monkeypatch`, because the
handler body resolves those names as **module globals at call time**.

```python
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')
lambda_client = boto3.client('lambda')

LANDING_BUCKET = os.environ['LANDING_BUCKET']
INGEST_JOB_NAME = os.environ['INGEST_JOB_NAME']

QUIET_PERIOD_SECONDS = 300
POLL_DELAYS_SECONDS = (60,)
STEADY_POLL_DELAY_SECONDS = 300
```

Two things happen at import time there, and both are usually what blocks testing: `boto3.client`
runs, and `os.environ[...]` runs. Both are satisfiable without touching the handler.

### `conftest.py` sets the environment at collection time, before the import

pytest imports `conftest.py` before it imports any test module, and a test module's import of the
handler is what triggers the handler's own import. So the environment is set at the top of
`conftest.py`, above the handler import, and the whole file is ordered deliberately:

```python
"""Environment and fakes.

The handler reads its configuration and builds its boto3 clients at import time,
which is the conventional Lambda shape. Both have to be satisfied before the
module is imported, so the environment is set here at collection time and the
clients are swapped per test with `monkeypatch.setattr`.
"""

import datetime as dt
import os

os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-2')
os.environ.setdefault('LANDING_BUCKET', 'test-landing')
os.environ.setdefault('LANDING_PREFIX', 'landing/')
os.environ.setdefault('MANIFEST_PREFIX', 'manifests/')
os.environ.setdefault('INGEST_JOB_NAME', 'test-ingest-job')
os.environ.setdefault('AWS_LAMBDA_FUNCTION_NAME', 'landing-zone-trigger')
os.environ.setdefault('AWS_LAMBDA_FUNCTION_VERSION', '7')

import pytest  # noqa: E402

from landing_zone import handler as handler_module  # noqa: E402
```

The two `# noqa: E402` markers are the price. `E402` is "module level import not at top of file",
and it is exactly what the file is doing on purpose.

!!! warning "isort will reorder the handler import above the environment, and the failure is a `KeyError`"

    ruff's import sorter has no idea that `from batch_scoring import handler` must follow the
    `os.environ` block. In a test module it will sort them into the wrong order and the handler's
    `os.environ['FEATURE_BUCKET']` raises at collection. `tests/conftest_batch_scoring.py`
    therefore imports the handler itself and re-exports it, so no test module ever imports it
    directly:

    ```python
    """...the environment is set here and the handler is imported
    and re-exported from here — a test module importing the handler directly would
    be sorted above this import and find the environment empty.
    """
    ```

### `monkeypatch.setattr` swaps every seam, per test

```python
@pytest.fixture
def clients(monkeypatch):
    """Swap every module-scope client, freeze the clock, shrink the delays.

    The handler keeps its conventional shape because the seams are module
    attributes rather than constructor parameters.
    """
    s3, glue, lam = FakeS3(), FakeGlue(), FakeLambda()
    monkeypatch.setattr(handler_module, 's3_client', s3)
    monkeypatch.setattr(handler_module, 'glue_client', glue)
    monkeypatch.setattr(handler_module, 'lambda_client', lam)
    monkeypatch.setattr(handler_module.time, 'time', lambda: EPOCH.timestamp())
    monkeypatch.setattr(handler_module, 'QUIET_PERIOD_SECONDS', 300)
    monkeypatch.setattr(handler_module, 'POLL_DELAYS_SECONDS', (1,))
    monkeypatch.setattr(handler_module, 'STEADY_POLL_DELAY_SECONDS', 1)
    return s3, glue, lam
```

Three different kinds of seam, all reached the same way: a client, a clock, and a duration. Nothing
about the handler is aware of any of it.

### A fixture in a non-`conftest` module is invisible to pytest

Each example here has its own fakes, and putting five examples' fakes in one `conftest.py` would
make every test file import every fake. The obvious fix — `tests/conftest_order_saga.py` with the
fixtures in it — does not work: **pytest resolves fixtures from `conftest.py` files and from the
test module's own namespace, and nowhere else.** Importing the fixture function by name to fix that
trips ruff `F401` (a fixture parameter is not a reference) and then `F811` at every use site.

`pytest_plugins = ['conftest_order_saga']` does register it, but the test module's own
`from conftest_order_saga import ...` runs first and produces
`PytestAssertRewriteWarning: Module already imported so cannot be rewritten`.

What works, with no warning and no `noqa`: the shared module exports a **plain factory**, and each
test module wraps it in a three-line fixture.

```python
# tests/conftest_order_saga.py
def install_fakes(monkeypatch) -> tuple[FakePayments, FakeInventory, FakeShipping]:
    """Swap every module-scope client and shrink every delay on the path to one second."""
    monkeypatch.setattr(Executor, 'RETRY_BACKOFF_SECONDS', 1)
    payments, inventory, shipping = FakePayments(), FakeInventory(), FakeShipping()
    monkeypatch.setattr(handler_module, 'payments_client', payments)
    monkeypatch.setattr(handler_module, 'inventory_client', inventory)
    monkeypatch.setattr(handler_module, 'shipping_client', shipping)
    monkeypatch.setattr(handler_module, 'CHARGE_RETRY_DELAY_SECONDS', 1)
    monkeypatch.setattr(handler_module, 'LABEL_RETRY_DELAY_SECONDS', 1)
    return payments, inventory, shipping


# tests/test_order_saga.py
@pytest.fixture
def saga_clients(monkeypatch):
    return install_fakes(monkeypatch)
```

## Everything a result will tell you

`runner.run()` returns a frozen `DurableFunctionTestResult` with four fields.

| Field | Type | What it holds |
| --- | --- | --- |
| `status` | `InvocationStatus` | `SUCCEEDED`, `FAILED`, `PENDING`, `RETRY` — the last two are internal |
| `operations` | `list[Operation]` | **Top-level operations only, in execution order** |
| `result` | `OperationPayload` or `None` | The handler's return value, serialized. `None` when it failed |
| `error` | `ErrorObject` or `None` | `.message`, `.type`, `.data`, `.stack_trace` |

`OperationPayload` is a `TypeAlias` for `str`. So `result.result` is a string or `None`, and
basedpyright rejects `json.loads(result.result)` until it is narrowed. Every test module in this
repository carries the same helper rather than sprinkling asserts:

```python
def payload(result) -> dict:
    """The handler's return value. `result.result` is None on a failed execution."""
    assert result.result is not None, f'execution did not return: {result.error}'
    return json.loads(result.result)
```

The mirror image, for asserting on a failure:

```python
def failure_message(result) -> str:
    """The error text of an execution that did not return."""
    assert result.result is None, 'execution succeeded'
    assert result.error is not None and result.error.message is not None
    return result.error.message
```

### `operations` is the orchestration assertion

It names which durable operations ran, in order. It is the cheapest structural test there is:

```python
def test_the_operation_history_is_leader_settle_freeze_publish(clients):
    s3, _glue, _lam = clients
    s3.pages = [s3_page({'landing/part-0001.csv': (120, 600)})]

    result = run(s3_event('landing/part-0001.csv'))

    assert [op.name for op in result.operations] == [
        'leader',
        'settle',
        'freeze_manifest',
        'publish_and_start',
    ]
```

!!! warning "`operations` holds only operations whose `parent_id` is `None`"

    Anything inside a child context, a map iteration or a parallel branch is absent from this list.
    It is reached through the parent's `child_operations`, covered under
    [nested operations](#nested-operations-are-reached-through-their-parent).

### The typed accessors are casts, not checks

Eight of them sit on `DurableFunctionTestResult`. `ContextOperation` carries the same set minus
`get_all_operations`, which is what makes nesting readable as a chain. Each one searches by name
and casts the hit to a concrete operation class.

| Accessor | Returns | Raises when the name is absent |
| --- | --- | --- |
| `get_operation_by_name(name)` | `Operation` | `DurableFunctionsTestError` |
| `get_step(name)` | `StepOperation` | `DurableFunctionsTestError` |
| `get_wait(name)` | `WaitOperation` | `DurableFunctionsTestError` |
| `get_context(name)` | `ContextOperation` | `DurableFunctionsTestError` |
| `get_callback(name)` | `CallbackOperation` | `DurableFunctionsTestError` |
| `get_invoke(name)` | `InvokeOperation` | `DurableFunctionsTestError` |
| `get_execution(name)` | `ExecutionOperation` | `DurableFunctionsTestError` |
| `get_all_operations()` | `list[Operation]`, recursive | never |

The implementation is one line each:

```python
def get_step(self, name: str) -> StepOperation:
    return cast(StepOperation, self.get_operation_by_name(name))
```

!!! danger "A wrong accessor type-checks clean and fails at runtime"

    `cast` is a promise to the type checker, not a check. Asking for a step by the name of a wait
    returns the `WaitOperation` wearing a `StepOperation` annotation, and the failure arrives later,
    on attribute access. Measured 2026-08-19:

    ```text
    get_step('pause').attempt → AttributeError: 'WaitOperation' object has no attribute 'attempt'
    ```

    The upside is that the accessor is the *only* way to satisfy basedpyright. Reading `.attempt`
    off a bare `Operation` pulled out of `child_operations` is a type error, which is why
    `stage.get_step('charge_card').attempt` is the idiom rather than `stage.child_operations[0]`.

Three more measured behaviours of this group:

- **The two levels raise different messages.** The result says
  `Operation with name 'score' not found`; a `ContextOperation` says
  `Child Operation with name 'score' not found`. A `pytest.raises(match=...)` written against one
  does not match the other, so match on the quoted name rather than the sentence.

- **`get_all_operations()` returns reversed order.** It walks a stack, popping from the end, so
  the last top-level operation comes out first and each subtree follows its parent. Measured on a
  handler running `first` (step), `pause` (wait) and `stage` (child context holding `nested`):

  ```text
  result.operations         → ['first', 'pause', 'stage']
  result.get_all_operations() → ['stage', 'nested', 'pause', 'first']
  ```

  Use it for counting and for membership, never for order:

  ```python
  assert sum(1 for op in result.get_all_operations() if op.name == 'score') == 3
  ```

- **`get_execution` can never find anything at top level.** Both factories skip the EXECUTION
  operation when building the list — `if operation.operation_type is OperationType.EXECUTION:
  continue`, with the comment *"don't want the EXECUTION operations in the list test code asserts
  against"*. Measured: `result.get_execution('execution-name')` raises
  `DurableFunctionsTestError: Operation with name 'execution-name' not found`.

### Every operation type, and what it adds

All operations share `operation_id`, `operation_type`, `status`, `parent_id`, `name`, `sub_type`,
`start_timestamp` and `end_timestamp`.

| `OperationType` | Class | Adds to the common fields |
| --- | --- | --- |
| `STEP` | `StepOperation` | `attempt`, `next_attempt_timestamp`, `result`, `error`, `child_operations` |
| `WAIT` | `WaitOperation` | `scheduled_end_timestamp` |
| `CONTEXT` | `ContextOperation` | `child_operations`, `result`, `error` |
| `CALLBACK` | `CallbackOperation` | `callback_id`, `result`, `error`, `child_operations` |
| `CHAINED_INVOKE` | `InvokeOperation` | `result`, `error` |
| `EXECUTION` | `ExecutionOperation` | `input_payload` |

Which call produces which:

- `STEP` — `context.step`, and `context.wait_for_condition`.
- `WAIT` — `context.wait`.
- `CONTEXT` — `run_in_child_context`, `map` and each of its iterations, `parallel` and each of its
  branches, `wait_for_callback`, and `with_retry`.
- `CALLBACK` — `create_callback`, and the callback minted inside `wait_for_callback`.
- `CHAINED_INVOKE` — `context.invoke`.
- `EXECUTION` — the execution itself, filtered out of `result.operations` by both factories.

`status` is an `OperationStatus`: `STARTED`, `PENDING`, `READY`, `SUCCEEDED`, `FAILED`,
`CANCELLED`, `TIMED_OUT`, `STOPPED`.

`sub_type` is an `OperationSubType` and is finer-grained than the type: `Step`, `Wait`, `Callback`,
`RunInChildContext`, `Map`, `MapIteration`, `Parallel`, `ParallelBranch`, `WaitForCallback`,
`WaitForCondition`, `ChainedInvoke`.

!!! note "`wait_for_condition` is a STEP, not a WAIT"

    `OperationType.from_sub_type` maps both `STEP` and `WAIT_FOR_CONDITION` onto
    `OperationType.STEP`. So a poll is reached with `result.get_step(name)`, and there is no
    per-poll wait operation. Its `.attempt` is the number of polls performed:

    ```python
    def test_the_poll_attempt_count_is_the_number_of_polls(partner):
        api, _dynamo = partner
        api.get_export_script = [export_running(), export_running(), export_ready(3)]

        result = run({'changedSince': CHANGED_SINCE})

        assert payload(result)['status'] == 'synced'
        assert result.get_step('export_ready').attempt == 3
        assert api.calls['get_export'] == 3
    ```

### A step's `.result` is the raw serdes envelope

`StepOperation.result` is whatever was written to the checkpoint, which is the **serialized** form,
not the value. Steps, child contexts and parallel branches fall back to the extended codec.
Measured 2026-08-19 on a step returning `{'n': 1, 'when': 'now'}`:

```text
step.result       '{"t":"m","v":{"n":{"t":"i","v":1},"when":{"t":"s","v":"now"}}}'
```

Decode it with the same codec the SDK used:

```python
from aws_durable_execution_sdk_python.serdes import ExtendedTypeSerDes, SerDesContext

decoded = ExtendedTypeSerDes().deserialize(step.result, SerDesContext())
assert decoded == {'n': 1, 'when': 'now'}
```

The envelope only appears for types JSON cannot carry on its own. Measured against the codec
directly:

```text
7          → 7
'a'        → "a"
1.5        → 1.5
True       → true
None       → null
[1, 2]     → [1,2]
{'n': 1}   → {"t":"m","v":{"n":{"t":"i","v":1}}}
(1, 2)     → {"t":"t","v":[{"t":"i","v":1},{"t":"i","v":2}]}
```

So a step returning an int can be compared with `int(step.result)` and a step returning a dict
cannot be compared with anything until it is deserialized. `context.invoke` is the exception: it
passes `DEFAULT_JSON_SERDES` explicitly for both payload and result, so an `InvokeOperation.result`
is plain JSON. What the default codec carries, and how to add a type it refuses, is in
[sdk-internals.md](sdk-internals.md).

!!! tip "Prefer asserting on the fake over asserting on the envelope"

    A step's checkpointed result is a fact about the SDK. What the step *did* is a fact about your
    code. `assert len(glue.job_runs) == 1` survives a serdes change; a hard-coded envelope string
    does not.

### Nested operations are reached through their parent

`ContextOperation` carries the same seven accessors, so nesting reads as a chain. The pipeline
example asserts two levels:

```python
def test_each_branch_is_its_own_context_nested_under_the_parallel_operation():
    """Branch names come from `durable_parallel_branch` and `ParallelBranch`."""
    result = run()

    parallel = result.get_context('extract_sources')

    assert [branch.name for branch in parallel.child_operations] == [
        'extract-orders',
        'extract-clickstream',
        'extract-inventory',
    ]


def test_a_branch_holds_its_own_steps_one_level_further_down():
    result = run()

    parallel = result.get_context('extract_sources')
    orders = parallel.get_context('extract-orders')
    inventory = parallel.get_context('extract-inventory')

    assert [step.name for step in orders.child_operations] == ['read_orders', 'stage_orders']
    assert [step.name for step in inventory.child_operations] == ['snapshot_inventory']
```

A map is the same shape: one `ContextOperation` at the top, one child `ContextOperation` per
iteration, and the iteration's steps below that. The scoring example pins both the failure and the
alternative:

```python
def test_a_step_inside_a_map_iteration_is_unreachable_with_get_step(scoring):
    """`get_step` scans only top-level operations. `get_all_operations` recurses."""
    s3, _endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)

    result = run(scoring_event(NINE_APPLICATIONS))

    with pytest.raises(DurableFunctionsTestError, match="'score' not found"):
        result.get_step('score')
    assert sum(1 for op in result.get_all_operations() if op.name == 'score') == 3
```

`with_retry` nests the same way, and it also names an operation you did not write. Each retry of
the block gets a backoff wait called `<name>-backoff-<attempt>`:

```python
def test_the_expired_step_itself_is_never_retried(partner):
    """Proof the 410 failed fast: the first store attempt was also its last."""
    api, _dynamo = partner
    api.download_records_script = [url_expired(), SUBSCRIBERS]

    result = run({'changedSince': CHANGED_SINCE})
    drain = result.get_context('drain')

    assert [op.name for op in drain.child_operations][:3] == [
        'mint_download_url',
        'store_records',
        'drain-backoff-1',
    ]
    assert drain.get_step('store_records').attempt == 1
```

## Driving a callback is the one flow `run()` cannot express

`runner.run()` blocks until the execution finishes. A gate waiting on a human never finishes, so
the test has to start the execution, wait until it has suspended on its token, answer it, and only
then wait for the result. That is four calls in order.

```text
 test                              runner / execution
  │
  ├── run_async(input) ───────────► starts, returns execution_arn immediately
  │                                 handler runs → wait_for_callback → submitter step
  │                                 posts the token to SQS → SUSPENDS
  │
  ├── wait_for_callback(arn, ─────► polls the history every `poll_interval`
  │       name=..., timeout=10)     returns the callback_id once CallbackStarted appears
  │   ◄──── callback_id
  │
  │   (the approval service would be doing its work here)
  │
  ├── send_callback_heartbeat(id) ► resets the heartbeat deadline. Repeatable.
  │
  ├── send_callback_success(id, b) ► resumes the execution with bytes
  │      or send_callback_failure(id, ErrorObject)
  │
  ├── wait_for_result(arn, ───────► blocks until the execution completes
  │       timeout=30)
  │   ◄──── DurableFunctionTestResult
  ▼
```

The helper that does the first two steps:

```python
def open_gate(runner, event: dict, timeout: int = 10) -> tuple[str, str]:
    """Start the execution and block until it is suspended on its callback."""
    execution_arn = runner.run_async(input=json.dumps(event), timeout=30)
    callback_id = runner.wait_for_callback(execution_arn, name=CALLBACK_OPERATION_NAME, timeout=timeout)
    return execution_arn, callback_id
```

And a whole test end to end:

```python
def test_an_approval_applies_the_change_and_records_the_reviewer(gate):
    _sqs, sfn, dynamodb = gate

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        runner.send_callback_success(callback_id, approval(note='ships with the backfill'))
        result = runner.wait_for_result(execution_arn, timeout=30)

    assert payload(result)['status'] == 'approved'
    assert payload(result)['reviewer'] == 'dana@example.com'
    assert sfn.executions[0]['name'] == 'CR-2041'
    assert audit_row(dynamodb)['verdict'] == {'S': 'approved'}
```

### `wait_for_callback(name=...)` wants the callback operation's name, not yours

This is the trap that costs the most time. `context.wait_for_callback(submitter, name='review')`
does **not** create an operation called `review` that holds a callback. It opens a child context
called `review` and puts two operations inside it: a `CallbackOperation` named
`review create callback id`, and a `StepOperation` named `review submitter`.

`runner.wait_for_callback` matches on the operation name, so it needs the first of those.

```python
# `wait_for_callback(name='review')` does not name the callback 'review'. It opens
# a child context under that name and mints the callback inside it, suffixed. The
# runner matches on the operation name, so this is the string it needs.
CALLBACK_OPERATION_NAME = 'review create callback id'

GATE_CONTEXT_NAME = 'review'
SUBMITTER_STEP_NAME = 'review submitter'
```

The decomposition is asserted rather than described:

```python
def test_the_gate_decomposes_into_a_callback_and_a_submitter_step(gate):
    """`wait_for_callback` is a child context holding two operations, which is why
    the runner is asked for '<name> create callback id' rather than '<name>'."""
    _sqs, _sfn, _dynamodb = gate

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        runner.send_callback_success(callback_id, approval())
        result = runner.wait_for_result(execution_arn, timeout=30)

    assert [op.name for op in result.operations] == [GATE_CONTEXT_NAME, 'apply_change', 'record_outcome']
    gate_context = result.get_context(GATE_CONTEXT_NAME)
    assert [op.name for op in gate_context.child_operations] == [
        CALLBACK_OPERATION_NAME,
        SUBMITTER_STEP_NAME,
    ]
```

!!! danger "The wrong name does not error — it hangs, then raises `TimeoutError`"

    `_get_callback_id_from_events` returns `None` when no `CallbackStarted` event carries the name,
    and `wait_for_callback` keeps polling until its own timeout expires. Passing `'review'` waits
    the full timeout and then raises `TimeoutError: Callback did not available within 60s`. Nothing
    says the name was wrong. A `name=None` returns the latest non-completed callback instead, which
    is a workable escape hatch in a handler with exactly one gate.

### Answering instantly re-runs the submitter

The callback's START checkpoint is written synchronously, so the token is visible the moment
`wait_for_callback` returns. The submitter step's SUCCEED checkpoint is flushed by a **background
thread**. A `send_callback_success` landing inside that window re-invokes the execution, the
pending flush is rejected for a stale checkpoint token, and the replay finds no submitter
checkpoint — so it posts the review request a second time.

Measured: the exactly-once assertion fails with 2 messages at zero delay, and passes with 0.3s.
That is at-least-once step semantics behaving as specified, so the test encodes a named human
delay rather than asserting on the race:

```python
def reviewer_reads_it() -> None:
    """A person takes longer than a millisecond to answer.

    The submitter step's SUCCEED is flushed by a background thread, and an answer
    landing inside that window is rejected for a stale checkpoint token — after
    which the replay finds no submitter checkpoint and posts the request again.
    That is at-least-once step semantics behaving as specified, and any scenario
    with a human in it clears the window by orders of magnitude.
    """
    time.sleep(0.3)
```

It buys the exactly-once assertion, which is the whole point of driving the gate at all:

```python
def test_the_review_request_is_posted_exactly_once_despite_the_replay(gate):
    """A second queued message means the submitter escaped its step and replayed."""
    sqs, sfn, dynamodb = gate

    with gate_runner() as runner:
        execution_arn, callback_id = open_gate(runner, change_event())
        reviewer_reads_it()
        runner.send_callback_success(callback_id, approval())
        runner.wait_for_result(execution_arn, timeout=30)

    assert len(sqs.messages) == 1
    assert len(sfn.executions) == 1
    assert len(dynamodb.items) == 1
```

### The three payload and timer details

- **`send_callback_success` takes `bytes`; the handler receives a `str`.** The harness stores
  `result.decode()`, and `Callback.result()` deserializes with `PASS_THROUGH_SERDES` when no serdes
  is configured, so the handler gets the raw JSON string and parses it itself.

  ```python
  def approval(reviewer: str = 'dana@example.com', note: str = '') -> bytes:
      """The body the approval service sends back with the token."""
      return json.dumps({'verdict': 'approved', 'reviewer': reviewer, 'note': note}).encode()
  ```

- **`send_callback_failure` takes an `ErrorObject`.** Build it with
  `ErrorObject.from_message('needs a backfill plan first')`. The message survives to the handler
  verbatim; the type does not. See [waits.md](waits.md) for what the handler catches.

- **A `Duration` of 0 disables a timer rather than firing it immediately.** The harness schedules a
  timeout only `if callback_options.timeout_seconds > 0`, and the same for the heartbeat. So the
  gate fixture starts with `REVIEW_SERVICE_HEARTBEAT_SECONDS = 0` to mean "no heartbeat deadline",
  and a test that wants one sets it:

  ```python
  def test_a_review_service_that_stops_beating_declines_before_the_window_closes(gate, monkeypatch):
      """The window is 20s and the run ends in about one, so only the heartbeat can
      have ended it."""
      _sqs, sfn, _dynamodb = gate
      monkeypatch.setattr(handler_module, 'REVIEW_SERVICE_HEARTBEAT_SECONDS', 1)

      with gate_runner() as runner:
          execution_arn, _callback_id = open_gate(runner, change_event())
          result = runner.wait_for_result(execution_arn, timeout=30)

      assert payload(result)['reason'] == 'review-service-silent'
      assert sfn.executions == []
  ```

  Its pair proves the heartbeat is what held the gate open, by beating the same 1s deadline for
  2.4s:

  ```python
  def beat_for(runner, callback_id: str, seconds: float, interval: float = 0.4) -> int:
      """Heartbeat the callback for a stretch of wall-clock, returning the beat count."""
      beats = 0
      deadline = time.monotonic() + seconds
      while time.monotonic() < deadline:
          time.sleep(interval)
          runner.send_callback_heartbeat(callback_id)
          beats += 1
      return beats


  def test_heartbeats_hold_the_gate_open_past_the_heartbeat_timeout(gate, monkeypatch):
      """The same 1s heartbeat as the test above, beaten for well over 1s."""
      _sqs, sfn, _dynamodb = gate
      monkeypatch.setattr(handler_module, 'REVIEW_SERVICE_HEARTBEAT_SECONDS', 1)

      with gate_runner() as runner:
          execution_arn, callback_id = open_gate(runner, change_event())
          beats = beat_for(runner, callback_id, seconds=2.4, interval=0.25)
          runner.send_callback_success(callback_id, approval())
          result = runner.wait_for_result(execution_arn, timeout=30)

      assert beats >= 6
      assert payload(result)['status'] == 'approved'
      assert len(sfn.executions) == 1
  ```

!!! note "`Task was destroyed but it is pending!` on stderr is not a failure"

    The harness prints it when a runner closes with a callback timeout still scheduled. It is
    asyncio cleanup noise from `Scheduler.call_later` and it appears on a fully green run.

## `StepOperation.attempt` counts executions, which is how a retry is asserted

The harness increments `attempt` on RETRY, SUCCEED **and** FAIL alike:

```python
# aws_durable_execution_sdk_python_testing/checkpoint/processors/base.py
if update.action in {
    OperationAction.RETRY,
    OperationAction.SUCCEED,
    OperationAction.FAIL,
}:
    attempt += 1
```

`StepProcessor` overrides the RETRY action to schedule the retry timer, and increments there too —
`attempt=current_attempt + 1`. So a step that succeeds first time reports `attempt == 1`, and two
retries followed by success report `3`. It is the count of executions, not the count of retries.

That makes a retry policy directly assertable. The three cases from the partner sync, which differ
only in the error the fake raises:

```python
def test_a_throttled_export_request_is_retried_until_it_succeeds(partner):
    api, _dynamo = partner
    api.request_export_script = [throttled(1), throttled(1), EXPORT_ID]

    result = run({'changedSince': CHANGED_SINCE})

    assert payload(result)['status'] == 'synced'
    assert result.get_step('request_export').attempt == 3
    assert api.calls['request_export'] == 3


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

Asserting `attempt` alongside the fake's own call count is deliberate. `attempt` proves the SDK
retried; `api.calls` proves the body ran that many times. A step whose checkpoint was replayed
rather than re-executed shows the two diverging.

The same reading works one level down. A retried step inside a child context replays the whole
stage body, and the sibling that already succeeded returns its checkpoint:

```python
def test_a_retried_label_does_not_charge_the_card_again(saga_clients):
    """A step retry replays the whole stage body. The charge returns its checkpoint instead."""
    payments, _inventory, shipping = saga_clients
    shipping.fail_next_label = carrier_outage()

    result = run(checkout_event())

    stage = result.get_context('fulfillment')
    assert stage.get_step('charge_card').attempt == 1
    assert stage.get_step('buy_label').attempt == 2
    assert payload(result)['status'] == 'placed'
    assert len(payments.charge_attempts) == 1
    assert len(shipping.labels) == 1
```

!!! warning "A step with no `retry_strategy` gets `RetryPresets.default()`, and it is slow"

    Six attempts, 5s initial delay, exponential to a 60s cap, full jitter — and the default
    retryable-error pattern is `.*`, so every exception retries. All of it is real wall clock in the
    harness. A test that exercises a failure path against an unconfigured step hangs until the
    runner times out. Every step whose failure a test drives needs an explicit `retry_strategy`
    reading module constants the fixture can shrink. Configuration is in [steps.md](steps.md).

## Patterns that make a durable test deterministic

### Freeze the clock

Settle detection compares the newest object's timestamp with now. Pinning `time.time` turns
"landed ten minutes ago" into a fact of the fixture rather than a race:

```python
EPOCH = dt.datetime(2026, 8, 18, 12, 0, tzinfo=dt.UTC)

monkeypatch.setattr(handler_module.time, 'time', lambda: EPOCH.timestamp())


def s3_page(entries: dict[str, tuple[int, int]]) -> dict:
    """Build one `list_objects_v2` page from key -> (size, age_in_seconds)."""
    return {
        'Contents': [
            {'Key': key, 'Size': size, 'LastModified': EPOCH - dt.timedelta(seconds=age)}
            for key, (size, age) in entries.items()
        ]
    }
```

The handler patches `handler_module.time`, not `time.time` globally, so the harness's own scheduler
keeps a real clock. Freezing the clock the runtime uses would deadlock the run.

### Build fakes that count

A fake exists to be counted, not to return data. Every one in this repository records its calls:

```python
class FakeS3:
    """Stands in for the S3 client, counting calls so replay can be asserted."""

    def __init__(self, pages: list[dict] | None = None) -> None:
        self.pages = pages if pages is not None else [{}]
        self.paginate_calls = 0
        self.put_objects: list[dict] = []

    def get_paginator(self, _operation: str) -> 'FakeS3':
        return self

    def paginate(self, **_kwargs) -> list[dict]:
        self.paginate_calls += 1
        return self.pages

    def put_object(self, **kwargs) -> dict:
        self.put_objects.append(kwargs)
        return {'ETag': '"fake"'}
```

A **scripted** fake takes it one step further: one queue per endpoint, holding either the value to
return or the exception to raise, so a test spells out a scenario as a list.

```python
class FakePartnerBillingApi:
    """A scripted partner, one queue per endpoint.

    A queue entry is either the value to return or the exception to raise, so a
    test can spell out "throttle, throttle, then succeed". The last entry repeats
    once the queue runs out, which is how an endpoint that never recovers is
    written.
    """

    def _next(self, endpoint: str, script: list):
        self.calls[endpoint] += 1
        entry = script[min(self.calls[endpoint] - 1, len(script) - 1)]
        if isinstance(entry, Exception):
            raise entry
        return entry
```

`api.request_export_script = [throttled(1), throttled(1), EXPORT_ID]` then reads as the scenario it
is.

!!! warning "Fakes reached from a map or a parallel need locks"

    `max_concurrency` becomes `ThreadPoolExecutor(max_workers=...)`, so branches and map iterations
    genuinely run on OS threads. An unsynchronized counter loses increments. It is also the only
    way to prove the cap does anything: a probe recording peak in-flight calls measured exactly 2
    against three branches.

    ```python
    class ConcurrencyProbe:
        """Records the most extracts that were ever in flight at the same time.

        Branches run on a ThreadPoolExecutor sized by `max_concurrency`, so the peak
        is the only direct evidence the cap is doing anything.
        """

        @contextlib.contextmanager
        def in_flight(self) -> Iterator[None]:
            with self._lock:
                self._in_flight += 1
                self.peak = max(self.peak, self._in_flight)
            try:
                time.sleep(self.dwell_seconds)
                yield
            finally:
                with self._lock:
                    self._in_flight -= 1
    ```

    ```python
    def test_max_concurrency_holds_the_third_extract_back(sources: Sources):
        run()

        assert sources.probe.peak == handler_module.MAX_CONCURRENT_EXTRACTS
    ```

### The guard test is what makes a once-only assertion mean anything

Every example carries one, and every one of them is three lines of the same shape: wrap a function
the handler body calls on **every** pass, count the entries, assert on more than one.

```python
def test_the_handler_body_really_does_re_enter(clients, monkeypatch):
    """Guards the tests above: they only prove anything if a replay happened."""
    s3, _glue, _lam = clients
    entries: list[str] = []
    original = handler_module.is_quiet

    def counting(*args, **kwargs):
        entries.append('body')
        return original(*args, **kwargs)

    monkeypatch.setattr(handler_module, 'is_quiet', counting)

    run_settled(s3)

    assert entries, 'the settle check never ran, so nothing was replayed'
```

The six guards in this repository, and the function each one wraps:

| Test file | Wrapped function | Assertion |
| --- | --- | --- |
| `tests/test_replay.py` | `is_quiet` | `assert entries` |
| `tests/test_order_saga.py` | `parse_order` | `assert len(entries) > 1` |
| `tests/test_batch_scoring.py` | `group_into_batches` | `assert len(entries) > 1` |
| `tests/test_approval_gate.py` | `parse_change_request` | `assert len(entries) > 1` |
| `tests/test_flaky_api_sync.py` | `build_export_poll_strategy` | `assert len(entries) > 1` |
| `tests/test_pipeline_chain.py` | `run_date_from_event` | `assert len(entries) > 1` |

Five of the six wrap a function called **before** the first durable operation, so the count rises
on every invocation and `> 1` is the assertion. That is the stronger form, and it is the one to
copy. The landing-zone guard is weaker on purpose: `is_quiet` runs inside the settle poll rather
than at the top of the body, so the only thing it can prove is that the poll ran at all.

### Durations are module constants so tests can shrink them

There is no clock skipping (see below), so every modeled second is a real second. A duration
written inline as `Duration.from_seconds(300)` is untestable; the same duration read from a module
constant is one `monkeypatch.setattr` away from 1 second.

```python
monkeypatch.setattr(handler_module, 'REQUEST_MAX_ATTEMPTS', 3)
monkeypatch.setattr(handler_module, 'REQUEST_BASE_DELAY_SECONDS', 1)
monkeypatch.setattr(handler_module, 'REQUEST_MAX_DELAY_SECONDS', 1)
monkeypatch.setattr(handler_module, 'POLL_MAX_ATTEMPTS', 4)
monkeypatch.setattr(handler_module, 'POLL_INITIAL_DELAY_SECONDS', 1)
monkeypatch.setattr(handler_module, 'POLL_MAX_DELAY_SECONDS', 1)
monkeypatch.setattr(handler_module, 'DOWNLOAD_MAX_ATTEMPTS', 2)
monkeypatch.setattr(handler_module, 'DOWNLOAD_BASE_DELAY_SECONDS', 1)
monkeypatch.setattr(handler_module, 'DOWNLOAD_MAX_DELAY_SECONDS', 1)
```

One second is the floor. Both the step executor and the wait-for-condition executor clamp any delay
below a second up to a second, so a zero-second retry still costs a real second.

### The harness has constants worth shrinking too

`Executor.RETRY_BACKOFF_SECONDS` is the harness's own delay before it re-invokes after an
`InvocationError`, and it defaults to **5**. An interrupted step pays it twice. Monkeypatching it
to 1 took the saga's test file from 43.0s to 14.9s — the handler's own durations were already 1s
and were not the cost.

```python
from aws_durable_execution_sdk_python_testing.executor import Executor

monkeypatch.setattr(Executor, 'RETRY_BACKOFF_SECONDS', 1)
```

Patching a class attribute on a vendored class is not something to do lightly. It is justified here
because the alternative is a suite nobody runs, and because `monkeypatch` restores it.

### A stuck execution is asserted by side effect, not by status

An execution that never terminates surfaces either as a `FAILED` result or as a raised
`TimeoutError`, depending on whether the execution timeout or the runner's wait expires first. Both
mean the same thing, so assert on what did **not** happen:

```python
def test_an_empty_landing_prefix_never_settles(clients):
    """Nothing to ingest is not a finished drop, so it keeps waiting rather than
    ingesting an empty manifest.

    A stuck execution surfaces either as a FAILED result or as a raised
    TimeoutError, depending on whether the execution timeout or the runner's
    wait expires first. Both mean the same thing, so assert on the side effect.
    """
    _s3, glue, _lam = clients

    with contextlib.suppress(TimeoutError), DurableFunctionTestRunner(
        handler_module.lambda_handler
    ) as runner:
        runner.run(input=json.dumps(s3_event('landing/gone.csv')), timeout=5)

    assert glue.job_runs == []
```

### A lost invocation is simulated with `TimedSuspendExecution`

Testing 1.2.1 has no fault injection. The only lever for "the invocation died mid-step, after the
side effect and before the checkpoint" is raising `TimedSuspendExecution` from inside the step
body. It works because it derives from `BaseException`: `StepOperationExecutor.execute` catches
`except Exception` and misses it, and `state.wrap_user_function` re-raises `SuspendExecution`
explicitly.

```python
def charge(self, idempotency_key: str, amount_cents: int, currency: str) -> str:
    self.charge_attempts.append(
        {'key': idempotency_key, 'amountCents': amount_cents, 'currency': currency}
    )
    charge_id = self.charges.setdefault(idempotency_key, f'ch_{len(self.charges) + 1}')
    if self.interrupt_next_charge:
        self.interrupt_next_charge = False
        raise TimedSuspendExecution.from_delay(
            'invocation lost after the processor took the money', INTERRUPT_RESUME_SECONDS
        )
    return charge_id
```

That is what makes step semantics testable at all. The paired assertion — same fake, same
interruption, one line of configuration different — is the only honest demonstration that
`AT_MOST_ONCE_PER_RETRY` does anything:

```python
def test_an_interrupted_charge_runs_its_body_exactly_once(saga_clients):
    payments, _inventory, _shipping = saga_clients
    payments.interrupt_next_charge = True

    run(checkout_event())

    assert len(payments.charge_attempts) == 1


def test_the_default_semantics_charge_the_card_twice(saga_clients, monkeypatch):
    payments, _inventory, _shipping = saga_clients
    monkeypatch.setattr(handler_module, 'CHARGE_SEMANTICS', StepSemantics.AT_LEAST_ONCE_PER_RETRY)
    payments.interrupt_next_charge = True

    result = run(checkout_event())

    assert payload(result)['status'] == 'placed'
    assert len(payments.charge_attempts) == 2
```

### Pin a harness limit as an executable test

A limit written in a comment rots. A limit written as a test tells you the day it stops being
true. `tests/test_harness_limits.py` says so in its docstring, and the batch-scoring suite pins an
SDK limit the same way:

```python
def test_item_batcher_does_not_group_anything_in_sdk_1_7_0():
    """`MapConfig.item_batcher` is accepted and never read.

    `map_handler` builds one executable per input and `MapExecutor.execute_item`
    passes the raw item, so a batch size of 2 over 4 items still yields 4
    iterations of `str`. Nothing in the SDK constructs the `BatchedInput` its own
    map signature offers, which is why the handler groups before the map.
    """
    OBSERVED_ITEMS.clear()
    with DurableFunctionTestRunner(batcher_probe) as runner:
        result = runner.run(input=json.dumps({'items': ['a', 'b', 'c', 'd']}), timeout=30)

    assert result.result is not None
    assert json.loads(result.result)['iterations'] == 4
    assert [item['type'] for item in OBSERVED_ITEMS] == ['str'] * 4
    assert sorted(item['item'] for item in OBSERVED_ITEMS) == ['a', 'b', 'c', 'd']
```

Use `@pytest.mark.xfail(strict=True)` when the assertion is what you *want* to be true, so the day
the harness fixes it the suite fails loudly rather than staying quietly wrong.

## Two harness limits shape how every test here is written

Both are properties of testing 1.2.1 rather than of the SDK, and both decide how the rest of the
suite is written. `tests/test_harness_limits.py` asserts them rather than describing them.

### `wait_for_condition` polling state is dropped on every retry

Every check receives `initial_state`, however many times it has already run.

```python
@durable_execution
def counting_handler(_event: dict, context: DurableContext) -> dict:
    def check(state, _check_context):
        SEEN.append(dict(state))
        return {'n': state['n'] + 1}

    def strategy(state, _attempt):
        if state['n'] >= 3:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(1))

    return context.wait_for_condition(
        check=check,
        config=WaitForConditionConfig(wait_strategy=strategy, initial_state={'n': 0}),
        name='count',
    )
```

```yaml
check saw    : [{'n': 0}, {'n': 0}, {'n': 0}, ...]   # never advances
strategy saw : [({'n': 1}, 1), ({'n': 1}, 2), ...]   # attempt advances correctly
```

The SDK is not at fault. It serializes the new state and sends it as the RETRY payload:

```python
retry_operation = OperationUpdate.create_wait_for_condition_retry(
    identifier=self.operation_identifier,
    payload=serialized_state,
    next_attempt_delay_seconds=delay_seconds,
)
```

On the next attempt it reads that state back out of the checkpoint:

```python
# aws_durable_execution_sdk_python/operation/wait_for_condition.py
if checkpointed_result.is_started_or_ready() and checkpointed_result.result:
    current_state = deserialize(...)
else:
    current_state = self.config.initial_state
```

The harness's step processor never reads `update.payload` on the RETRY branch. It copies the
previous `step_details.result` forward, which is `None` on the first retry and stays `None`:

```python
# aws_durable_execution_sdk_python_testing/checkpoint/processors/step.py
new_step_details = StepDetails(
    attempt=current_attempt + 1,
    next_attempt_timestamp=next_attempt_time,
    result=(
        current_op.step_details.result
        if current_op and current_op.step_details
        else None
    ),
    error=(...),
)
```

Note that the shared base processor *does* write `result=update.payload`. The STEP processor
overrides `process()` for the RETRY action to schedule the retry timer, and the state is lost in
that override.

!!! danger "Any poll whose stop condition depends on accumulated state never terminates locally"

    A counter, a running total, a "seen this before" set — all of them stay at their initial value,
    so the strategy never stops polling and the execution runs to its timeout.

The test that pins it is marked `strict=True`, so the day the harness reads the payload the suite
turns red and the limit gets deleted:

```python
@pytest.mark.xfail(
    reason=(
        'testing 1.2.1 drops wait_for_condition polling state. The SDK sends the '
        'serialized state as OperationUpdate.payload on RETRY; the in-memory step '
        'processor never reads it and copies the previous step_details.result, '
        'which is None. Every check therefore receives initial_state.'
    ),
    strict=True,
)
def test_wait_for_condition_threads_state_between_attempts():
    SEEN.clear()
    with DurableFunctionTestRunner(counting_handler) as runner:
        runner.run(input='{}', timeout=15)

    assert [s['n'] for s in SEEN] == [0, 1, 2]
```

`landing_zone` sidesteps this by design rather than working around it. Settle detection reads the
newest object's own timestamp and compares it with now, so an attempt needs no memory of the one
before it. Comparing successive listings would have been the obvious implementation and would have
depended on exactly the state the harness drops.

The rule generalises past the bug: **compute a stop condition from what the check just observed,
never from a running tally.** A poll written that way survives a lost checkpoint, and it is easier
to reason about. More on the design in [waits.md](waits.md).

### A modeled wait costs real wall-clock time

There is no time skipping in 1.2.1. `SkipClock` and a `skip_time` flag exist on `main` and are not
released.

```python
def test_a_modeled_wait_costs_real_wall_clock_time():
    """No SkipClock in 1.2.1, so handler durations have to be injectable."""

    @durable_execution
    def waits(_event: dict, context: DurableContext) -> str:
        context.wait(Duration.from_seconds(3), name='long')
        return 'done'

    start = time.monotonic()
    with DurableFunctionTestRunner(waits) as runner:
        runner.run(input='{}', timeout=30)
    assert time.monotonic() - start >= 3
```

Measured 2026-08-19 with a ten-second wait:

```text
modeled 10s wait took 10.31s wall clock -> InvocationStatus.SUCCEEDED
```

The consequences run through the whole suite. Keep every duration in a test at 1–3 seconds. Make
durations module constants a fixture can shrink. Shrink the harness's own
`Executor.RETRY_BACKOFF_SECONDS` and the runner's `poll_interval` too. Production values would make
one end-to-end run of the landing zone sit for twenty-one minutes.

## Two more limits shape one example each

### `context.invoke` cannot be driven at all without filling a processor slot

`OperationTransformer._DEFAULT_PROCESSORS` holds entries for STEP, WAIT, CONTEXT, CALLBACK and
EXECUTION only. A chained invoke raises
`InvalidParameterValueException: Checkpoint for OperationType.CHAINED_INVOKE is not implemented yet`
inside the background checkpoint thread. The parent surfaces it as a `BackgroundThreadError`, the
execution never completes, and `runner.run()` raises `TimeoutError` — so there is no result object
to inspect and nothing on screen names the real cause.

The fix is a processor of your own, installed for the duration of a test with
`monkeypatch.setitem`:

```python
def register_load_function(monkeypatch) -> RecordingInvokeProcessor:
    """Give the harness a chained-invoke processor and record what it receives."""
    processor = RecordingInvokeProcessor(load_warehouse)
    monkeypatch.setitem(
        OperationTransformer._DEFAULT_PROCESSORS, OperationType.CHAINED_INVOKE, processor
    )
    return processor
```

It completes the invoke inside its own START checkpoint, which is a path the SDK already supports:

```python
class RecordingInvokeProcessor:
    """Completes a chained invoke inside its own START checkpoint.

    The SDK checks the operation's status again after the synchronous START
    checkpoint returns, precisely so an invoke that finished immediately costs no
    suspension. Marking the operation SUCCEEDED there is therefore a path the SDK
    already supports, and the parent resumes with the result rather than waiting
    for a service that the in-process harness does not have.
    """

    def process(self, update: OperationUpdate, **_harness_arguments: object) -> Operation:
        serdes_context = SerDesContext()
        payload = DEFAULT_JSON_SERDES.deserialize(update.payload or 'null', serdes_context)
        options = update.chained_invoke_options
        self.calls.append((options.function_name if options else '', payload))
        return Operation(
            operation_id=update.operation_id,
            operation_type=OperationType.CHAINED_INVOKE,
            status=OperationStatus.SUCCEEDED,
            parent_id=update.parent_id,
            name=update.name,
            sub_type=update.sub_type,
            chained_invoke_details=ChainedInvokeDetails(
                result=DEFAULT_JSON_SERDES.serialize(self.downstream(payload), serdes_context)
            ),
        )
```

The recorded calls then become the assertion that the downstream function got the right payload:

```python
def test_the_handoff_names_the_load_function_and_carries_the_staged_keys(
    sources: Sources, handoff: RecordingInvokeProcessor
):
    run()

    function_name, sent = handoff.calls[0]

    assert function_name == handler_module.LOAD_FUNCTION_NAME
    assert sent['runDate'] == RUN_DATE
    assert sent['datasets']['orders'] == staging_key('staging/', 'orders', RUN_DATE)
```

!!! warning "Reaching into a private attribute is a real cost, weighed against no coverage at all"

    `_DEFAULT_PROCESSORS` is private and may move. The alternative was leaving the chained invoke
    entirely untested, and `monkeypatch.setitem` restores the mapping after every test. When the
    harness ships a processor, this fixture is deleted.

### The original exception class does not survive a failed batch or a step boundary

`ErrorObject.from_exception` keeps `str(exception)` and `type(exception).__name__`. Once an error
crosses a step boundary it arrives as a `CallableRuntimeError` carrying the original class name as
a **string**, so `isinstance` against the class that was raised no longer holds:

```python
def test_an_expired_url_keeps_only_its_class_name_across_the_step_boundary():
    """A step re-raises as CallableRuntimeError, so `isinstance` no longer holds."""
    crossed = CallableRuntimeError(
        message='410 signed download url rejected',
        error_type='DownloadUrlExpired',
        data=None,
        stack_trace=None,
    )

    assert not isinstance(crossed, DownloadUrlExpired)
    assert failing_type_name(crossed) == 'DownloadUrlExpired'
    assert plan_block_retry(crossed, 1, BLOCK_LIMITS).should_retry
```

Inside a `BatchResult` even the name is flattened to `CallableRuntimeError`, and only the message
survives. So assert on the message, and put anything a caller must branch on into the message text:

```python
def test_a_lost_batch_is_reported_by_message_on_the_batch_result(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.permanent['APP-0060'] = 'ValidationError'

    failures = payload(run(scoring_event(NINE_APPLICATIONS)))['failedBatches']

    assert len(failures) == 1
    assert 'ValidationError' in failures[0]
```

## Running the suite

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest tests/test_replay.py -q      # one file
.venv/bin/python -m ruff check .
.venv/bin/python -m basedpyright
```

`pyproject.toml` sets `pythonpath = ["src", "tests"]`, which is what makes both
`from landing_zone import handler` and `from conftest import s3_page` resolve with no packaging
gymnastics:

```toml
[tool.pytest.ini_options]
minversion = "8.0"
testpaths = ["tests"]
pythonpath = ["src", "tests"]
```

Measured 2026-08-19: **147 passed, 1 xfailed in 128.95s**. Almost all of that is real waiting —
modeled durations, retry backoffs, and the runner's own poll interval. The xfail is the
`wait_for_condition` state limit above, and it is `strict`, so it turns into a failure the day the
harness fixes it.

## Further reading

- [Best practices for durable
  functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-best-practices.html) — the AWS
  guidance on determinism and side effects, which is what the replay tests here enforce.
- [Durable execution patterns and best
  practices](https://docs.aws.amazon.com/durable-execution/patterns/best-practices/).
- [`lambda_service.py`][lambda-service] — `InvocationStatus`, `OperationStatus`, `OperationType`,
  `OperationSubType`, `ErrorObject`, and every `OperationUpdate.create_*` factory quoted above.
- [`operation/wait_for_condition.py`][wait-for-condition] — where the polling state is serialized
  on RETRY and read back on the next attempt.
- [`serdes.py`][serdes] — the codec behind every `.result` envelope on this page.
- `aws_durable_execution_sdk_python_testing/runner.py`, in the installed testing package, holds the
  four runners, `DurableFunctionTestResult`, and every operation class in the table above. It is
  the fastest way to check an accessor's behaviour, because every one of them is a line long.

[lambda-service]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/lambda_service.py
[wait-for-condition]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/operation/wait_for_condition.py
[serdes]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/serdes.py
