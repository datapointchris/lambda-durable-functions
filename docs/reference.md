# Reference

Every name the SDK exposes, with a line on what it is for and a link to the page that teaches it.
Read this when you already know what you want and need the exact spelling, the exact field name, or
the exact default. Read [concepts.md](concepts.md) first if you do not.

Nothing here is a tutorial. The tables carry signatures, fields, defaults and enum members. The
teaching pages carry the reasoning and the runnable code.

## Versions this documents

| Package | Version | Index |
| --- | --- | --- |
| `aws-durable-execution-sdk-python` | 1.7.0 | [PyPI][pypi-sdk] |
| `aws-durable-execution-sdk-python-testing` | 1.2.1 | [PyPI][pypi-testing] |

Every field name, default and enum member below was read from the installed source on **2026-08-19**.
Re-measure before trusting any of it against a newer release — several of the findings on this page
are implementation detail rather than documented contract.

```bash
.venv/bin/python -c "import aws_durable_execution_sdk_python as s; print(s.__version__)"
.venv/bin/python -c "import aws_durable_execution_sdk_python_testing as t; print(t.__version__)"
```

## `DurableContext` is the whole orchestration surface

Nine methods start durable operations. Two more, and one attribute, cover the context itself. There
is nothing else — no ambient context, no registry, no module-level API.

```python
step(func: Callable[[StepContext], T], name: str | None = None, config: StepConfig | None = None) -> T

wait(duration: Duration, name: str | None = None) -> None

wait_for_condition(check: Callable[[T, WaitForConditionCheckContext], T],
                   config: WaitForConditionConfig[T], name: str | None = None) -> T

wait_for_callback(submitter: Callable[[str, WaitForCallbackContext], None],
                  name: str | None = None, config: WaitForCallbackConfig | None = None) -> Any

create_callback(name: str | None = None, config: CallbackConfig | None = None) -> Callback

map(inputs: Sequence[U],
    func: Callable[[DurableContext, U | BatchedInput[Any, U], int, Sequence[U]], T],
    name: str | None = None, config: MapConfig | None = None) -> BatchResult[R]

parallel(functions: Sequence[Callable[[DurableContext], T] | ParallelBranch[T]],
         name: str | None = None, config: ParallelConfig | None = None) -> BatchResult[T]

run_in_child_context(func: Callable[[DurableContext], T],
                     name: str | None = None, config: ChildConfig | None = None) -> T

invoke(function_name: str, payload: P, name: str | None = None,
       config: InvokeConfig[P, R] | None = None) -> R

is_replaying() -> bool
set_logger(new_logger: LoggerInterface) -> None
execution_context: ExecutionContext          # attribute, not a method
```

| Member | What it is for | Taught in |
| --- | --- | --- |
| `step` | Run a side effect once and checkpoint its return value | [steps.md](steps.md) |
| `wait` | Suspend for a modeled duration and resume | [waits.md](waits.md) |
| `wait_for_condition` | Poll something external until a strategy says stop | [waits.md](waits.md) |
| `wait_for_callback` | Hand a token out and suspend until someone answers it | [waits.md](waits.md) |
| `create_callback` | Mint the token yourself and block on `.result()` later | [waits.md](waits.md) |
| `map` | Run one function per item, concurrently, with a failure budget | [fan-out.md](fan-out.md) |
| `parallel` | Run a fixed set of named branches concurrently | [fan-out.md](fan-out.md) |
| `run_in_child_context` | Group operations so they succeed or fail as a unit | [steps.md](steps.md) |
| `invoke` | Chain to a second durable function and wait for its result | [fan-out.md](fan-out.md) |
| `is_replaying` | True while the context is re-reading checkpoints | [concepts.md](concepts.md) |
| `set_logger` | Replace the replay-aware logger | [sdk-internals.md](sdk-internals.md) |
| `execution_context` | Carries `durable_execution_arn`, and nothing else | [concepts.md](concepts.md) |

!!! warning "`wait` rejects a duration under one second"

    `wait` raises `ValidationError("duration must be at least 1 second")` for anything below one
    second, including `Duration()`. Retry and poll delays are clamped up to one second instead of
    rejected, so a zero-second retry still costs a real second.

!!! note "Two `DurableContext` names exist and they are not the same object"

    `aws_durable_execution_sdk_python.DurableContext` is the concrete class. The one in
    `aws_durable_execution_sdk_python.types` is a Protocol carrying `step`,
    `run_in_child_context`, `map`, `parallel`, `wait` and `create_callback` — no
    `wait_for_condition`, no `invoke`, no `is_replaying`, no `execution_context`. `with_retry`
    types its callable against the Protocol, so a block annotated with the concrete class fails
    basedpyright. See [typing-and-tooling.md](typing-and-tooling.md).

## Config dataclasses are all frozen, and nearly every field has a default

Each one is `@dataclass(frozen=True)`. Construct with keywords and pass positionally to nothing.
`ParallelBranch.func` is the only required field in this section.

### `Duration` stores whole seconds

| Member | Signature | Note |
| --- | --- | --- |
| `Duration` | `Duration(seconds: int = 0)` | Negative raises `ValidationError` |
| `from_seconds` | `Duration.from_seconds(value: float)` | `int(value)`, so fractions truncate |
| `from_minutes` | `Duration.from_minutes(value: float)` | `int(value * 60)` |
| `from_hours` | `Duration.from_hours(value: float)` | `int(value * 3600)` |
| `from_days` | `Duration.from_days(value: float)` | `int(value * 86400)` |
| `to_seconds` | `duration.to_seconds() -> int` | The only accessor |

### `StepConfig` has three fields

| Field | Type | Default |
| --- | --- | --- |
| `retry_strategy` | `Callable[[Exception, int], RetryDecision]` \| `None` | `None` |
| `step_semantics` | `StepSemantics` | `AT_LEAST_ONCE_PER_RETRY` |
| `serdes` | `SerDes` \| `None` | `None` |

!!! danger "No `retry_strategy` means six attempts over about a minute"

    A step with no `StepConfig` gets `RetryPresets.default()` — six attempts, 5s initial delay,
    exponential to a 60s cap, full jitter (`operation/step.py` line 294, measured 2026-08-19). The
    default retryable-error pattern is `re.compile(r".*")`, so every exception retries, including a
    permanent one. Give any step whose failure path matters an explicit strategy.

### `MapConfig` and `ParallelConfig` differ by two fields

`MapConfig` carries `item_batcher` and `item_namer`; `ParallelConfig` carries neither. Everything
else is shared.

| Field | Type | `MapConfig` default | `ParallelConfig` default |
| --- | --- | --- | --- |
| `max_concurrency` | `int` \| `None` | `None` | `None` |
| `completion_config` | `CompletionConfig` | `CompletionConfig()` | `CompletionConfig.all_successful()` |
| `serdes` | `SerDes` \| `None` | `None` | `None` |
| `item_serdes` | `SerDes` \| `None` | `None` | `None` |
| `summary_generator` | `SummaryGenerator` \| `None` | `None` | `None` |
| `nesting_type` | `NestingType` | `NESTED` | `NESTED` |
| `item_batcher` | `ItemBatcher` | `ItemBatcher()` | absent |
| `item_namer` | `Callable[[T, int], str]` \| `None` | `None` | absent |

The default `completion_config` differs. A `map` with no config tolerates every failure; a
`parallel` with no config tolerates none.

!!! danger "`item_batcher` is accepted and never read in 1.7.0"

    `map_handler` builds one `Executable` per input and `MapExecutor.execute_item` passes the raw
    item. Nothing in the package constructs the `BatchedInput` that `map`'s own signature offers.
    Measured 2026-08-19: `ItemBatcher(max_items_per_batch=2)` over four strings still yields four
    iterations, each receiving a `str`. Group before the map. `tests/test_batch_scoring.py` pins
    this as an executable test.

### `CompletionConfig` decides when a batch stops

| Field | Type | Default |
| --- | --- | --- |
| `min_successful` | `int` \| `None` | `None` |
| `tolerated_failure_count` | `int` \| `None` | `None` |
| `tolerated_failure_percentage` | `int` \| `float` \| `None` | `None` |

Four constructors ship with it.

| Constructor | Equivalent to |
| --- | --- |
| `CompletionConfig.first_successful()` | `min_successful=1` |
| `CompletionConfig.all_completed()` | every field `None` |
| `CompletionConfig.all_successful()` | `tolerated_failure_count=0, tolerated_failure_percentage=0` |
| `CompletionConfig()` | every field `None`, identical to `all_completed()` |

!!! warning "Exceeding the tolerance never raises"

    The batch completes with `BatchResult.completion_reason` set to
    `CompletionReason.FAILURE_TOLERANCE_EXCEEDED` and returns normally. Reading only
    `get_results()` publishes a partial run as a complete one. Check `completion_reason`, or call
    `throw_if_error()`. [fan-out.md](fan-out.md) has the worked case.

### `ItemBatcher`, `ChildConfig`, `CallbackConfig`, `WaitForCallbackConfig`, `InvokeConfig`

| Config | Fields and defaults |
| --- | --- |
| `ItemBatcher` | `max_items_per_batch=0`, `max_item_bytes_per_batch=0`, `batch_input=None` |
| `ChildConfig` | `serdes`, `item_serdes`, `sub_type`, `summary_generator` all `None`; `is_virtual=False` |
| `CallbackConfig` | `timeout=Duration()`, `heartbeat_timeout=Duration()`, `serdes=None` |
| `WaitForCallbackConfig` | the three above, plus `retry_strategy=None` |
| `InvokeConfig` | `timeout=Duration()`, `serdes_payload=None`, `serdes_result=None`, `tenant_id=None` |
| `ParallelBranch` | `func: Callable`, `name: str` \| `None = None` |

`CallbackConfig` and `InvokeConfig` expose `timeout_seconds`; `CallbackConfig` also exposes
`heartbeat_timeout_seconds`.

!!! warning "A `Duration` of zero disables a timer rather than firing it immediately"

    The harness schedules a callback timeout only when `timeout_seconds > 0`, and the same for the
    heartbeat. An unset `heartbeat_timeout` therefore means no heartbeat deadline at all, not a
    deadline of zero.

## Five enumerations decide behavior

Members are written plain here so the rows fit; they are ordinary enum attributes.

| Enum | Members | Meaning |
| --- | --- | --- |
| `StepSemantics` | AT_MOST_ONCE_PER_RETRY, AT_LEAST_ONCE_PER_RETRY | Whether an interrupted body re-runs |
| `NestingType` | NESTED, FLAT | Whether a branch gets its own CONTEXT operation |
| `JitterStrategy` | NONE, FULL, HALF | How much noise is added to a computed delay |
| `CompletionReason` | ALL_COMPLETED, MIN_SUCCESSFUL_REACHED, FAILURE_TOLERANCE_EXCEEDED | Why it stopped |
| `BatchItemStatus` | SUCCEEDED, FAILED, STARTED | One item's outcome inside a batch |

`FLAT` nesting skips the per-branch CONTEXT operation. The SDK's own docstring puts the saving at
about 30% of operation consumption and the cost at branches no longer appearing separately in
execution history.

!!! note "`STARTED` is how a canceled branch is recorded"

    When the tolerance is exceeded the executor cancels what has not finished, and those items stay
    `STARTED`. So `success_count + failure_count` can be less than `total_count`. Measured
    2026-08-19: three failures against a tolerance of two, out of five items, gave success 1,
    failure 3, started 1.

## `BatchResult` is what `map` and `parallel` return

| Member | Type | Note |
| --- | --- | --- |
| `all` | `list[BatchItem[R]]` | Every item, whatever its status |
| `completion_reason` | `CompletionReason` | The only signal that a run was cut short |
| `get_results()` | `list[R]` | Succeeded items only — failures vanish silently |
| `get_errors()` | `list[ErrorObject]` | Failed items only |
| `throw_if_error()` | `None` | Opt-in: turns any failure into a raise |
| `success_count` / `failure_count` / `started_count` / `total_count` | `int` | Counts by status |
| `succeeded()` / `failed()` / `started()` | `list[BatchItem[R]]` | The items themselves |
| `has_failure` | `bool` | Any item failed |

!!! warning "The top-level `BatchResult` and the `types` one are different objects"

    `aws_durable_execution_sdk_python.BatchResult` is the concrete dataclass from
    `concurrency.models`. `aws_durable_execution_sdk_python.types.BatchResult` is a Protocol
    carrying `get_results()` alone. Annotate against the concrete one or `completion_reason` and
    `get_errors()` are not visible to the type checker.

## Retries: one decision type, two strategy builders, seven presets

| Name | Arguments | For |
| --- | --- | --- |
| `RetryDecision` | `(should_retry: bool, delay: Duration)` | What a strategy returns |
| `RetryDecision.retry` | `(delay: Duration)` | Retry after `delay` |
| `RetryDecision.no_retry` | `()` | Stop, and fail the operation |
| `RetryDecision.delay_seconds` | property, `int` | The delay, in seconds |
| `create_retry_strategy` | `(config: RetryStrategyConfig or None)` | Exponential backoff |
| `create_linear_retry_strategy` | `(config: LinearRetryStrategyConfig or None)` | Linear backoff |

`RetryStrategyConfig` fields: `max_attempts=3`, `initial_delay=Duration.from_seconds(5)`,
`max_delay=Duration.from_minutes(5)`, `backoff_rate=2.0`, `jitter_strategy=FULL`,
`retryable_errors=None`, `retryable_error_types=None`.

`LinearRetryStrategyConfig` fields: `max_attempts=6`, `initial_delay=Duration.from_seconds(1)`,
`increment=Duration.from_seconds(1)`, `max_delay=Duration.from_minutes(5)`,
`jitter_strategy=FULL`, `retryable_errors=None`, `retryable_error_types=None`.

`RetryPresets` lives in `aws_durable_execution_sdk_python.retries` and is not exported at the top
level.

| Preset | Attempts | Initial | Cap | Backoff | Jitter |
| --- | --- | --- | --- | --- | --- |
| `RetryPresets.none()` | 1 | — | — | — | — |
| `RetryPresets.default()` | 6 | 5s | 60s | 2 | `FULL` |
| `RetryPresets.transient()` | 3 | 5s | 5m | 2 | `HALF` |
| `RetryPresets.resource_availability()` | 5 | 5s | 5m | 2 | `FULL` |
| `RetryPresets.critical()` | 10 | 1s | 60s | 1.5 | `NONE` |
| `RetryPresets.linear()` | 6 | 1s | 5m | +1s per attempt | `NONE` |
| `RetryPresets.fixed(interval)` | 5 | `interval` or 5s | same | 1 | `NONE` |

`none()` sets `max_attempts=1`, so the strategy declines on the first failure and no delay is ever
computed. The other six carry the delay columns shown.

!!! note "The two implicit defaults are not the same strategy"

    A step with no `retry_strategy` gets `RetryPresets.default()`: six attempts, capped at 60s.
    A `with_retry` block with no `retry_strategy` gets `create_retry_strategy()`, which is bare
    `RetryStrategyConfig()`: three attempts, capped at five minutes. Measured 2026-08-19 in
    `operation/step.py` line 294 and `retries.py` line 353.

### `with_retry` retries a block of durable operations

```python
with_retry(context, func: Callable[[DurableContext, int], T],
           config: WithRetryConfig, name: str | None = None) -> T
```

| `WithRetryConfig` field | Default | Effect |
| --- | --- | --- |
| `retry_strategy` | `None` | Falls back to bare `RetryStrategyConfig()` |
| `wrap_with_run_in_child_context` | `True` | Final failure arrives as `CallableRuntimeError` |
| `child_context_config` | `None` | A `ChildConfig` forwarded when wrapping is on |

With `wrap_with_run_in_child_context=False` the original exception is re-raised unchanged. Each
loop iteration allocates fresh operation ids, so steps inside the block genuinely re-execute rather
than replaying their checkpoints. [steps.md](steps.md) covers when that matters.

## Waits: two decision types that are not interchangeable

| Name | Fields or signature | For |
| --- | --- | --- |
| `WaitDecision` | `should_wait: bool`, `delay: Duration` | What `create_wait_strategy` returns |
| `WaitDecision.wait` / `.no_wait` | classmethods | Building one |
| `WaitStrategyConfig` | see below | Input to `create_wait_strategy` |
| `create_wait_strategy` | `(config) -> Callable[[T, int], WaitDecision]` | Exponential poll backoff |
| `WaitForConditionDecision` | `should_continue: bool`, `delay: Duration` | What `wait_for_condition` reads |
| `WaitForConditionDecision.stop_polling` | classmethod | Finish, return the state |
| `WaitForConditionDecision.continue_waiting` | `(delay: Duration)` | Poll again after `delay` |
| `WaitForConditionConfig` | `wait_strategy`, `initial_state`, `serdes=None` | Input to `wait_for_condition` |

`WaitStrategyConfig` fields: `should_continue_polling: Callable[[T], bool]`, `max_attempts=60`,
`initial_delay=Duration.from_seconds(5)`, `max_delay=Duration.from_minutes(5)`, `backoff_rate=1.5`,
`jitter_strategy=FULL`, `timeout=None`.

!!! danger "`create_wait_strategy` cannot be handed to `wait_for_condition`"

    It returns `WaitDecision`, whose field is `should_wait`. The `wait_for_condition` executor reads
    `WaitForConditionDecision.should_continue`, and nothing in the SDK converts between them.
    Passing the strategy straight into `WaitForConditionConfig` raises `AttributeError` on the first
    poll. An adapter function is mandatory — [waits.md](waits.md) has the one this repo uses.

`WaitStrategyConfig.timeout` is annotated `# Not implemented yet` in the source. It is accepted and
has no effect in 1.7.0.

## Five decorators, and only one of them is required

| Decorator | Wraps | Note |
| --- | --- | --- |
| `@durable_execution` | `(event, context) -> Any` | The handler. Takes `boto3_client` and `plugins` |
| `@durable_step` | `(StepContext, *args) -> T` | Curries; the call returns a closure and runs nothing |
| `@durable_parallel_branch` | `(DurableContext, *args) -> T` | Fixes the branch name at decoration time |
| `@durable_with_child_context` | `(DurableContext, *args) -> T` | Same currying, for a child context |
| `@durable_wait_for_callback` | `(str, WaitForCallbackContext, *args)` | Same currying, for a submitter |

!!! warning "A decorated call with no `context.step(...)` around it is a silent no-op"

    `@durable_step` returns a closure of one parameter. The body never executes and nothing is
    checkpointed. [sdk-internals.md](sdk-internals.md) shows the wrapper that does it.

The `plugins` keyword on `@durable_execution` is marked EXPERIMENTAL in the SDK docstring, with
known issues, and may change or be removed.

## Exceptions split three ways, and the third way defeats `except Exception`

```text
Exception
└── DurableExecutionsError                    base of everything catchable
    ├── UnrecoverableError                    carries .termination_reason
    │   ├── ExecutionError                    execution ends FAILED, no Lambda retry
    │   │   ├── CallbackError                 .callback_id
    │   │   └── NonDeterministicExecutionError .step_id
    │   └── InvocationError                   raised out of the handler, Lambda retries
    │       ├── BotoClientError               .is_retryable(), .error_category
    │       │   ├── CheckpointError
    │       │   └── GetExecutionStateError
    │       └── StepInterruptedError          .step_id
    ├── UserlandError
    │   └── CallableRuntimeError              .message .error_type .data .stack_trace
    ├── ValidationError                       bad arguments to an operation
    ├── InvalidStateError
    ├── SerDesError                           the default codec refused a type
    └── OrderedLockError

BaseException                                 deliberately NOT caught by `except Exception`
├── SuspendExecution
│   └── TimedSuspendExecution                 .scheduled_timestamp, .from_delay(msg, seconds)
├── BackgroundThreadError                     .source_exception
└── OrphanedChildException
```

| Exception | You catch it when |
| --- | --- |
| `CallableRuntimeError` | Any user-code failure crosses a step, child-context or callback boundary |
| `StepInterruptedError` | An `AT_MOST_ONCE_PER_RETRY` step was interrupted before its end checkpoint |
| `CallbackError` | Only inside the child context — `wait_for_callback` re-wraps it on the way out |
| `SerDesError` | A step returned a type the checkpoint codec does not carry |
| `ValidationError` | You passed something the SDK rejects up front, such as a sub-second `wait` |

!!! danger "The original exception class does not survive a boundary"

    `ErrorObject.from_exception` in `lambda_service.py` flattens every user exception, and
    `to_callable_runtime_error` re-raises it as `CallableRuntimeError`. The class name survives as
    the `error_type` **string**, and so does the message. `isinstance` against the class you raised
    no longer holds, and its attributes are gone. Classify on the message or on `error_type`, and
    put anything a caller must branch on into the message text.

!!! warning "`SuspendExecution` and friends derive from `BaseException` on purpose"

    They are how the runtime suspends and resumes, in the same register as `KeyboardInterrupt`.
    A broad `except Exception` in a step body will not swallow them, which is the intended
    behavior. Never widen a handler to `except BaseException` inside durable code.

`InvocationError` and `ExecutionError` are the fork that decides what happens to the invocation.
An `ExecutionError` ends the execution FAILED. An `InvocationError` is re-raised out of the handler
so Lambda retries the invocation, which is why a `StepInterruptedError` must not be caught by
compensation logic that means to run once.

## SerDes: one ABC, four implementations, one closed type set

| Name | Module | For |
| --- | --- | --- |
| `SerDes` | `serdes` | ABC with `serialize(value, SerDesContext)` and `deserialize(data, SerDesContext)` |
| `SerDesContext` | `serdes` | `operation_id: str = ""`, `durable_execution_arn: str = ""` |
| `ExtendedTypeSerDes` | `serdes` | The default codec for steps and branches; tagged envelopes |
| `JsonSerDes` | `serdes` | Plain `json.dumps` / `json.loads`; the default for `invoke` |
| `PassThroughSerDes` | `serdes` | Stores the string as-is; what an unconfigured callback result uses |
| `FileSystemSerDes` | `filesystem_serdes` | Offloads the payload to a durable filesystem |

The default codec carries exactly these types:

```text
None  str  int  float  bool  bytes  UUID  Decimal  datetime  date  tuple  list  dict  BatchResult
```

Anything else raises `SerDesError: Unsupported type`, after the step body has already run. A
dataclass is not in the set. `src/landing_zone/serdes.py` in this repo is the fix — a `SerDes` that
delegates to `ExtendedTypeSerDes` so a `datetime` field survives the round trip.
[sdk-internals.md](sdk-internals.md) has the measurement and the nested-dataclass caveat.

!!! danger "`FileSystemSerDes` must never point at Lambda's `/tmp`"

    `/tmp` is local to one execution environment. A replay may land in a different one, the file
    will not be found, and deserialization fails. The SDK's own docstring names the two supported
    targets: an S3 bucket mounted as a filesystem, or EFS.

## The top-level package exports less than you need

`from aws_durable_execution_sdk_python import ...` reaches exactly these:

```text
BatchResult              DurableContext           DurableExecutionsError   InvocationError
ParallelBranch           StepContext              ValidationError          WithRetryConfig
durable_execution        durable_step             durable_parallel_branch  with_retry
durable_with_child_context                        durable_wait_for_callback
__version__
```

Everything else comes from a submodule. This is the table that stops the import hunt.

| You want | Import from |
| --- | --- |
| `Duration`, `StepConfig`, `MapConfig`, `ParallelConfig`, `CompletionConfig` | `...config` |
| `ItemBatcher`, `ChildConfig`, `CallbackConfig`, `WaitForCallbackConfig`, `InvokeConfig` | `...config` |
| `StepSemantics`, `NestingType`, `JitterStrategy`, `BatchedInput` | `...config` |
| `RetryDecision`, `RetryStrategyConfig`, `create_retry_strategy`, `RetryPresets` | `...retries` |
| `WaitForConditionConfig`, `WaitForConditionDecision`, `WaitStrategyConfig` | `...waits` |
| `WaitDecision`, `create_wait_strategy` | `...waits` |
| `WaitForConditionCheckContext`, `WaitForCallbackContext`, `LoggerInterface` | `...types` |
| `SerDes`, `SerDesContext`, `ExtendedTypeSerDes`, `JsonSerDes`, `PassThroughSerDes` | `...serdes` |
| `FileSystemSerDes`, `FileSystemSerDesConfig` | `...filesystem_serdes` |
| `CallableRuntimeError`, `StepInterruptedError`, `CallbackError`, `SerDesError` | `...exceptions` |
| `CompletionReason`, `BatchItemStatus`, `BatchItem` | `...concurrency.models` |

`CompletionReason` is the one that catches people out. `BatchResult` and `ParallelBranch` are
re-exported at the top level and it is not.

## The testing package is four runners and one result object

Full treatment is [testing.md](testing.md). This is the surface.

| Name | Signature or fields | Note |
| --- | --- | --- |
| `DurableFunctionTestRunner` | `(handler, poll_interval=1.0)` | Runs the real runtime in-process |
| `DurableChildContextTestRunner` | subclass of the above | Drives a child-context function directly |
| `DurableFunctionCloudTestRunner` | `(function_name, region, ...)` | Drives a function deployed to AWS |
| `WebRunner` | `(config: WebRunnerConfig)` | Serves the same runtime over HTTP endpoints |

`DurableFunctionTestRunner` is a context manager. These are its methods.

| Method | Arguments | Note |
| --- | --- | --- |
| `runner.run` | `(input: str` \| `None, timeout=900)` | Blocks and returns a `DurableFunctionTestResult` |
| `runner.run_async` | `(input, timeout)` | Returns the execution ARN and does not block |
| `runner.wait_for_result` | `(arn, timeout)` | Blocks on an async run |
| `runner.wait_for_callback` | `(arn, name=None, timeout=60)` | Returns the callback id once it is open |
| `runner.send_callback_success` | `(callback_id, result)` | Takes bytes; the handler receives a str |
| `runner.send_callback_failure` | `(callback_id, error)` | Arrives as `CallableRuntimeError` |
| `runner.send_callback_heartbeat` | `(callback_id)` | Resets the heartbeat deadline |

| Result member | Type | Note |
| --- | --- | --- |
| `result.status` | `InvocationStatus` | `SUCCEEDED` or `FAILED` |
| `result.result` | `OperationPayload` \| `None` | `None` when the execution failed — narrow before parsing |
| `result.error` | `ErrorObject` \| `None` | Why it failed |
| `result.operations` | `list[Operation]` | **Top-level only**, in execution order |
| `result.get_all_operations()` | `list[Operation]` | Recurses, and returns **reversed** order |
| `get_step` / `get_wait` / `get_callback` | by name | Top-level scan only |
| `get_context` / `get_invoke` / `get_execution` | by name | Same, per operation type |

A name that matches nothing raises `DurableFunctionsTestError`. A step nested inside a map iteration
or a parallel branch is not top level, so reach it through `get_context(...)` twice, or through
`get_all_operations()`.

| Operation type | Extra fields |
| --- | --- |
| `Operation` | `operation_id`, `operation_type`, `status`, `parent_id`, `name`, `sub_type`, timestamps |
| `ContextOperation` | `child_operations`, `result`, `error`, plus its own `get_step` / `get_context` |
| `StepOperation` | `attempt`, `next_attempt_timestamp`, `result`, `error` |
| `WaitOperation` | `scheduled_end_timestamp` |
| `CallbackOperation` | `callback_id`, `result`, `error` |
| `InvokeOperation` | `result`, `error` |

!!! note "`StepOperation.attempt` counts executions, not retries"

    The harness increments it on RETRY, SUCCEED and FAIL alike. A step that succeeds first time
    reports `attempt == 1`, and two retries then success reports 3.

!!! warning "A step's `.result` is the raw serdes envelope"

    It is the tagged `{"t": ..., "v": ...}` form, not your value. Deserialize it with
    `ExtendedTypeSerDes().deserialize(raw, SerDesContext())`. A primitive return value takes a fast
    path and carries no envelope, so a step returning a `str` looks different from one returning a
    `dict`. An `InvokeOperation.result` is plain JSON, because `invoke` passes `DEFAULT_JSON_SERDES`
    explicitly.

## External reading, and what each one is worth

### AWS documentation

- **[Lambda durable functions][aws-lambda-df]** — the service-level chapter: what a durable
  function is, how it is deployed, and what Lambda charges for.
- **[Durable Execution SDK developer guide][aws-de-guide]** — the API-level guide across languages,
  and the closest thing to a specification for the operation types.
- **[Durable execution best practices][aws-de-best]** — step sizing, determinism rules, and the
  patterns AWS expects. Read it before designing an orchestration, not after.
- **[Lambda durable-function best practices][aws-lambda-best]** — the Lambda-side half: timeouts,
  retention, concurrency, and what to keep out of a step.
- **[Exponential backoff and jitter][aws-jitter]** — the reasoning `JitterStrategy` implements,
  cited by the SDK's own docstring.

The best-practices pages are the ones to read first. They state that step names are part of a
step's deterministic identity, which is the rule to follow even though 1.7.0 identifies steps by
ordinal. [sdk-internals.md](sdk-internals.md) has that measurement and why it is not something to
design around.

### The Python SDK source

Reading the source settles questions the documentation does not reach. These are the files worth
opening, in the order they answer things.

- **[aws/aws-durable-execution-sdk-python][sdk-repo]** — the repo. Issues and `main` both run ahead
  of the released package, so a bug you hit may already be fixed there.
- **[`context.py`][src-context]** — every `DurableContext` method, and the four decorators that
  curry.
- **[`config.py`][src-config]** — every config dataclass with its real defaults, and the
  `NestingType` cost note.
- **[`retries.py`][src-retries]** — the `RetryPresets` numbers, and the `.*` default retryable
  pattern that makes everything retry.
- **[`waits.py`][src-waits]** — the two decision types that are not interchangeable, and the
  `timeout` field marked not implemented.
- **[`serdes.py`][src-serdes]** — the `TypeTag` set, which is the definitive answer to what will
  checkpoint.
- **[`exceptions.py`][src-exceptions]** — which errors retry the invocation and which fail the
  execution.
- **[`execution.py`][src-execution]** — what `@durable_execution` does to the handler, and how an
  error leaves it.
- **[`logger.py`][src-logger]** — the one predicate behind replay-aware logging.
- **[`filesystem_serdes.py`][src-fs-serdes]** — offloading a payload past the checkpoint size
  limit, and the `/tmp` warning.

### Upstream issues that change how you write code

- **[#600 — polling state resets to `initial_state`][issue-600]** — `wait_for_condition` restores
  checkpointed state only when it is truthy. A check returning `None`, `0`, `{}` or `[]` therefore
  restarts from `initial_state` on every attempt, silently. The issue proposes keying on attempt
  count instead.
- **[#574 — silent reset when checkpointed state fails to deserialize][issue-574]** — the same
  reset, from a different cause, with no error surfaced. A custom `serdes` that raises looks
  exactly like a poll that never advances.

Both point the same way. Keep `wait_for_condition` state a non-empty dict, and compute the stop
condition from what the check just observed rather than from an accumulated tally. The local test
runner in testing 1.2.1 drops that state entirely, which makes the discipline compulsory rather
than advisory — [testing.md](testing.md) has the harness measurement.

### This repo

The [index](index.md) lists the worked examples. Every claim on this page is backed by code in this
repository that runs.

```bash
.venv/bin/python -m pytest -q
```

[pypi-sdk]: https://pypi.org/project/aws-durable-execution-sdk-python/
[pypi-testing]: https://pypi.org/project/aws-durable-execution-sdk-python-testing/
[aws-lambda-df]: https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html
[aws-de-guide]: https://docs.aws.amazon.com/durable-execution/
[aws-de-best]: https://docs.aws.amazon.com/durable-execution/patterns/best-practices/
[aws-lambda-best]: https://docs.aws.amazon.com/lambda/latest/dg/durable-best-practices.html
[aws-jitter]: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
[sdk-repo]: https://github.com/aws/aws-durable-execution-sdk-python
[issue-600]: https://github.com/aws/aws-durable-execution-sdk-python/issues/600
[issue-574]: https://github.com/aws/aws-durable-execution-sdk-python/issues/574
[src-context]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/context.py
[src-config]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/config.py
[src-retries]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/retries.py
[src-waits]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/waits.py
[src-serdes]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/serdes.py
[src-exceptions]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/exceptions.py
[src-execution]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/execution.py
[src-logger]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/logger.py
[src-fs-serdes]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/filesystem_serdes.py
