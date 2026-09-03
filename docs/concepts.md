# The execution model

A durable function is a Lambda function whose single logical run outlives the invocation that
started it. This page is the model every other page assumes: what an execution is, what a
checkpoint is, why the handler body re-runs from the top on every resume, what determinism actually
forbids, and where a given line of code belongs. Read it before [Steps](steps.md),
[Waits](waits.md) and [Fan-out](fan-out.md), which assume all of it.

Measured against `aws-durable-execution-sdk-python` **1.7.0**,
`aws-durable-execution-sdk-python-testing` **1.2.1**, and the Lambda API model shipped in botocore
1.43.74. Every number and every transcript below came from running the code in this repository.
Re-measure before trusting any of it against a newer release.

AWS's own descriptions of the same model are the [Lambda durable functions developer
guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html) and the [durable
execution SDK guide](https://docs.aws.amazon.com/durable-execution/).

## A durable function is one execution spread over many invocations

An ordinary Lambda function has one unit: the invocation. A durable function has two, and keeping
them apart is most of the work.

| Unit | Lives for | Ends when | Bounded by |
| --- | --- | --- | --- |
| **Execution** | up to 366 days | the handler returns or raises, or it times out | `ExecutionTimeout` |
| **Invocation** | up to 90 minutes | the handler returns or suspends | the function's `Timeout` |

One execution is made of many invocations. The service starts an invocation, the SDK replays the
handler up to wherever it got to, the handler runs a little further, and the invocation ends —
either because the execution finished or because the handler suspended waiting for something. The
process does not survive. No local variable, no open socket, no in-memory accumulator carries
across.

What does carry across is the **operation history**: an ordered list of durable operations and
their recorded outcomes. The SDK fetches it at the start of every invocation with
`GetDurableExecutionState` and appends to it with `CheckpointDurableExecution`. Neither is yours to
call. The Lambda API model says so of the checkpoint API, quoted verbatim: *"You typically don't
need to call this API directly as the SDK handles checkpointing automatically."*

An operation is one of six types. The list is exhaustive, read from `OperationType` in
[`lambda_service.py`][lambda_service]:

| `OperationType` | Created by | Recorded outcome |
| --- | --- | --- |
| `EXECUTION` | the execution itself | the handler's return value or error |
| `STEP` | `context.step`, `context.wait_for_condition` | the step's return value |
| `WAIT` | `context.wait` | nothing; a scheduled resume timestamp |
| `CALLBACK` | `context.create_callback` | whatever the external system sent |
| `CONTEXT` | `run_in_child_context`, `map`, `parallel`, `wait_for_callback` | the branch or batch result |
| `CHAINED_INVOKE` | `context.invoke` | the invoked function's result |

Each operation also carries a finer `OperationSubType`, and the mapping is not one-to-one. The
[full table below](#nine-calls-create-operations-everything-else-is-ordinary-code) gives every pair.

## The handler body re-runs from the top; step bodies do not

This is the single fact that makes durable functions different from every other Lambda you have
written. There is no coroutine, no green thread, no saved stack. Resuming means calling
`lambda_handler(event, context)` again from the first line.
[`execution.py`][execution] submits your handler to a thread pool, once, per invocation:

```python
user_future = executor.submit(func, input_event, durable_context)
```

Suspension is an exception. `SuspendExecution` derives from `BaseException`, is raised by
[`suspend.py`][suspend], and unwinds the whole handler:

```python
except SuspendExecution:
    logger.debug("Suspending execution...")
    return DurableExecutionInvocationOutput(status=InvocationStatus.PENDING).to_dict()
```

The handler body re-runs. A durable operation that already has a `SUCCEEDED` checkpoint does not —
its executor returns the deserialized checkpoint and never calls your function.

### One `landing_zone` execution, measured across two invocations

The figure below is not illustrative. It is a real run of [`src/landing_zone/handler.py`][handler]
driven through `DurableFunctionTestRunner`, with a fake S3 that reports an active drop
on the first listing and a settled one afterwards.

```text
INVOCATION 1 ──────────────────────────────────────────────────────────────────────────────
  lambda_handler(event, context)                                    body pass 1
    context.step(check_leader, 'leader')      ordinal 1   no checkpoint
        └─ body RUNS                                      1 × ListDurableExecutionsByFunction
    unquote_plus(...) over event['Records']               RUNS  (ordinary code)
    context.wait_for_condition(..., 'settle') ordinal 2   attempt 1
        └─ check RUNS                                     1 × ListObjectsV2  → not quiet
        └─ RETRY checkpoint written, then SuspendExecution
  invocation returns PENDING                              nothing in the process survives

           ⏸  the service holds the execution until the poll delay elapses

INVOCATION 2 ──────────────────────────────────────────────────────────────────────────────
  lambda_handler(event, context)                                    body pass 2
    context.step(check_leader, 'leader')      ordinal 1   SUCCEEDED
        └─ body SKIPPED, checkpoint returns True          0 × ListDurableExecutionsByFunction
    unquote_plus(...) over event['Records']               RUNS AGAIN  (ordinary code)
    context.wait_for_condition(..., 'settle') ordinal 2   attempt 2
        └─ check RUNS                                     1 × ListObjectsV2  → quiet
        └─ SUCCEED checkpoint written
    context.step(freeze_manifest, ...)        ordinal 3   no checkpoint
        └─ body RUNS                                      1 × ListObjectsV2
    context.step(publish_and_start, ...)      ordinal 4   no checkpoint
        └─ body RUNS                                      1 × PutObject, 1 × StartJobRun
  return {'status': 'ingested', ...}                      execution SUCCEEDED
```

The counters from that run:

| Probe | Count | What it proves |
| --- | --- | --- |
| Invocations of the decorated handler | 2 | the service really did resume it |
| Passes through the handler body | 2 | the body re-runs from the top, it does not resume mid-way |
| `list_durable_executions_by_function` calls | 1 | the `leader` step body ran once across two body passes |
| `list_objects_v2` calls | 3 | settle attempt 1, settle attempt 2, then `freeze_manifest` |
| `put_object` calls | 1 | the side effect did not repeat |
| `start_job_run` calls | 1 | the side effect did not repeat |

`tests/test_replay.py` asserts these counters as a suite, against its own already-settled scenario.
A count of 2 on a side effect is the signature failure of a durable function: an effect sitting in
handler code rather than inside a step.

!!! warning "Ordinary code between operations runs on every pass"

    `unquote_plus` above ran twice, and that is correct and harmless because it is pure. Anything
    with an effect in that position runs twice too, and nothing in the SDK will tell you. The
    number of body passes is not bounded by anything you control — a step retry, a poll attempt, a
    modeled wait and a lost invocation each add one.

## A checkpoint is one operation's durable record, addressed by its ordinal

A checkpoint is a row in the operation history. It carries the operation's identifier, its type and
sub-type, its status, and — for the types that produce a value — the serialized result.

Writing one is an `OperationAction`, and there are five: `START`, `SUCCEED`, `FAIL`, `RETRY`,
`CANCEL`. Reading one back gives an `OperationStatus`, and there are eight: `STARTED`, `PENDING`,
`READY`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `TIMED_OUT`, `STOPPED`. Both enums are in
[`lambda_service.py`][lambda_service].

Only `SUCCEEDED` short-circuits a step body on replay. `FAILED` re-raises the recorded error
instead, which is what makes a compensation branch replay-stable — see [Steps](steps.md).

Two further properties matter to the model.

**Checkpoints are chained.** The Lambda API model states it directly: *"Each checkpoint operation
consumes the current checkpoint token and returns a new one for the next checkpoint. This ensures
that checkpoints are applied in the correct order and prevents duplicate or out-of-order state
updates."* A checkpoint written against a stale token is rejected. That is why answering a callback
the instant its token appears can lose the submitter's own checkpoint — see [Waits](waits.md).

**Checkpoints are addressed by an ordinal, not by a name.** Every context holds its own counter.
Each operation takes the next number, and the operation id is a blake2b hash of that number under
the context's prefix. The name you pass to `context.step(...)` travels beside the id as metadata
and nothing in the SDK compares it. [SDK internals](sdk-internals.md) has the derivation and the
exact source; the consequence is the next section, and it is the most expensive thing on this page.

!!! note "Payload limits are a design constraint, not a runtime detail"

    `CHECKPOINT_SIZE_LIMIT_BYTES` in [`constants.py`][constants] is 256 KB. A step result over that
    pushes a child context into `ReplayChildren` mode. The handler's own return value is checked
    against `LAMBDA_RESPONSE_SIZE_LIMIT`, which `execution.py` sets to `6 * 1024 * 1024 - 50` bytes.
    A larger result is checkpointed instead of returned inline. Return keys and identifiers from
    steps, not payloads.

## Divergence is silent, and a step collects another step's result

The SDK ships an exception named `NonDeterministicExecutionError` in [`exceptions.py`][exceptions].
Measured against SDK 1.7.0: **nothing in the package raises it, and nothing in the
testing package raises it either.** It is a defined type with no throw site. There is no
determinism checker.

So the failure has to be measured directly. This probe follows the shape of
`tests/test_harness_limits.py` — a small `@durable_execution` handler driven through the runner —
and inserts one extra step ahead of an already-checkpointed one on the second body pass:

```python
"""Insert an operation ahead of a checkpointed one on replay and see what happens."""
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner

BODY_ENTRIES = []
OBSERVED = []


@durable_execution
def diverging(_event: dict, context: DurableContext) -> dict:
    BODY_ENTRIES.append('body')
    if len(BODY_ENTRIES) > 1:
        injected = context.step(lambda _: 'injected-ran', name='injected')
        OBSERVED.append(('injected returned', injected))
    alpha = context.step(lambda _: 'alpha-ran', name='alpha')
    OBSERVED.append(('alpha returned', alpha))
    context.wait(Duration.from_seconds(1), name='pause')
    return {'observed': [list(o) for o in OBSERVED], 'entries': len(BODY_ENTRIES)}


with DurableFunctionTestRunner(diverging) as runner:
    result = runner.run(input='{}', timeout=60)
```

Measured output, with the root logger at `WARNING`:

```text
STATUS  : InvocationStatus.SUCCEEDED
ERROR   : None
ENTRIES : 3
OBSERVED: ('alpha returned',    'alpha-ran')     ← invocation 1, ordinal 1, body ran
OBSERVED: ('injected returned', 'alpha-ran')     ← invocation 2, ordinal 1, body NEVER ran
OBSERVED: ('alpha returned',    None)            ← invocation 2, ordinal 2, took the wait's slot
OBSERVED: ('injected returned', 'alpha-ran')
OBSERVED: ('alpha returned',    None)
TOPLEVEL: [('alpha', 'StepOperation'), ('pause', 'WaitOperation'), ('pause', 'WaitOperation')]
```

Read the four findings off that transcript.

- The execution **succeeded**. `result.error` is `None` and no warning was logged.
- `injected` returned `'alpha-ran'`. It claimed ordinal 1, found a `SUCCEEDED` checkpoint there, and
  returned another step's value. Its own body never executed.
- `alpha` then returned `None`. It claimed ordinal 2, which belonged to the `WAIT`, and a wait
  records no result.
- `injected` never appears in `result.operations` at all. It consumed a checkpoint rather than
  creating one, so the history has no trace of it.

!!! danger "A divergent replay is silent data corruption, not an error"

    Nothing raises. Nothing logs. The step whose body you are debugging never ran, and the value it
    returned came from a different step. This is why the determinism rules below are rules rather
    than advice, and why AWS's [best practices for durable
    execution](https://docs.aws.amazon.com/durable-execution/patterns/best-practices/) put
    deterministic orchestration first.

## Five determinism rules, and what each one costs when broken

Determinism here means one thing only: **on every invocation, the handler body must reach the same
durable operations, in the same order, before it reaches new work.** It does not require pure
functions, and it does not forbid side effects — it forbids them outside a step.

| # | Rule | Broken by | Cost |
| --- | --- | --- | --- |
| 1 | No wall clock in the handler body | `time.time()`, `datetime.now()` | a fresh value every pass |
| 2 | No randomness in the handler body | `uuid4()`, `random`, `secrets` | a fresh value every pass |
| 3 | No unordered iteration driving operations | iterating a `set` | operation order differs per invocation |
| 4 | No conditional operation from rules 1-3 | `if time.time() % 2: step(...)` | silent corruption |
| 5 | No module state between operations | a module list appended in the body | warm keeps it, cold does not |

### Rules 1 and 2, measured

A value minted in the handler body is minted again on every pass. A value returned from a step is
frozen at its checkpoint. The same probe shape shows both at once:

```python
@durable_execution
def tokens(_event: dict, context: DurableContext) -> dict:
    body_token = uuid.uuid4().hex[:8]
    BODY_TOKENS.append(body_token)

    def mint(_step_context) -> str:
        token = uuid.uuid4().hex[:8]
        STEP_TOKENS.append(token)
        return token

    step_token = context.step(mint, name='mint')
    context.wait(Duration.from_seconds(1), name='pause')
    return {'body': body_token, 'step': step_token}
```

```text
RESULT     : {"body": "a1bf9f57", "step": "d038c430"}
BODY TOKENS: ['2065624a', 'a1bf9f57']    ← two invocations, two different values
STEP TOKENS: ['d038c430']                ← minted once; the checkpoint answered on the resume
```

The returned `body` is the *second* invocation's token. The first one is gone, and anything that
consumed it — a request already sent, a key already written — now disagrees with what the execution
returns. `landing_zone` puts its clock read inside the settle check for exactly this reason, and the
docstring says so.

### Rule 3, measured

CPython randomizes string hashing per process, so a `set` of keys iterates in a different order in
each one. A durable execution spans processes by construction. Three runs:

```text
['landing/part-0001.csv', 'landing/part-0004.csv', 'landing/part-0002.csv', 'landing/part-0003.csv']
['landing/part-0004.csv', 'landing/part-0002.csv', 'landing/part-0001.csv', 'landing/part-0003.csv']
['landing/part-0001.csv', 'landing/part-0004.csv', 'landing/part-0003.csv', 'landing/part-0002.csv']
```

`landing_zone` builds a set of triggering keys and never iterates it to create operations — it only
tests it for emptiness. Where order becomes durable, it sorts first, in `build_manifest`:

```python
def build_manifest(run_id: str, objects: dict[str, int]) -> Manifest:
    """Freeze a listing into the manifest an ingest job will consume."""
    keys = tuple(sorted(objects))
    return Manifest(run_id=run_id, keys=keys, total_bytes=sum(objects.values()))
```

!!! note "A set is fine; iterating one into operations is not"

    Rule 3 is about what drives operation creation, not about which container you use. Membership
    tests, emptiness checks and lookups are all safe. `for key in some_set: context.step(...)` is
    not. Sort at the boundary, as above, or use [`context.map`](fan-out.md), which takes an ordered
    `Sequence` and indexes iterations by position.

### Rule 5 is the one that looks safe

Module-level state does persist across invocations when Lambda reuses the container, which makes it
look like a working cache. It is empty on a cold start, and a durable execution suspended for a day
resumes cold with near certainty. The probes on this page rely on that persistence deliberately,
because the test runner is one process — see [Testing](testing.md). Do not carry it into a handler.
Durable state is what a step returns.

## `is_replaying()` answers a narrower question than its name suggests

`context.is_replaying()` is real and useful, and it is not a guard. It reports whether **this
context's next operation already has a checkpoint** — nothing about whether a line of code has run
before.

Tracing it at two fixed points in a body that waits once and then retries a step twice, measured:

```text
TRACE : ('body-top',  False)   ← invocation 1: no history at all
TRACE : ('body-top',  True )   ← invocation 2: the wait is checkpointed
TRACE : ('after-wait', False)  ←   ...but nothing follows it yet, so this region is NEW
TRACE : ('body-top',  True )   ← invocation 3: the step now has a RETRY checkpoint
TRACE : ('after-wait', True )  ←   ...so the same line is now REPLAY
TRACE : ('body-top',  True )   ← invocation 4
TRACE : ('after-wait', True )
TRACE : ('after-step', False)  ← past the last checkpointed operation again
```

The same source line reported `False`, then `True`, then `True`. Nothing about the line changed.
What changed is whether an operation after it had been checkpointed yet. The mechanism — the
lookahead in `_replay_aware` and the flip to `NEW` at the boundary — is in
[SDK internals](sdk-internals.md), which covers it for the logger.

A guarded side effect therefore duplicates whenever its region is still the frontier. Measured with
an invocation that dies before any checkpoint lands:

```python
@durable_execution
def guarded(_event: dict, context: DurableContext) -> dict:
    PASSES.append('pass')

    if not context.is_replaying():
        GUARDED.append('side effect')

    if len(PASSES) == 1:
        raise TimedSuspendExecution.from_delay('invocation lost before any checkpoint', 1)

    context.step(lambda _: 'ok', name='only')
    return {'guarded': len(GUARDED), 'passes': len(PASSES)}
```

```text
RESULT      : {"guarded": 2, "passes": 2}
GUARDED RUNS: 2
BODY PASSES : 2
```

!!! danger "Never guard a side effect with `is_replaying()`"

    The guard above ran twice. A step is the only construct that gives you exactly-once execution,
    because only a step writes the checkpoint that suppresses its own body. Use `is_replaying()` for
    observability — a metric you do not want double-counted, a debug line — and nothing else.

## Nine calls create operations; everything else is ordinary code

The full `DurableContext` surface, read from [`context.py`][context] and
`OperationType.from_sub_type`. Nine methods create durable operations. Three members do not.

| `DurableContext` member | `OperationType` | `OperationSubType` | Covered by |
| --- | --- | --- | --- |
| `step(...)` | `STEP` | `Step` | [Steps](steps.md) |
| `wait_for_condition(...)` | `STEP` | `WaitForCondition` | [Waits](waits.md) |
| `wait(...)` | `WAIT` | `Wait` | [Waits](waits.md) |
| `create_callback(...)` | `CALLBACK` | `Callback` | [Waits](waits.md) |
| `wait_for_callback(...)` | `CONTEXT` | `WaitForCallback` | [Waits](waits.md) |
| `run_in_child_context(...)` | `CONTEXT` | `RunInChildContext` | [Steps](steps.md) |
| `map(...)` | `CONTEXT` | `Map`, then `MapIteration` per item | [Fan-out](fan-out.md) |
| `parallel(...)` | `CONTEXT` | `Parallel`, then `ParallelBranch` per branch | [Fan-out](fan-out.md) |
| `invoke(...)` | `CHAINED_INVOKE` | `ChainedInvoke` | [Fan-out](fan-out.md) |
| `is_replaying()` | — | — | this page |
| `set_logger(...)` | — | — | [SDK internals](sdk-internals.md) |
| `execution_context.durable_execution_arn` | — | — | [Reference](reference.md) |

Two entries repay a second look.

`wait_for_condition` is a **`STEP`**, not a `WAIT`. Its poll count is `result.get_step(name).attempt`
in a test, and there is no separate operation per poll.

`wait_for_callback` opens a **child context** holding two operations rather than creating one
callback. Naming it `'review'` produces `'review create callback id'` and `'review submitter'`
inside a context named `'review'`. That naming trips up every first attempt to drive one from a
test; [Waits](waits.md) has the working form.

Everything not in that table is ordinary Python. `json.loads`, a comprehension, a `boto3` client
constructed at module scope, a call into `logic.py` — all of it runs on every body pass, and none
of it is recorded anywhere.

## Put it in a step when repeating it would be wrong, or its value must freeze

The decision has two tests, and either one is sufficient. **Would running this twice be wrong?**
**Must this value be identical on the next invocation?** A yes to either means a step. No to both
means handler code, and a step there is pure cost — [SDK internals](sdk-internals.md) measures what
that cost is.

| Code | Where it goes | Which test it fails |
| --- | --- | --- |
| An AWS API call, an HTTP request, a database write | **step** | repeating it is wrong |
| Publishing a message, starting a job, sending an email | **step** | repeating it is wrong |
| `time.time()`, `datetime.now()` | **step** | the value must freeze |
| `uuid4()`, a nonce, a generated idempotency key | **step** | the value must freeze |
| Reading a config value from a service at runtime | **step** | the value must freeze |
| A read whose result decides which operations run next | **step** | the value must freeze |
| Parsing the event, or building a request body from it | handler | neither — the event is durable |
| Filtering, sorting, arithmetic on data already in memory | handler | neither |
| Calling a pure function in `logic.py` | handler | neither |
| A log line | inside a step body, or plain handler code | never its own step |
| Constructing a `boto3` client | module scope | neither, and it is reused across invocations |
| Reading `os.environ` | module scope | neither |
| Retrying, backing off, timing out | neither | `StepConfig(retry_strategy=...)` — see [Steps](steps.md) |

!!! warning "A read can fail the second test even though it has no side effect"

    `list_landed_objects()` is a harmless read. It is inside a step in `landing_zone` because the
    manifest it produces decides what the ingest job consumes, and two listings taken a minute apart
    do not agree. Purity is not the test; whether the value has to survive the resume is.

The complement is just as firm. Never put pure computation in a step, never give a log line its own
step, and never wrap a whole handler in one. A step buys durability with a blocking checkpoint, and
[SDK internals](sdk-internals.md) has the measurement. AWS reaches the same conclusion in its
[durable functions best
practices](https://docs.aws.amazon.com/lambda/latest/dg/durable-best-practices.html).

## Timeout, retention and execution naming are function configuration

None of this is handler code. It is `DurableConfig` on the function, and the values below are the
bounds declared in the Lambda API model shipped in botocore 1.43.74.

| Field | Unit | Min | Max | Governs |
| --- | --- | --- | --- | --- |
| `ExecutionTimeout` | seconds | 1 | 31,622,400 (366 days) | the whole execution, not one invocation |
| `RetentionPeriodInDays` | days | 1 | 90 | how long `GetDurableExecutionHistory` still answers |
| `KMSKeyArn` | ARN | — | — | encryption of input, output and error payloads |

The function's own `Timeout` is separate and bounds one invocation: 1 to 5,400 seconds. A handler
that suspends never approaches it, which is the point — a 366-day execution is built from
invocations that each last seconds.

An execution is in one of five states, from the `ExecutionStatus` enum in the same model:
`RUNNING`, `SUCCEEDED`, `FAILED`, `TIMED_OUT`, `STOPPED`. `TIMED_OUT` is `ExecutionTimeout`
expiring. `STOPPED` is a `StopDurableExecution` call.

### An execution name makes starting one idempotent

`Invoke` takes an optional `DurableExecutionName`. The API model documents the behavior exactly:

> A unique name for the durable execution. If you invoke a durable function using a name that
> already exists with the same payload, Lambda returns the existing execution instead of creating a
> duplicate. If the payload differs, Lambda returns a `DurableExecutionAlreadyStartedException`
> error. If not specified, Lambda generates a unique identifier automatically.

!!! note "That is a different problem from the one `landing_zone` solves"

    An execution name deduplicates *identical* starts. `landing_zone` receives forty S3 events with
    forty different payloads, so every one is a legitimately distinct execution. Collapsing them
    needs leader election — `is_leader` in the handler — not a name. Reserved concurrency does not
    work either: it throttles, so the extra async invokes retry into a DLQ instead of being ignored.

Provisioning all of this — Terraform, versions, aliases, the S3 trigger wiring — is out of scope for
this site and lives in the separate [deployment
guide](https://docs.ichrisbirch.com/aws/lambda-durable-functions/).

## Where to go next

| Page | Answers |
| --- | --- |
| [Home](index.md) | The example this site is built on, and every finding in one list |
| [Steps](steps.md) | Step semantics, retry strategies, child contexts, and serializing a step's result |
| [Waits](waits.md) | `wait`, `wait_for_condition`, callbacks, and the two clocks on a callback |
| [Fan-out](fan-out.md) | `map`, `parallel`, `invoke`, failure tolerance, and reading a `BatchResult` |
| [Testing](testing.md) | Driving all of it through the test runner, and what the harness cannot do |
| [SDK internals](sdk-internals.md) | The mechanism behind this page, read from the shipped source |
| [Typing and tooling](typing-and-tooling.md) | Why basedpyright reports a missing parameter |
| [Reference](reference.md) | The measured API surface of SDK 1.7.0, in full |

Upstream: the [SDK source](https://github.com/aws/aws-durable-execution-sdk-python), the [durable
execution developer guide](https://docs.aws.amazon.com/durable-execution/), and its [best
practices](https://docs.aws.amazon.com/durable-execution/patterns/best-practices/).

[context]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/context.py
[exceptions]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/exceptions.py
[execution]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/execution.py
[lambda_service]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/lambda_service.py
[suspend]: https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/suspend.py
[handler]: https://github.com/datapointchris/lambda-durable-functions/blob/main/src/landing_zone/handler.py
