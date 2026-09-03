# Fan-out and composition

Four `DurableContext` methods spread work sideways: `map`, `parallel`, `run_in_child_context` and
`invoke`. This page answers which one a given shape of work needs, what their config dataclasses
actually do, how partial failure is reported, and where the resulting operations turn up in the
execution history. It is for someone who has read [Concepts](concepts.md) and [Steps](steps.md) and
now has more than one thing to run.

Every claim here is measured against SDK 1.7.0 and testing 1.2.1, either by the
repository's own suite or by a probe whose output is quoted. The two worked examples are
`src/batch_scoring/` (a nightly credit-risk run, 30 passing tests) and `src/pipeline_chain/` (three
parallel extracts and a handoff, 21 passing tests).

Upstream references: the [Lambda durable functions
guide](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html), the [durable execution
developer guide](https://docs.aws.amazon.com/durable-execution/), and the [SDK
source](https://github.com/aws/aws-durable-execution-sdk-python).

## What varies decides which primitive you reach for

| Primitive | What varies | What is fixed | Returns |
| --- | --- | --- | --- |
| `context.map` | the data | one function, applied to every item | `BatchResult[R]` |
| `context.parallel` | the work | the set of branches, known at build time | `BatchResult[T]` |
| `context.run_in_child_context` | nothing | one body, run once | the body's `T` |
| `context.invoke` | the function | this execution's role as caller | the callee's `R` |

`map` is for a thousand identical jobs over different rows. `parallel` is for three different jobs
that happen to be independent. `run_in_child_context` is for grouping sequential work so it
succeeds or fails as one unit. `invoke` is for work that wants a different Lambda — a different
timeout, memory size, or IAM role.

The first two return a `BatchResult` and never raise on an item failure. The second two behave like
ordinary calls and propagate whatever their body raised.

!!! note "All four nest"
    A `map` iteration receives a full `DurableContext`, so it can run its own steps, its own
    `parallel`, and its own nested `map`. `src/batch_scoring/handler.py` uses exactly that: the map
    fans out over batches and each iteration runs a `score` step and a `store` step.

## `context.map` runs one iteration per input, not one per batch

```python
def map(
    self,
    inputs: Sequence[U],
    func: Callable[[DurableContext, U | BatchedInput[Any, U], int, Sequence[U]], T],
    name: str | None = None,
    config: MapConfig | None = None,
) -> BatchResult[R]: ...
```

The callable receives four arguments: a fresh child context, the item, its index, and the whole
input sequence. `src/batch_scoring/handler.py` fans a night's loan applications out over a
SageMaker endpoint, one map iteration per batch of applications:

```python
batch_result = context.map(
    batches,
    score_batch,
    name='score_batches',
    config=MapConfig(
        max_concurrency=MAX_CONCURRENT_BATCHES,
        # Inert in SDK 1.7.0: map_handler never reads config.item_batcher, so every
        # iteration still gets one raw input. group_into_batches does the grouping.
        item_batcher=ItemBatcher(max_items_per_batch=APPLICATIONS_PER_BATCH),
        completion_config=CompletionConfig(tolerated_failure_count=TOLERATED_BATCH_FAILURES),
        item_namer=batch_operation_name,
    ),
)
```

The iteration body is a full durable context of its own:

```python
def score_batch(
    batch_context: DurableContext,
    item: list[str] | BatchedInput[Any, list[str]],
    index: int,
    _batches: Sequence[list[str]],
) -> dict:
    """One map iteration. Its `batch_context` is a full DurableContext of its own.

    The declared item type is the union `map` promises. The `BatchedInput` half
    of it is unreachable while `item_batcher` goes unread, so the cast is safe.
    """
    batch = cast(list[str], item)

    def score(step_context: StepContext) -> dict:
        rows = load_feature_rows(batch)
        scorable, rejected = partition_scorable(rows)
        probabilities = invoke_scoring_endpoint(scorable)
        step_context.logger.info(
            'batch %d: %d scorable, %d rejected', index, len(scorable), len(rejected)
        )
        return {'scored': score_rows(scorable, probabilities), 'rejected': list(rejected)}

    scored = batch_context.step(score, name='score', config=step_config)

    def store(step_context: StepContext) -> str:
        key = write_batch_scores(run_id, index, scored['scored'])
        step_context.logger.info('batch %d wrote %s', index, key)
        return key

    scores_key = batch_context.step(store, name='store')
    return batch_summary(index, scored['scored'], scored['rejected'], scores_key)
```

The iteration count is proved by the fakes rather than by the return value:

```python
def test_the_endpoint_is_called_once_per_batch_not_once_per_application(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)

    run(scoring_event(NINE_APPLICATIONS))

    assert len(endpoint.invocations) == 3
    assert [len(batch) for batch in endpoint.invocations] == [3, 3, 3]
    assert len(s3.get_calls) == 9
```

### `ItemBatcher` is accepted and never read

!!! danger "`MapConfig.item_batcher` does nothing in SDK 1.7.0"
    Measured. `map_handler` in
    [`operation/map.py`](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/operation/map.py)
    builds one `Executable` per input, and `MapExecutor.execute_item` passes `self.items[index]`
    raw. Nothing anywhere in the package constructs the `BatchedInput` that `map`'s own signature
    offers. `ItemBatcher(max_items_per_batch=2)` over four strings still yields four iterations,
    each receiving a `str`. Group the inputs yourself before they reach the map.

The repository pins that as an executable test so it flips green if the SDK ever wires it up:

```python
def test_item_batcher_does_not_group_anything_in_sdk_1_7_0():
    OBSERVED_ITEMS.clear()
    with DurableFunctionTestRunner(batcher_probe) as runner:
        result = runner.run(input=json.dumps({'items': ['a', 'b', 'c', 'd']}), timeout=30)

    assert result.result is not None
    assert json.loads(result.result)['iterations'] == 4
    assert [item['type'] for item in OBSERVED_ITEMS] == ['str'] * 4
    assert sorted(item['item'] for item in OBSERVED_ITEMS) == ['a', 'b', 'c', 'd']
```

The grouping therefore lives in `src/batch_scoring/logic.py`, where it is a pure function with its
own tests:

```python
def group_into_batches(application_ids: Sequence[str], max_items_per_batch: int) -> list[list[str]]:
    """Split the run's applications into the batches one map iteration each will score.

    `MapConfig.item_batcher` is inert in SDK 1.7.0, so the grouping the map
    operation appears to offer has to happen before the inputs reach it.
    """
    if max_items_per_batch < 1:
        raise ValueError(f'max_items_per_batch must be at least 1, got {max_items_per_batch}')
    return [
        list(application_ids[start : start + max_items_per_batch])
        for start in range(0, len(application_ids), max_items_per_batch)
    ]
```

### `item_namer` is what makes the history searchable

Without it, iterations are named `map-item-0`, `map-item-1` and so on — the `name_prefix` is the
literal `"map-item-"` in `MapExecutor.from_items`. `item_namer` takes `(item, index)` and returns a
string:

```python
def batch_operation_name(batch: Sequence[str], index: int) -> str:
    """Name a map iteration after the applications it holds, so history is searchable."""
    return f'batch-{index:04d}-{batch[0]}'
```

That produces `batch-0000-APP-0010` in the execution history, which is greppable in CloudWatch and
assertable in a test.

### `MapConfig` and `ParallelConfig` fields

| Field | `MapConfig` | `ParallelConfig` | Effect |
| --- | --- | --- | --- |
| `max_concurrency` | yes | yes | `ThreadPoolExecutor(max_workers=...)`; `None` means unbounded |
| `completion_config` | yes | yes | when the batch is considered done — see below |
| `nesting_type` | yes | yes | `NESTED` or `FLAT`; whether each branch gets a CONTEXT operation |
| `serdes` | yes | yes | codec for the whole `BatchResult` at the handler level |
| `item_serdes` | yes | yes | codec for one item's result inside its child context |
| `summary_generator` | yes | yes | compact JSON stand-in when the result exceeds 256KB |
| `item_batcher` | yes | no | inert in 1.7.0 |
| `item_namer` | yes | no | `(item, index) -> str`, names the iteration |

`max_concurrency` becomes real OS threads, so a shared fake needs a lock. `src/pipeline_chain/`
proves the cap with a probe that records peak in-flight calls:

```python
@pytest.mark.usefixtures('handoff')
def test_max_concurrency_holds_the_third_extract_back(sources: Sources):
    run()

    assert sources.probe.peak == handler_module.MAX_CONCURRENT_EXTRACTS
```

## `NestingType` trades observability for operation count

`NESTED` is the default. Each iteration or branch gets its own CONTEXT operation, and its steps sit
one level below that. `FLAT` makes the child context virtual: it skips its own START, SUCCEED and
FAIL checkpoints, and the operations inside it report to the parent instead.

Measured with a two-item map running one step per iteration, walking
`result.operations` recursively:

```text
=== NESTED ===
fan [ContextOperation]
  map-item-0 [ContextOperation]
    work [StepOperation]
  map-item-1 [ContextOperation]
    work [StepOperation]
total operations: 5

=== FLAT ===
fan [ContextOperation]
  work [StepOperation]
  work [StepOperation]
total operations: 3
```

| | `NESTED` (default) | `FLAT` |
| --- | --- | --- |
| Per-branch CONTEXT operation | yes | no |
| Branch name in history | yes | gone |
| Operations for the tree above | 5 | 3 |
| SDK's own estimate | baseline | ~30% fewer operations |
| Maximum iterations | lower | higher |

!!! warning "`FLAT` discards `item_namer` and every per-branch name"
    Under `FLAT` there is no CONTEXT operation to carry a name, so `item_namer` and
    `ParallelBranch(name=...)` have nothing to name. Both steps above appear as bare `work`
    siblings, indistinguishable in the history. Reach for `FLAT` when the iteration count is
    pushing the execution's operation limit, not to tidy the tree.

## `context.parallel` runs a fixed set of different branches

```python
def parallel(
    self,
    functions: Sequence[Callable[[DurableContext], T] | ParallelBranch[T]],
    name: str | None = None,
    config: ParallelConfig | None = None,
) -> BatchResult[T]: ...
```

`src/pipeline_chain/handler.py` extracts orders, clickstream and inventory from three unrelated
systems. Two branches are dedicated functions carrying `@durable_parallel_branch`; the third is
built from a body shared with every snapshot-table dataset, so `ParallelBranch` names it at the
call site:

```python
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    run_date = run_date_from_event(event)

    def extract_inventory(branch_context: DurableContext) -> dict:
        return snapshot_branch(branch_context, 'inventory', INVENTORY_TABLE, run_date)

    extracts = context.parallel(
        functions=[
            extract_orders(run_date),
            extract_clickstream(run_date),
            ParallelBranch(func=extract_inventory, name='extract-inventory'),
        ],
        name='extract_sources',
        config=ParallelConfig(
            max_concurrency=MAX_CONCURRENT_EXTRACTS,
            completion_config=CompletionConfig(
                tolerated_failure_count=TOLERATED_EXTRACT_FAILURES
            ),
        ),
    )
```

### `durable_parallel_branch` is a decorator factory, and calling it builds the branch

```python
@durable_parallel_branch(name='extract-orders')
def extract_orders(context: DurableContext, run_date: str) -> dict:
    """Two steps, because the Aurora read is the expensive half.

    A failed staging write replays the write alone; the query stays checkpointed.
    """

    def read(step_context: StepContext) -> list[dict]:
        rows = read_orders(run_date)
        step_context.logger.info('orders read rows=%d', len(rows))
        return rows

    rows = context.step(read, name='read_orders', config=EXTRACT_STEP_CONFIG)

    def stage(step_context: StepContext) -> dict:
        key = staging_key(STAGING_PREFIX, 'orders', run_date)
        stage_records(key, rows)
        step_context.logger.info('orders staged at %s', key)
        return extract_summary('orders', key, len(rows))

    return context.step(stage, name='stage_orders', config=EXTRACT_STEP_CONFIG)
```

The decorated function's parameters after the context are bound at the call site.
`extract_orders(run_date)` runs nothing — it returns a `ParallelBranch` whose `func` closes over
`run_date` and takes only a `DurableContext`. This is the same currying `@durable_step` uses; see
[SDK internals](sdk-internals.md).

!!! warning "An unnamed `@durable_parallel_branch()` does not fall back to the function name"
    Its docstring says "If None, the function's `__name__` is used". The implementation passes
    `name=name` straight into `ParallelBranch`, and `ParallelExecutor.get_iteration_name` only
    consults the branch name when it is not `None`. Measured: a decorated branch with no
    name appears in the history as `parallel-branch-0`. Always pass `name=`.

`ParallelBranch` is itself callable and delegates to its wrapped function, which is why it can be
handed to `parallel` in the same list as plain callables.

## `CompletionConfig` decides when the batch stops waiting

| Field | Meaning | Comparison |
| --- | --- | --- |
| `min_successful` | stop once this many have succeeded | `succeeded >= min_successful` |
| `tolerated_failure_count` | breach past this many failures | `failed > tolerated_count` |
| `tolerated_failure_percentage` | breach past this failed share | `failed / total * 100 > tolerated_pct` |

Both tolerances are strict inequalities, so `tolerated_failure_count=2` allows exactly two
failures. All three fields are `None` by default. Three presets are available on the class:
`first_successful()` sets `min_successful=1`, `all_completed()` sets nothing at all, and
`all_successful()` sets both tolerances to `0`. A fourth, `first_completed()`, is commented out in
the source and cannot be called.

Measured over four map items, one of which always raises:

| Config | `completion_reason` | Results kept |
| --- | --- | --- |
| `config` omitted entirely | `FAILURE_TOLERANCE_EXCEEDED` | the 3 that succeeded |
| `MapConfig()` with no `completion_config` | `FAILURE_TOLERANCE_EXCEEDED` | the 2 that finished first |
| `tolerated_failure_count=1` | `ALL_COMPLETED` | 3 |
| `tolerated_failure_percentage=50` | `ALL_COMPLETED` | 3 |
| `tolerated_failure_percentage=10` | `FAILURE_TOLERANCE_EXCEEDED` | 3 |
| `CompletionConfig.all_successful()` | `FAILURE_TOLERANCE_EXCEEDED` | 3 |

The default is zero tolerance. With no criteria set at all, one failure out of four is already a
breach. Tolerating anything requires saying so.

The first two rows differ only in how many results survived the breach, and that is a race rather
than a config difference — see the cancellation note below.

!!! danger "A breached tolerance never raises"
    Exceeding `tolerated_failure_count` produces a SUCCEEDED execution carrying a `BatchResult`
    whose `completion_reason` says otherwise. Reading only `get_results()` publishes a partial run
    as a complete one. Measured on `src/pipeline_chain/`: two of three branches failed against
    `tolerated_failure_count=1`, and the batch still returned normally. A handler that does not read
    `completion_reason` hands off a warehouse that is mostly yesterday.

The handler reads the reason and decides:

```python
# A map never raises. An exceeded tolerance is a SUCCEEDED execution carrying a
# BatchResult that says so, and publishing its partial scores would read as a full night.
if batch_result.completion_reason is CompletionReason.FAILURE_TOLERANCE_EXCEEDED:
    raise ScoringRunAborted(
        f'{batch_result.failure_count} of {batch_result.total_count} batches failed, '
        f'tolerating {TOLERATED_BATCH_FAILURES}'
    )
```

`BatchResult.throw_if_error()` is the shorthand when any failure at all should end the execution.
It raises the first failed item's error, converted to a `CallableRuntimeError`.

### A breach cancels outstanding work, and canceled is not failed

When the tolerance is exceeded the executor cancels the futures that have not started and shuts the
pool down without waiting. A canceled branch is recorded as `BatchItemStatus.STARTED`, so
`success_count + failure_count` can be less than `total_count`. The second row of the matrix above
shows it: three successes were expected and only two arrived, with one item left `STARTED`. The
same effect was measured in `src/batch_scoring/` at three failures against a tolerance of two out of
five items — success 1, failure 3, started 1.

!!! note "`min_successful` stops the waiting, not the work"
    Measured with two branches and `min_successful=1`: the batch returned
    `MIN_SUCCESSFUL_REACHED` with `success_count=1` and `started_count=1`, and the slow branch's
    body had already been entered. Threads already running keep running in the background and raise
    `OrphanedChildException` on their next attempt to checkpoint. A branch with side effects is not
    made safe by a `min_successful` that excludes it.

## Reading a `BatchResult`

| Member | Type | What it gives |
| --- | --- | --- |
| `get_results()` | `list[R]` | results of SUCCEEDED items only, in index order |
| `get_errors()` | `list[ErrorObject]` | errors of FAILED items only |
| `completion_reason` | `CompletionReason` | which criterion ended the batch |
| `success_count` / `failure_count` | `int` | counts of SUCCEEDED and FAILED |
| `started_count` / `total_count` | `int` | items still STARTED, and every item |
| `succeeded()` / `failed()` / `started()` | `list[BatchItem[R]]` | the items, with `index` and `status` |
| `has_failure` / `status` | `bool` / `BatchItemStatus` | whether anything failed at all |
| `throw_if_error()` | `None` | raises the first failure as a `CallableRuntimeError` |
| `all` | `list[BatchItem[R]]` | every item, whatever its status |

`CompletionReason` has exactly three members: `ALL_COMPLETED`, `MIN_SUCCESSFUL_REACHED` and
`FAILURE_TOLERANCE_EXCEEDED`. `BatchItemStatus` has three as well: `SUCCEEDED`, `FAILED` and
`STARTED`.

!!! warning "`CompletionReason` is not exported at the package top level"
    It imports from `aws_durable_execution_sdk_python.concurrency.models`, alongside the concrete
    `BatchResult`. The `BatchResult` exported from `aws_durable_execution_sdk_python.types` is a
    Protocol carrying only `get_results()`; the concrete class `map` and `parallel` actually return
    is the one with `completion_reason`, `failure_count` and `get_errors()`. Import path detail is
    on the [Reference](reference.md) page.

!!! warning "A failed item loses its exception class"
    `ErrorObject.from_exception` flattens every user exception to `ErrorType='CallableRuntimeError'`
    and only `ErrorMessage` survives. Assert on the message, and put anything a caller must branch
    on into the message text.

The repository asserts both halves — a tolerated loss finishes the run, and its error is reachable
by message:

```python
def test_a_batch_lost_within_tolerance_still_finishes_the_run(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.permanent['APP-0060'] = 'ValidationError'

    result = run(scoring_event(NINE_APPLICATIONS))

    assert payload(result)['batches'] == 2
    assert payload(result)['scored'] == 6
    assert payload(result)['bands'] == {'approve': 3, 'refer': 1, 'decline': 2}
    assert SUMMARY_KEY in s3.objects


def test_a_lost_batch_is_reported_by_message_on_the_batch_result(scoring):
    s3, endpoint = scoring
    seed_features(s3, NINE_APPLICATIONS)
    endpoint.permanent['APP-0060'] = 'ValidationError'

    failures = payload(run(scoring_event(NINE_APPLICATIONS)))['failedBatches']

    assert len(failures) == 1
    assert 'ValidationError' in failures[0]
```

## Every branch needs its own retry strategy

A step with no `StepConfig` inherits `RetryPresets.default()`: six attempts, a 5s initial delay,
exponential backoff to a 60s cap, full jitter, and a retryable-error pattern of `.*` that matches
every exception. A permanently poisoned branch therefore holds the whole batch open for minutes
before it can count as a failure.

`src/pipeline_chain/handler.py` classifies instead:

```python
def retry_transient_source(error: Exception, attempt: int) -> RetryDecision:
    """Retry a source that refused the attempt; give up on anything else.

    Failing fast on a permanent error is what gets the other two datasets loaded
    tonight, because the parallel operation tolerates one dead branch.
    """
    if attempt >= EXTRACT_MAX_ATTEMPTS or not is_transient(type(error).__name__):
        return RetryDecision.no_retry()
    return RetryDecision.retry(Duration.from_seconds(EXTRACT_RETRY_DELAY_SECONDS))


EXTRACT_STEP_CONFIG = StepConfig(retry_strategy=retry_transient_source)
```

`src/batch_scoring/handler.py` reaches the same conclusion through `create_retry_strategy`, naming
the endpoint codes a second attempt can fix — `ThrottlingException`, `ModelNotReadyException` and
`ServiceUnavailable`. Retry construction in full is on [Steps](steps.md); the backoff strategy
builders are on [Waits](waits.md).

!!! warning "A step retry inside a branch suspends the whole execution"
    It does not sleep in place. The handler body re-enters and every checkpointed operation
    replays. Measured deterministically across three runs of `src/batch_scoring/`: two body entries,
    and only the throttled iteration's step re-ran. That is what makes an exactly-once assertion on
    a map's side effects meaningful, and it is why the durations in a test are module constants the
    test shrinks.

## `run_in_child_context` groups work so it fails as a unit

```python
def run_in_child_context(
    self,
    func: Callable[[DurableContext], T],
    name: str | None = None,
    config: ChildConfig | None = None,
) -> T: ...
```

Unlike `map` and `parallel`, this returns the body's own value and propagates its exception. It is
the primitive the other two are built on — both wrap their executor in a `child_handler` call.

`src/order_saga/handler.py` puts a card charge and a shipping-label purchase in one child context so
the fulfillment stage fails as a whole, and the parent compensates:

```python
try:
    fulfillment = context.run_in_child_context(fulfill, name='fulfillment')
# StepInterruptedError is deliberately not caught. It asks Lambda to retry the
# invocation, and the stage's FAIL checkpoint reaches this handler as a
# CallableRuntimeError on the replay that follows.
except CallableRuntimeError as stage_failure:
    reason = str(stage_failure)
```

`ChildConfig` carries `serdes`, `item_serdes`, `sub_type`, `summary_generator` and `is_virtual`.
`is_virtual=True` is what `NestingType.FLAT` sets internally: the context becomes a naming scope for
step ids and writes no checkpoints of its own.

!!! note "A `CallableRuntimeError` out of a child context has lost the original exception type"
    `ErrorObject.from_exception` re-wraps the failing step's error, so `error_type` reads
    `CallableRuntimeError` rather than the domain error. The message survives. Compensation must be
    uniform rather than branching on the type.

## `context.invoke` hands off to a different durable function

```python
def invoke(
    self,
    function_name: str,
    payload: P,
    name: str | None = None,
    config: InvokeConfig[P, R] | None = None,
) -> R: ...
```

Loading the warehouse in `src/pipeline_chain/` wants a different timeout, memory size and IAM role,
so it is a second durable function rather than a fourth stage. The chained invoke is durable: the
caller suspends and resumes with the callee's result.

```python
payload = build_load_payload(run_date, extracts.get_results(), DATASETS)
if extracts.completion_reason is CompletionReason.FAILURE_TOLERANCE_EXCEEDED:
    return {'runDate': run_date, 'mode': 'abandoned', 'missing': payload['missing']}

loaded = context.invoke(
    LOAD_FUNCTION_NAME,
    payload,
    name='handoff',
    config=InvokeConfig(timeout=Duration.from_seconds(HANDOFF_TIMEOUT_SECONDS)),
)
return {'runDate': run_date, 'mode': payload['mode'], 'load': loaded}
```

| `InvokeConfig` field | Default | Effect |
| --- | --- | --- |
| `timeout` | `Duration()`, meaning none | how long to wait for the callee |
| `serdes_payload` | `DEFAULT_JSON_SERDES` | codec for the payload sent |
| `serdes_result` | `DEFAULT_JSON_SERDES` | codec for the result returned |
| `tenant_id` | `None` | scopes the invocation to a tenant |

!!! note "Invoke uses plain JSON where steps use the extended codec"
    Steps and branches fall back to `EXTENDED_TYPES_SERDES`, so a `StepOperation.result` is a
    `{"t": ..., "v": ...}` envelope. `context.invoke` passes `DEFAULT_JSON_SERDES` explicitly for
    both payload and result, so an `InvokeOperation.result` is plain JSON. Serialization in full is
    on [SDK internals](sdk-internals.md).

!!! danger "testing 1.2.1 cannot drive `context.invoke` out of the box"
    `OperationTransformer._DEFAULT_PROCESSORS` holds entries for STEP, WAIT, CONTEXT, CALLBACK and
    EXECUTION only. A chained invoke raises an `InvalidParameterValueException` reading
    `Checkpoint for OperationType.CHAINED_INVOKE is not implemented yet`, inside the background
    checkpoint thread. The parent surfaces it as `BackgroundThreadError` and the execution never
    completes, so `runner.run()` raises `TimeoutError` and there is no result object to inspect.
    `tests/conftest_pipeline_chain.py` fills the slot with a processor that completes the invoke
    inside its own START checkpoint. [Testing](testing.md) has the mechanism.

A branch returns plain JSON types for a different reason: its result becomes a `BatchItem` inside
the `BatchResult`, and the default codec for that rejects a dataclass.

```python
def extract_summary(dataset: str, key: str, row_count: int) -> dict:
    """What a branch returns.

    Plain JSON types only: a BatchResult item is serialized with the default
    codec, which rejects a dataclass.
    """
    return {'dataset': dataset, 'stagingKey': key, 'rowCount': row_count}
```

## Nested operations form a tree, and only the top level is directly addressable

A `map` or `parallel` is one CONTEXT operation at the top level. Each iteration or branch is a
CONTEXT operation beneath it. Steps sit one level below that.

```text
result.operations              ['extract_sources', 'handoff']
  extract_sources              ContextOperation  (the parallel)
    extract-orders             ContextOperation  (a branch)
      read_orders              StepOperation
      stage_orders             StepOperation
    extract-clickstream        ContextOperation
      index_clickstream        StepOperation
    extract-inventory          ContextOperation
      snapshot_inventory       StepOperation
  handoff                      InvokeOperation
```

`result.operations` holds only operations whose `parent_id` is `None`. Reach further down with
`get_context(name)` chained, or with `get_all_operations()`, which recurses:

```python
@pytest.mark.usefixtures('sources', 'handoff')
def test_each_branch_is_its_own_context_nested_under_the_parallel_operation():
    """Branch names come from `durable_parallel_branch` and `ParallelBranch`."""
    result = run()

    parallel = result.get_context('extract_sources')

    assert [branch.name for branch in parallel.child_operations] == [
        'extract-orders',
        'extract-clickstream',
        'extract-inventory',
    ]


@pytest.mark.usefixtures('sources', 'handoff')
def test_a_branch_holds_its_own_steps_one_level_further_down():
    result = run()

    parallel = result.get_context('extract_sources')
    orders = parallel.get_context('extract-orders')
    inventory = parallel.get_context('extract-inventory')

    assert [step.name for step in orders.child_operations] == ['read_orders', 'stage_orders']
    assert [step.name for step in inventory.child_operations] == ['snapshot_inventory']
```

!!! warning "`get_step` scans only top-level operations"
    A step inside a map iteration is unreachable with it and raises `DurableFunctionsTestError`.
    `get_all_operations()` recurses, and returns its list in reversed execution order.

The repository pins that too:

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

Prefer `ContextOperation.get_step(name)` over indexing `child_operations`: basedpyright rejects
`.attempt` on a bare `Operation`, and `get_step` casts to `StepOperation`. [Typing and
tooling](typing-and-tooling.md) covers why.

## An iteration returns a pointer, because the whole `BatchResult` is one checkpoint

`CHECKPOINT_SIZE_LIMIT_BYTES` is 256KB, and a `BatchResult` is checkpointed whole. A thousand
iterations each returning their rows will not fit. Return a key and a count:

```python
def batch_summary(
    batch_index: int,
    scored: Sequence[dict],
    rejected: Sequence[str],
    scores_key: str,
) -> dict:
    """The per-iteration result the map checkpoints: counts and a key, never score rows.

    A map's BatchResult is checkpointed whole, against a 256KB limit, so an
    iteration returns a pointer to its output rather than the output.
    """
    return {
        'batchIndex': batch_index,
        'scoresKey': scores_key,
        'scored': len(scored),
        'rejected': list(rejected),
        'bands': tally_bands(scored),
    }
```

`summary_generator` is the escape hatch when the result genuinely exceeds the limit. `map` and
`parallel` install their own by default — `MapSummaryGenerator` and `ParallelSummaryGenerator`,
which emit the counts and the completion reason as JSON — and the operation is marked
`ReplayChildren=true` so the full result is rebuilt from the children on replay.

## What this page measured

- `MapConfig.item_batcher` is accepted and never read in SDK 1.7.0. Group before the map.
- `NestingType.FLAT` removed two of five operations from a two-item map, and took the branch names
  with them.
- A `@durable_parallel_branch()` with no name is `parallel-branch-0`, contradicting its docstring.
- The default completion behavior is zero tolerance: one failure out of four breaches, and the
  batch still returns normally.
- `tolerated_failure_percentage` compares strictly greater, so 50 tolerates one failure in four and
  10 does not.
- `min_successful` stops the waiting rather than the work; a branch already running keeps running.
- A breach cancels unstarted work, and canceled items are recorded `STARTED`, not `FAILED`.
- `context.invoke` cannot be driven by testing 1.2.1 without supplying a CHAINED_INVOKE processor.

None of this is verified against real Lambda. No AWS account exists for this workspace yet.

## Further reading

- [Overview](index.md) — the site, and the `landing_zone` example the other pages build on
- [Concepts](concepts.md) — replay, checkpoints, determinism
- [Steps](steps.md) — `context.step`, retries, step semantics
- [Waits](waits.md) — `wait`, `wait_for_condition`, `wait_for_callback`
- [Testing](testing.md) — the local runner, and the harness gaps this page relies on
- [Reference](reference.md) — the full measured API surface
- [AWS best practices for durable
  execution](https://docs.aws.amazon.com/durable-execution/patterns/best-practices/)
- [Lambda durable functions best
  practices](https://docs.aws.amazon.com/lambda/latest/dg/durable-best-practices.html)
- [`operation/map.py`](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/operation/map.py)
  and
  [`operation/parallel.py`](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/operation/parallel.py)
- [`config.py`](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/config.py)
  — every dataclass on this page
