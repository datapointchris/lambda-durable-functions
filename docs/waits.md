# Waiting and suspension

This page covers the four calls that suspend a durable execution: `context.wait` for a fixed
duration, `context.wait_for_condition` for polling, `context.wait_for_callback` for waiting on
something outside AWS, and `context.create_callback` for the same thing without the submitter step.
It is for someone who has read [Concepts](concepts.md) and now has to decide which wait to reach for,
how to configure it, and what breaks when they get it wrong.

Every claim marked *measured* was run against aws-durable-execution-sdk-python 1.7.0 and
aws-durable-execution-sdk-python-testing 1.2.1 on 2026-08-19, in this repository's `.venv`.

## Four calls wait, and each one suspends the invocation

| Call | Waits for | Returns | Operation recorded |
| --- | --- | --- | --- |
| `context.wait` | a timer | `None` | `WaitOperation` |
| `context.wait_for_condition` | the check to stop it | the final state | `StepOperation` |
| `context.wait_for_callback` | an external answer | the callback payload | `ContextOperation` |
| `context.create_callback` | nothing on its own | a `Callback` future | `CallbackOperation` |

The `wait_for_condition` operation carries the sub-type `WaitForCondition`, and the
`wait_for_callback` context holds two child operations. Both are measured further down.

None of them blocks a thread for the duration. Each one checkpoints, raises `SuspendExecution`, and
lets the invocation end. Lambda re-invokes the function when the wait is over and the handler body
runs again from the top. [Concepts](concepts.md) covers that replay; this page assumes it.

## A suspended execution is not running, so the length of a wait is a design choice

AWS documents suspension as free: while a durable execution waits, no compute is billed, and the
wait does not consume the invocation's 15-minute limit. See
[Durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html) and the
[durable execution developer guide](https://docs.aws.amazon.com/durable-execution/). That is the
whole reason a five-minute settle window is expressible at all.

!!! note "This claim is documented, not measured here"
    No AWS account is attached to this workspace, so the billing behaviour is taken from the AWS
    documentation rather than from a bill. Everything else on this page was run locally.

The practical consequence is that a wait should be as long as the real-world process it models. A
human review window is hours. A settle period for a file drop is minutes. Shortening either one to
make a test finish faster is the wrong lever — shrink the constant in the test instead, as
[Testing](testing.md) describes.

## `context.wait` takes whole seconds, and anything under one fails the execution

`context.wait` converts the `Duration` to seconds and refuses a value below one. Measured from
`context.py`:

```python
def wait(self, duration: Duration, name: str | None = None) -> None:
    seconds = duration.to_seconds()
    if seconds < 1:
        msg = "duration must be at least 1 second"
        raise ValidationError(msg)
```

The example in `tests/test_harness_limits.py` is the minimal shape:

```python
@durable_execution
def waits(_event: dict, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(3), name='long')
    return 'done'
```

`Duration` holds a whole number of seconds. These are every way to build one and read it back:

| Constructor | Example | Seconds |
| --- | --- | --- |
| `Duration(seconds=...)` | `Duration(seconds=90)` | 90 |
| `Duration.from_seconds(v)` | `Duration.from_seconds(30)` | 30 |
| `Duration.from_minutes(v)` | `Duration.from_minutes(5)` | 300 |
| `Duration.from_hours(v)` | `Duration.from_hours(4)` | 14400 |
| `Duration.from_days(v)` | `Duration.from_days(1)` | 86400 |
| `duration.to_seconds()` | `Duration.from_hours(4).to_seconds()` | 14400 |

!!! danger "A sub-second duration truncates to zero and fails the whole execution"
    `Duration.from_seconds` casts with `int()`, so `Duration.from_seconds(0.5).to_seconds()` is `0`
    — measured. `context.wait` then raises `ValidationError`, which is not caught anywhere in the
    SDK, so the execution ends `InvocationStatus.FAILED` with
    `ErrorObject(message='duration must be at least 1 second', type='ValidationError')`. Measured on
    2026-08-19. A fractional duration is never a short wait; it is a dead function.

A modeled wait was measured at one operation and real wall-clock time. A 2-second wait took 2.31s,
recorded `[('cool_off', 'WaitOperation')]` at the top level, and carried a
`scheduled_end_timestamp` of `2026-08-19 18:45:36.745223+00:00`. Read it back with
`result.get_wait('cool_off')`.

## Never `time.sleep` in the handler — the wait has to be an operation

A `time.sleep` holds the invocation open, burns billed compute, counts against the 15-minute limit,
and is re-executed in full on every replay because nothing checkpointed it. `context.wait` does the
opposite of all four. The same argument applies to a polling loop written with `while` and
`time.sleep`: it belongs in `wait_for_condition`, where each poll is a checkpoint.

!!! warning "A wait inside a step body is worse than a wait in the handler"
    A step body runs to completion inside one invocation. Sleeping in one holds a billed invocation
    open and gains nothing durable. [Steps](steps.md) covers what belongs inside a step body.

## `wait_for_condition` runs the check first, then the strategy, then suspends

The contract is two callables and one state value that is threaded between them.

| Piece | Signature | Job |
| --- | --- | --- |
| `check` | `(state: T, ctx: WaitForConditionCheckContext) -> T` | observe the world, return the new state |
| `wait_strategy` | `(state: T, attempt: int) -> WaitForConditionDecision` | stop, or wait how long |
| `initial_state` | `T` | what `check` receives on attempt 1 |
| `serdes` | `SerDes \| None` | how the state is checkpointed, default `ExtendedTypeSerDes` |

The strategy returns one of two decisions. `WaitForConditionDecision.stop_polling()` ends the wait
and `wait_for_condition` returns the state the check just produced.
`WaitForConditionDecision.continue_waiting(Duration.from_seconds(n))` checkpoints a RETRY and
suspends for `n` seconds.

```text
  attempt 1                                                    attempt n + 1
      │                                                              ▲
      ▼                                                              │
  state = initial_state                    state = checkpointed result if truthy,
      │                                            otherwise initial_state
      │                                                              │
      ├──────────────────────────────────────────────────────────────┤
      ▼                                                              │
  check(state, check_context) ──► new_state                          │
      │                                                              │
      ▼                                                              │
  wait_strategy(new_state, attempt)                                  │
      │                                                              │
      ├── stop_polling() ──► checkpoint SUCCEED (blocks) ──► return new_state
      │                                                              │
      └── continue_waiting(delay) ──► checkpoint RETRY (blocks)      │
                                             │                       │
                                             ▼                       │
                       suspend for max(delay, 1s), no compute billed │
                                             │                       │
                                             ▼                       │
                            Lambda re-invokes, handler body replays ─┘
```

The first check runs immediately. There is no wait before attempt 1, so a `wait_for_condition` whose
condition is already met costs one check and no suspension at all.

The check runs as part of a durable operation, so it is the right place for a clock read or an API
call. `check_context.logger` is the replay-aware logger; [SDK internals](sdk-internals.md) covers
why that matters.

## `attempt` is 1-based, so index a delay schedule with `attempt - 1`

Measured from `operation/wait_for_condition.py`:

```python
attempt: int = 1
if checkpointed_result.operation and checkpointed_result.operation.step_details:
    attempt = checkpointed_result.operation.step_details.attempt + 1
```

`landing_zone` turns that into a pure function, so the off-by-one is testable without the SDK:

```python
def next_poll_delay_seconds(attempt: int, schedule: tuple[int, ...], steady: int) -> int:
    """Delay before poll `attempt`, which the SDK numbers from 1.

    Indexing `schedule` with `attempt` rather than `attempt - 1` silently skips
    element 0 and falls through to `steady`.
    """
    index = attempt - 1
    return schedule[index] if index < len(schedule) else steady
```

`tests/test_logic.py` pins both ends of it:

```python
def test_the_first_attempt_uses_schedule_element_zero():
    assert next_poll_delay_seconds(1, schedule=(60, 120), steady=300) == 60


@pytest.mark.parametrize(('attempt', 'expected'), [(2, 120), (3, 300), (9, 300)])
def test_later_attempts_walk_the_schedule_then_hold_steady(attempt, expected):
    assert next_poll_delay_seconds(attempt, schedule=(60, 120), steady=300) == expected
```

The failure mode is silent. A schedule indexed with `attempt` never uses element 0, and the first
poll falls through to the steady delay, which is usually much longer. Nothing errors.

## The settle poll reads the world, never a running tally

`landing_zone` waits until nothing new has landed in an S3 prefix for a quiet period. The check
lists the prefix and compares the newest object's timestamp to now. The strategy stops when the
listing is both quiet and non-empty.

```python
def settle_check(_state: dict, check_context: WaitForConditionCheckContext) -> dict:
    """Reads the clock, so it must run inside the step to be replay-stable."""
    landed = list_landed_objects()
    newest = max((o['modified_epoch'] for o in landed.values()), default=None)
    quiet = is_quiet(newest, time.time(), QUIET_PERIOD_SECONDS)
    check_context.logger.info('landed=%d quiet=%s', len(landed), quiet)
    return {'object_count': len(landed), 'quiet': quiet}


def keep_waiting(state: dict, attempt: int) -> WaitForConditionDecision:
    if state['quiet'] and state['object_count']:
        return WaitForConditionDecision.stop_polling()
    seconds = next_poll_delay_seconds(attempt, POLL_DELAYS_SECONDS, STEADY_POLL_DELAY_SECONDS)
    return WaitForConditionDecision.continue_waiting(Duration.from_seconds(seconds))


context.wait_for_condition(
    check=settle_check,
    config=WaitForConditionConfig(
        wait_strategy=keep_waiting,
        initial_state={'object_count': 0, 'quiet': False},
    ),
    name='settle',
)
```

The decision itself is a pure function with no SDK types in it:

```python
def is_quiet(newest_epoch: float | None, now_epoch: float, quiet_seconds: int) -> bool:
    """True when nothing has landed recently enough to expect more.

    Settle detection reads the newest object's timestamp rather than comparing
    successive listings, so a poll attempt needs no memory of the one before it.
    An empty prefix is never quiet — there is nothing to ingest yet.
    """
    if newest_epoch is None:
        return False
    return (now_epoch - newest_epoch) >= quiet_seconds
```

Comparing successive listings would have been the obvious implementation. It would also have made
the stop condition depend on state carried between attempts, which is exactly what the next section
says not to rely on.

## `wait_for_condition` is one operation, however many polls it makes

There is no separate operation per poll. Measured with a three-poll condition on 2026-08-19:

```text
ops                  : [('probe', 'StepOperation', OperationSubType.WAIT_FOR_CONDITION)]
attempt on operation : 3
raw result           : {"t":"m","v":{"polls":{"t":"i","v":1}}}
```

So `result.get_step('<name>').attempt` is the number of polls performed, and the operation's
`result` is the serialized final state. It is the raw serdes envelope — decode it with
`ExtendedTypeSerDes().deserialize(raw, SerDesContext())`. [Testing](testing.md) covers reading
operations back.

`landing_zone` asserts the whole top-level history in `tests/test_handler.py`, which shows the wait
sitting among ordinary steps:

```python
assert [op.name for op in result.operations] == [
    'leader',
    'settle',
    'freeze_manifest',
    'publish_and_start',
]
```

## State is restored only when the checkpointed result is truthy

This is the trap that decides how a poll should be written. Measured from
`operation/wait_for_condition.py`:

```python
if checkpointed_result.is_started_or_ready() and checkpointed_result.result:
    current_state = deserialize(...)
else:
    current_state = self.config.initial_state
```

!!! warning "A falsy checkpoint restarts the poll from `initial_state`, silently"
    The guard tests the checkpointed value for truthiness rather than for presence. Whenever it is
    falsy the check receives `initial_state` again instead of what the previous attempt returned,
    and nothing is logged. Upstream
    [issue #600](https://github.com/aws/aws-durable-execution-sdk-python/issues/600) identifies this
    and proposes keying on the attempt count instead.

The checkpointed value is the *serialized* state, not the state object, which narrows when the guard
actually bites. Measured against `ExtendedTypeSerDes` on 2026-08-19, every falsy Python value
serializes to a non-empty string:

```text
None  -> 'null'    0  -> '0'     False -> 'false'
{}    -> '{"t":"m","v":{}}'      []    -> '[]'      '' -> '""'
```

So under the default codec the guard degrades to a presence check, and a check returning `None`
still round-trips. It reaches the `initial_state` branch when `checkpointed_result.result` is
actually `None` — no payload persisted — or when a custom `SerDes` returns an empty string.
[SDK internals](sdk-internals.md) covers writing one.

The local harness hits the `None` case on every single retry, because testing 1.2.1 does not persist
the RETRY payload at all. Measured with a counter that should have reached 3:

```text
check saw    : [{'polls': 0}, {'polls': 0}, {'polls': 0}]
strategy saw : [({'polls': 1}, 1), ({'polls': 1}, 2), ({'polls': 1}, 3)]
```

`attempt` advances correctly. The state does not. The mechanism is a harness bug rather than an SDK
bug, and [Testing](testing.md) traces it through the in-memory step processor;
`tests/test_harness_limits.py` pins it as a strict `xfail` so it flips green when fixed.

!!! danger "Never write a stop condition that depends on accumulated state"
    A poll that stops when a running tally crosses a threshold never terminates in the local
    harness, and is fragile against #600 in production. Compute the stop condition from what the
    check just observed. `is_quiet(newest_epoch, now_epoch, quiet_seconds)` above is the shape:
    every attempt reaches the same verdict from the same observation, with no memory.

The rule generalises past both bugs. A poll whose decision comes from the world it just observed is
easier to reason about than one carrying a tally, and it survives a lost checkpoint.

## A poll delay below one second is clamped up to one second

The executor rounds any sub-second delay up and logs a warning. Measured from
`operation/wait_for_condition.py`:

```python
delay_seconds = decision.delay_seconds
if delay_seconds is not None and delay_seconds < 1:
    logger.warning(
        "WaitDecision delay_seconds step for id: %s, name: %s,"
        "is %d < 1. Setting to minimum of 1 seconds.",
        ...
    )
    delay_seconds = 1
```

Combined with the absence of clock skipping in testing 1.2.1, one second is the floor for a poll in
a local test. A test that polls ten times costs at least ten seconds of real wall clock.

## `create_wait_strategy` gives backoff with jitter, in the wrong decision type

`WaitStrategyConfig` plus `create_wait_strategy` builds an exponential-backoff strategy so you do
not hand-roll one. The fields, with their measured defaults:

| Field | Default | Meaning |
| --- | --- | --- |
| `should_continue_polling` | required | `Callable[[T], bool]` — takes the state only, no attempt |
| `max_attempts` | `60` | stop polling once `attempts_made >= max_attempts` |
| `initial_delay` | `Duration.from_seconds(5)` | the delay before attempt 2 |
| `max_delay` | `Duration.from_minutes(5)` | ceiling on the computed delay |
| `backoff_rate` | `1.5` | multiplier per attempt |
| `jitter_strategy` | `JitterStrategy.FULL` | how the delay is randomised |
| `timeout` | `None` | declared and not implemented in 1.7.0 |

The delay is computed as `min(initial_delay * backoff_rate ** (attempts_made - 1), max_delay)`,
jittered, then rounded up with a floor of one second.

| `JitterStrategy` | Delay returned |
| --- | --- |
| `NONE` | the exact computed delay |
| `HALF` | `delay / 2 + random(0, delay / 2)` |
| `FULL` | `random(0, delay)` |

Full jitter is the default and is what the
[AWS backoff and jitter article](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
recommends for spreading a herd of pollers.

!!! danger "`create_wait_strategy` cannot be passed to `wait_for_condition` directly"
    `create_wait_strategy` returns `WaitDecision`, whose field is `should_wait`. The
    `wait_for_condition` executor reads `WaitForConditionDecision.should_continue`. Nothing in the
    SDK converts between them, so passing the strategy straight into `WaitForConditionConfig` raises
    `AttributeError` on the first poll. An adapter is mandatory.

`flaky_api_sync` writes that adapter once:

```python
def as_condition_strategy(
    wait_strategy: Callable[[dict, int], WaitDecision],
) -> Callable[[dict, int], WaitForConditionDecision]:
    """Adapt a `create_wait_strategy` callable to what `wait_for_condition` reads.

    `create_wait_strategy` returns `WaitDecision`, whose field is `should_wait`.
    The wait_for_condition executor reads `WaitForConditionDecision.should_continue`
    and nothing in the SDK converts between them, so passing the strategy straight
    into `WaitForConditionConfig` raises `AttributeError` on the first poll.
    """

    def decide(observed: dict, attempts_made: int) -> WaitForConditionDecision:
        decision: WaitDecision = wait_strategy(observed, attempts_made)
        if not decision.should_wait:
            return WaitForConditionDecision.stop_polling()
        return WaitForConditionDecision.continue_waiting(decision.delay)

    return decide


def build_export_poll_strategy() -> Callable[[dict, int], WaitForConditionDecision]:
    """Exponential backoff with full jitter, built from the SDK's own wait strategy."""
    return as_condition_strategy(
        create_wait_strategy(
            WaitStrategyConfig(
                should_continue_polling=export_is_running,
                max_attempts=POLL_MAX_ATTEMPTS,
                initial_delay=Duration.from_seconds(POLL_INITIAL_DELAY_SECONDS),
                max_delay=Duration.from_seconds(POLL_MAX_DELAY_SECONDS),
                backoff_rate=POLL_BACKOFF_RATE,
                jitter_strategy=JitterStrategy.FULL,
            )
        )
    )
```

`should_continue_polling` receives the state alone, which keeps it a plain predicate:

```python
def export_is_running(export: dict) -> bool:
    """True while the partner is still building the export.

    Reads only the status just observed. Poll state is not threaded between
    attempts, so a stop condition computed from accumulated counters never fires.
    """
    return export['status'] == 'RUNNING'
```

!!! warning "Exhausting `max_attempts` looks identical to the condition being met"
    `create_wait_strategy` returns `WaitDecision.no_wait()` both when the predicate says stop and
    when `attempts_made >= max_attempts`. `wait_for_condition` then returns normally in either case.
    The caller has to re-read the final state to tell them apart, which is what `flaky_api_sync`
    does immediately after the wait:

    ```python
    observed = context.wait_for_condition(
        check=read_export_status,
        config=WaitForConditionConfig(
            wait_strategy=build_export_poll_strategy(),
            initial_state={'status': 'RUNNING', 'record_count': 0},
        ),
        name='export_ready',
    )
    if observed['status'] != 'READY':
        return {'status': 'export-unavailable', 'exportId': export_id, 'partnerStatus': observed['status']}
    ```

## `wait_for_callback` hands a token out and suspends until someone answers

Polling is for a system that only answers when asked. A callback is for a system that will come back
to you — a person approving a change, a partner posting a webhook, a job that finishes on its own
schedule. `approval_gate` is a change-request gate: it posts the request to the approval service's
SQS queue with a callback token, suspends, and applies or declines the change when the answer
arrives.

```python
def request_review(callback_token: str, callback_context: WaitForCallbackContext) -> None:
    message_id = post_review_request(build_review_message(change_request, callback_token))
    callback_context.logger.info('review of %s queued as %s', change_request.request_id, message_id)

try:
    answer = context.wait_for_callback(
        request_review,
        name='review',
        config=WaitForCallbackConfig(
            timeout=Duration.from_seconds(REVIEW_WINDOW_SECONDS),
            heartbeat_timeout=Duration.from_seconds(REVIEW_SERVICE_HEARTBEAT_SECONDS),
        ),
    )
except CallableRuntimeError as gate_failure:
    decision = Decision(verdict=DECLINED, reason=classify_gate_failure(gate_failure.message))
else:
    decision = parse_decision(answer)
```

The submitter is handed the token and does one thing with it: give it to whoever will answer. Its
declared return type is `None`, and `wait_for_callback` returns `callback.result()` rather than
anything the submitter produced. Returning a value from it therefore reaches nobody.

The outside world answers with three APIs, which the local runner mirrors:

| AWS API | Local runner | Effect |
| --- | --- | --- |
| `SendDurableExecutionCallbackSuccess` | `send_callback_success(id, result)` | the wait returns the payload |
| `SendDurableExecutionCallbackFailure` | `send_callback_failure(id, error)` | the wait raises |
| `SendDurableExecutionCallbackHeartbeat` | `send_callback_heartbeat(id)` | resets the heartbeat timer |

## The gate decomposes into a child context, a callback and a submitter step

`wait_for_callback` is a helper. Measured from `operation/callback.py`, it opens a child context and
puts two operations inside it:

```python
name_with_space: str = f"{name} " if name else ""
callback: Callback = context.create_callback(
    name=f"{name_with_space}create callback id", config=config
)

def submitter_step(step_context: StepContext):
    return submitter(
        callback.callback_id, WaitForCallbackContext(logger=step_context.logger)
    )

context.step(func=submitter_step, name=f"{name_with_space}submitter", config=step_config)

return callback.result()
```

So `wait_for_callback(name='review')` produces the tree `tests/test_approval_gate.py` asserts:

```python
assert [op.name for op in result.operations] == [GATE_CONTEXT_NAME, 'apply_change', 'record_outcome']
gate_context = result.get_context(GATE_CONTEXT_NAME)
assert [op.name for op in gate_context.child_operations] == [
    CALLBACK_OPERATION_NAME,
    SUBMITTER_STEP_NAME,
]
```

with those constants defined as the measured names:

```python
CALLBACK_OPERATION_NAME = 'review create callback id'
GATE_CONTEXT_NAME = 'review'
SUBMITTER_STEP_NAME = 'review submitter'
```

!!! warning "`runner.wait_for_callback` matches the callback operation, not the name you passed"
    Asking the runner for `'review'` returns `None` forever and the call raises `TimeoutError` after
    its timeout. It has to be asked for `'review create callback id'`. Measured on 2026-08-19.
    [Testing](testing.md) covers driving a callback from a test.

The submitter runs in an ordinary step, which means at-least-once semantics apply to it. Posting the
same token twice is harmless; posting a second *different* request is not. [Steps](steps.md) covers
`StepSemantics` and when the distinction bites.

## Two clocks bound a callback, and a zero duration disables one

`CallbackConfig` carries both, and `WaitForCallbackConfig` extends it with a `retry_strategy` for
the submitter step.

| Field | Type | Watches |
| --- | --- | --- |
| `timeout` | `Duration` | the whole wait — how long the answer may take |
| `heartbeat_timeout` | `Duration` | the answering system — how long it may go silent |
| `serdes` | `SerDes \| None` | how the returned payload is decoded |
| `retry_strategy` | `Callable[[Exception, int], RetryDecision] \| None` | retries the submitter step only |

`approval_gate` names what each one is for:

```python
REVIEW_WINDOW_SECONDS = 4 * 60 * 60
REVIEW_SERVICE_HEARTBEAT_SECONDS = 15 * 60
```

The window is the business deadline. The heartbeat watches the approval service itself: it beats
while the request is still open in the queue, so a service that dies is noticed in minutes rather
than holding the change open for four hours.

!!! danger "A `Duration` of zero disables the timer instead of firing immediately"
    Both fields default to `Duration()`, which is zero seconds. Measured from the harness executor,
    each timer is scheduled only `if callback_options.timeout_seconds > 0` and only
    `if callback_options.heartbeat_timeout_seconds > 0`. An unset `heartbeat_timeout` therefore
    means no heartbeat deadline at all, and an unset `timeout` means nothing in the gate will ever
    end the wait. Set `timeout` on every callback, and treat `heartbeat_timeout` as opt-in.

`tests/conftest_approval_gate.py` uses that behaviour deliberately, setting the heartbeat to zero so
only the tests that mean to exercise it turn it on:

```python
monkeypatch.setattr(handler_module, 'REVIEW_WINDOW_SECONDS', 20)
monkeypatch.setattr(handler_module, 'REVIEW_SERVICE_HEARTBEAT_SECONDS', 0)
```

## Every callback failure arrives as one exception type

A reviewer rejecting the change, the window lapsing, and the approval service going silent all reach
the handler as `CallableRuntimeError`. `Callback.result()` raises `CallbackError` inside the child
context; `CallbackError` is an `ExecutionError` rather than an `InvocationError`, so the child
executor catches it and re-raises `error_object.to_callable_runtime_error()`.

The three cases are separated only by the message string. Measured markers:

| Cause | Message | Classified as |
| --- | --- | --- |
| `send_callback_failure` | the caller's `ErrorObject` message, verbatim | `reviewer-rejected` |
| `timeout` elapsed | `Callback timed out: Callback.Timeout` | `window-lapsed` |
| `heartbeat_timeout` elapsed | `Callback heartbeat timed out: Callback.Heartbeat` | `review-service-silent` |

That makes the classification a pure function, testable without the SDK:

```python
WINDOW_TIMEOUT_MARKER = 'Callback.Timeout'
HEARTBEAT_TIMEOUT_MARKER = 'Callback.Heartbeat'


def classify_gate_failure(message: str | None) -> str:
    """Why the gate failed.

    A rejection, a lapsed window and a dead approval service all reach the handler
    as one exception type, so the message is the only thing separating them.
    """
    text = message or ''
    if HEARTBEAT_TIMEOUT_MARKER in text:
        return REVIEW_SERVICE_SILENT
    if WINDOW_TIMEOUT_MARKER in text:
        return WINDOW_LAPSED
    return REVIEWER_REJECTED
```

!!! note "The `except` branch is replay-stable"
    On a later replay the same `CallableRuntimeError` is re-raised from the failed checkpoint, so
    the handler takes the same branch every pass. Catching the failure and deciding from it is
    deterministic, which is what makes it safe to do in the handler body rather than in a step.

## The answer arrives as bytes and is read back as a raw string

`send_callback_success` takes `bytes | None`. The harness stores `result.decode()`. With no `serdes`
configured, `Callback.result()` deserializes with `PASS_THROUGH_SERDES`, so the handler receives the
raw JSON string and parses it itself:

```python
def approval(reviewer: str = 'dana@example.com', note: str = '') -> bytes:
    """The body the approval service sends back with the token."""
    return json.dumps({'verdict': 'approved', 'reviewer': reviewer, 'note': note}).encode()
```

```python
def parse_decision(answer: str | None) -> Decision:
    """Read the approval service's answer.

    An answer nobody signed is not an approval, however it is spelled — the audit
    row has to name a reviewer, so a payload without one can only be declined.
    """
    try:
        parsed = json.loads(answer) if answer else None
    except json.JSONDecodeError:
        parsed = None
    if not isinstance(parsed, dict) or not parsed.get('reviewer'):
        return Decision(verdict=DECLINED, reason=UNSIGNED_DECISION)
    ...
```

Treat the payload as untrusted. It came from outside the execution, and `parse_decision` declines
anything that is not a signed dictionary rather than trusting a `verdict` field on its own.

## `create_callback` is the same machinery without the submitter step

`context.create_callback(name, config) -> Callback` mints the token and returns a future. You hand
the token out yourself and call `callback.result()` when you want to suspend. That is what
`wait_for_callback` does internally, shown in the decomposition above.

Reach for it when the token has to travel somewhere `wait_for_callback`'s single submitter cannot
express — several recipients, or a token embedded in a payload built by a later step. Otherwise
`wait_for_callback` is the same thing with the submitter already wrapped in a checkpointed step,
which is what you want.

## Durations in a test are module constants the test shrinks

testing 1.2.1 has no clock skipping. `SkipClock` and a `skip_time` flag exist on `main` and are not
released. So every modeled wait costs real wall-clock time, measured at 2.31s for a 2-second wait on
2026-08-19.

The consequence for design is concrete: a handler's durations are module-level constants, never
literals inside the call, so a test can shrink them.

```python
QUIET_PERIOD_SECONDS = 300
POLL_DELAYS_SECONDS = (60,)
STEADY_POLL_DELAY_SECONDS = 300
```

```python
monkeypatch.setattr(handler_module, 'QUIET_PERIOD_SECONDS', 300)
monkeypatch.setattr(handler_module, 'POLL_DELAYS_SECONDS', (1,))
monkeypatch.setattr(handler_module, 'STEADY_POLL_DELAY_SECONDS', 1)
```

Only the poll delays are real waits. `QUIET_PERIOD_SECONDS` is a threshold compared against a clock
the fixture freezes, so it stays at its production value and costs nothing. The two delays drop to
one second, which is the floor the executor clamps to anyway.

Keep every real duration in a test between one and three seconds. `landing_zone`'s unmodified
delays are 60 seconds for the first poll and 300 for every one after, so a single end-to-end run
against production values would sit for minutes per attempt. [Testing](testing.md) covers the
runner, `poll_interval`, and driving a callback with `run_async`.

## Where to go next

| Page | Answers |
| --- | --- |
| [Concepts](concepts.md) | What replay is, and why the handler body re-enters after every wait |
| [Steps](steps.md) | What belongs in a step, retry strategies, and `StepSemantics` |
| [Fan-out](fan-out.md) | `map`, `parallel`, and how a `BatchResult` reports partial failure |
| [Testing](testing.md) | Driving a wait and a callback through `DurableFunctionTestRunner` |
| [SDK internals](sdk-internals.md) | Checkpoint mechanics, serialization, and the replay-aware logger |
| [Typing and tooling](typing-and-tooling.md) | Why basedpyright rejects a concrete context in `with_retry` |
| [Reference](reference.md) | Every config dataclass and its fields |
| [Overview](index.md) | The example, and what the whole site covers |

External references:

- [Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)
- [Durable execution developer guide](https://docs.aws.amazon.com/durable-execution/)
- [Durable execution best practices](https://docs.aws.amazon.com/durable-execution/patterns/best-practices/)
- [Lambda best practices](https://docs.aws.amazon.com/lambda/latest/dg/durable-best-practices.html)
- [aws/aws-durable-execution-sdk-python](https://github.com/aws/aws-durable-execution-sdk-python)
- [`waits.py`](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/waits.py)
  — `WaitStrategyConfig`, `create_wait_strategy`, `WaitForConditionDecision`
- [`config.py`](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/config.py)
  — `Duration`, `CallbackConfig`, `WaitForCallbackConfig`, `JitterStrategy`
- [`context.py`](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/packages/aws-durable-execution-sdk-python/src/aws_durable_execution_sdk_python/context.py)
  — `wait`, `wait_for_condition`, `wait_for_callback`, `create_callback`, `Callback.result`
