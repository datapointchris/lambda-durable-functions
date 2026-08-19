# Testing a durable Lambda

The handler keeps the shape any Lambda reviewer expects. Module-scope clients, module-scope
configuration, `lambda_handler(event, context)` at module level. No factory, no dependency
injection container, no wrapper. Testability comes from `monkeypatch`, not from restructuring.

**Measured 2026-08-18** against SDK 1.7.0 and testing 1.2.1.

## Three layers, and only the innermost needs the SDK

```python
# src/landing_zone/                                       tested by
# ──────────────────────────────────────────────────────────────────
logic.py    is_quiet · build_manifest                     # plain pytest
            next_poll_delay_seconds
#      ▲ no SDK import, no clients, no context
#      │ called by
handler.py  list_landed_objects · write_manifest          # fake client
            start_ingest_job · is_leader
#      ▲ module-scope client, swapped with monkeypatch
#      │ called by
handler.py  lambda_handler                                # TestRunner
              def settle_check(...) · freeze_manifest     # glue only
              def publish_and_start(...)
```

Most assertions belong in layer one. `tests/test_logic.py` runs in 0.01s with no fakes and no
runtime.

## The nested `def` is deliberately not unit tested

A closure inside the handler cannot be imported, so it cannot be reached directly. That is
acceptable exactly while it holds no logic — it calls a tested function and logs. A branch appearing
inside one is the signal to lift that branch into `logic.py`, not to make the closure reachable.

## `monkeypatch` reaches every seam without changing the handler

The handler builds its clients and reads its configuration at import time, which is the
conventional Lambda shape and also what usually blocks testing. Both are satisfiable:

- environment, in `conftest.py` at collection time, before the module is imported
- clients and durations, per test, with `monkeypatch.setattr`

```python
@pytest.fixture
def clients(monkeypatch):
    s3, glue, lam = FakeS3(), FakeGlue(), FakeLambda()
    monkeypatch.setattr(handler_module, 's3_client', s3)
    monkeypatch.setattr(handler_module, 'glue_client', glue)
    monkeypatch.setattr(handler_module, 'lambda_client', lam)
    monkeypatch.setattr(handler_module.time, 'time', lambda: EPOCH.timestamp())
    monkeypatch.setattr(handler_module, 'POLL_DELAYS_SECONDS', (1,))
    return s3, glue, lam
```

Freezing `time.time` is what makes settle detection deterministic. The fixture pins the clock and
`s3_page()` builds objects at a stated age relative to it, so "landed ten minutes ago" is a fact of
the fixture rather than a race.

This works because the handler body resolves those names as module globals at call time. Nothing
about the handler is aware of it.

## `DurableFunctionTestRunner` runs the real runtime in-process

```python
with DurableFunctionTestRunner(handler_module.lambda_handler) as runner:
    result = runner.run(input=json.dumps(event), timeout=30)
```

`result` carries `status`, `operations`, `result` and `error`. `operations` is the orchestration
assertion — which durable operations ran, named, in order:

```text
['leader', 'settle', 'freeze_manifest', 'publish_and_start']
```

`result.result` is typed `OperationPayload | None` and is `None` on a failed execution, so narrow
it before `json.loads`. `tests/test_handler.py` has a `payload()` helper that asserts and reports
`result.error` when it is missing.

## The runner suspends and replays, which is the test worth having

The suite asserts it directly. `list_landed_objects` is called exactly twice across a full run —
once by the settle check, once by `freeze_manifest` — while the handler body re-enters on every
resume:

```python
assert s3.paginate_calls == 2      # a third means a checkpointed step re-ran
assert len(glue.job_runs) == 1     # a second means the job start escaped its step
assert len(s3.put_objects) == 1
assert lam.list_calls == 1
```

`test_the_handler_body_really_does_re_enter` guards those: without a replay actually happening they
would pass for the wrong reason.

That is the failure mode unique to durable functions. A side effect sitting in handler code rather
than inside a step shows up as a count of 2.

Assert call counts on every fake, not just return values. It is the only thing that catches a step
you forgot to wrap. See `tests/test_replay.py`.

## Two limits of the harness, pinned as executable tests

Both are properties of testing 1.2.1 rather than of the SDK, and both shape how the suite is
written. `tests/test_harness_limits.py` asserts them so they flip green when fixed.

### `wait_for_condition` polling state is dropped on every retry

The check always receives `initial_state`. Measured with a counter that should reach 3:

```yaml
check saw    : [{'n': 0}, {'n': 0}, {'n': 0}, ...]   # never advances
strategy saw : [({'n': 1}, 1), ({'n': 1}, 2), ...]   # attempt advances correctly
```

The SDK is not at fault. It sends the serialized state as `OperationUpdate.payload` on RETRY:

```python
retry_operation = OperationUpdate.create_wait_for_condition_retry(
    identifier=self.operation_identifier,
    payload=serialized_state,
    next_attempt_delay_seconds=delay_seconds,
)
```

The harness's in-memory step processor never reads `update.payload` on the RETRY branch. It copies
`current_op.step_details.result` forward, which is `None` on the first retry and stays `None`:

```python
new_step_details = StepDetails(
    attempt=current_attempt + 1,
    next_attempt_timestamp=next_attempt_time,
    result=(current_op.step_details.result if current_op and current_op.step_details else None),
)
```

**Consequence:** any polling loop whose stop condition depends on accumulated state never
terminates locally.

`landing_zone` sidesteps this by design rather than working around it. Settle detection reads the
newest object's own timestamp and compares it to now, so an attempt needs no memory of the one
before it — `is_quiet(newest_epoch, now_epoch, quiet_seconds)`. Comparing successive listings would
have been the obvious implementation and would have depended on exactly the state the harness
drops. It is also the more honest definition of settled.

The rule generalises past this bug: a poll whose decision comes from the world it just observed is
simpler to reason about than one carrying a running tally, and it survives a lost checkpoint.

This is unverified against real Lambda. No AWS account exists for this workspace yet.

### A modeled wait costs real wall-clock time

```text
modeled 10s wait took 10.31s wall clock -> InvocationStatus.SUCCEEDED
```

`SkipClock` and a `skip_time` flag exist on `main` and are not in 1.2.1. So handler durations stay
module constants that tests shrink to seconds. Production values would make one end-to-end run sit
for 21 minutes.

## Running it

```bash
cd ~/code/aws/durable_functions
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest -q            # 34 passed, 1 xfailed in ~28s
.venv/bin/python -m ruff check .
.venv/bin/python -m basedpyright
```

The suite takes ~28 seconds, almost all of it real waits.
