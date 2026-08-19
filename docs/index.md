# Lambda Durable Functions

A worked reference for AWS Lambda durable functions in Python. Every claim here is backed by code in
this repository that runs, and by the shipped SDK source rather than the documentation.

Measured against `aws-durable-execution-sdk-python` **1.7.0** and
`aws-durable-execution-sdk-python-testing` **1.2.1**, on 2026-08-19.

Source: [datapointchris/lambda-durable-functions](https://github.com/datapointchris/lambda-durable-functions).

## Start here

| Page | Answers |
|---|---|
| [Concepts](concepts.md) | What a durable function is, why the handler body re-runs, and the determinism rules |
| [Steps](steps.md) | `context.step`, the three spellings and which to write, replay-safe logging, what a step costs, `StepConfig` in full |
| [Waits and Suspension](waits.md) | `wait`, `wait_for_condition`, `wait_for_callback`, backoff and jitter |
| [Fan-out and Composition](fan-out.md) | `map`, `parallel`, child contexts, `invoke`, and partial-failure tolerance |
| [Testing](testing.md) | The local runner, the full assertion surface, driving callbacks, and the harness limits |
| [SDK Internals](sdk-internals.md) | What the shipped source actually does, read rather than documented |
| [Typing and Tooling](typing-and-tooling.md) | Why basedpyright reports a missing parameter and mypy stays silent |
| [Reference](reference.md) | Every method, config and exception as a lookup table, plus annotated external links |

Deployment, Terraform, versions and aliases, and the S3 trigger wiring are in the separate
[Lambda Durable Functions deployment guide](https://docs.ichrisbirch.com/aws/lambda-durable-functions/).
This site covers the SDK and the code; that one covers getting it into an account.

## Six worked examples

Each is a complete, tested Lambda in the conventional shape — module-scope clients, module-scope
configuration, `lambda_handler(event, context)` at module level. None of them restructures the
handler to be testable.

| Example | Demonstrates |
|---|---|
| `landing_zone` | `wait_for_condition` settle polling, a dataclass that needs a custom `SerDes`, leader election across many triggers |
| `order_saga` | `AT_MOST_ONCE_PER_RETRY` vs the default, `run_in_child_context` as a failure unit, compensation |
| `batch_scoring` | `context.map`, `max_concurrency`, `CompletionConfig` failure tolerance, reading `BatchResult` |
| `approval_gate` | `wait_for_callback`, callback timeout and heartbeat, approve / reject / lapse |
| `flaky_api_sync` | Custom `retry_strategy`, retryable versus permanent errors, `with_retry`, backoff with jitter |
| `pipeline_chain` | `context.parallel`, `ParallelBranch`, `context.invoke`, nested operation history |

## The shortest useful summary

- **`@durable_step` curries.** A decorated call returns a closure and runs nothing. Without
  `context.step(...)` around it, the step never executes and nothing is checkpointed.
- **Write a nested `def`, not a lambda.** A lambda cannot hold two statements, so it cannot both do
  the work and log. The `def` receives `step_context` as its only parameter. See [Steps](steps.md).
- **A log inside a step body cannot repeat.** A succeeded checkpoint short-circuits the body.
  `context.logger` in the handler is different — it only suppresses while the context is replaying.
- **A step costs two checkpoints**, one of them blocking, plus durable state for its return value.
  Pure computation does not belong in one and a log line never does.
- **The default codec carries a closed set of types.** A dataclass raises `SerDesError` at
  checkpoint time, after the body has already run.
- **Call counts are the assertion that matters.** The handler body replays; step bodies do not. A
  side effect that escaped its step shows up as a count of two. See [Testing](testing.md).

## Running it

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest -q         # 147 passed, 1 xfailed
.venv/bin/python -m ruff check .
.venv/bin/python -m basedpyright
```

The suite takes a little over two minutes. Almost all of it is real waits — the released test runner
has no time skipping.
