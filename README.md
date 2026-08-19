# lambda-durable-functions

A worked, tested reference for AWS Lambda durable functions in Python. Runnable code, a passing
test suite, and the SDK internals that are not in the documentation.

Read the docs at [docs.ichrisbirch.com/lambda-durable-functions](https://docs.ichrisbirch.com/lambda-durable-functions/).

## Six worked examples

Each is a complete, tested Lambda in the conventional shape — module-scope clients, module-scope
configuration, `lambda_handler(event, context)` at module level. None restructures the handler to be
testable; tests reach every seam with `monkeypatch.setattr`.

| Example | Demonstrates |
|---|---|
| `landing_zone` | `wait_for_condition` settle polling, a dataclass needing a custom `SerDes`, leader election |
| `order_saga` | `AT_MOST_ONCE_PER_RETRY` vs the default, `run_in_child_context` as a failure unit, compensation |
| `batch_scoring` | `context.map`, `max_concurrency`, `CompletionConfig` failure tolerance, `BatchResult` |
| `approval_gate` | `wait_for_callback`, callback timeout and heartbeat, approve / reject / lapse |
| `flaky_api_sync` | Custom `retry_strategy`, retryable versus permanent errors, `with_retry`, jittered backoff |
| `pipeline_chain` | `context.parallel`, `ParallelBranch`, `context.invoke`, nested operation history |

| Document | Answers |
|---|---|
| [docs/concepts.md](docs/concepts.md) | The execution model, replay, and the determinism rules |
| [docs/steps.md](docs/steps.md) | `context.step`, which spelling to write, replay-safe logging, `StepConfig` |
| [docs/waits.md](docs/waits.md) | `wait`, `wait_for_condition`, `wait_for_callback`, backoff and jitter |
| [docs/fan-out.md](docs/fan-out.md) | `map`, `parallel`, child contexts, `invoke`, partial failure |
| [docs/testing.md](docs/testing.md) | The local runner, the full assertion surface, callbacks, harness limits |
| [docs/sdk-internals.md](docs/sdk-internals.md) | What the shipped source does, read rather than documented |
| [docs/typing-and-tooling.md](docs/typing-and-tooling.md) | The missing-parameter diagnostic, and mypy's silence |
| [docs/reference.md](docs/reference.md) | Every method and config as a lookup table, plus annotated links |

## Running it

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest -q         # 147 passed, 1 xfailed
.venv/bin/python -m ruff check .
.venv/bin/python -m basedpyright
```

A little over two minutes, almost all of it real waits — the released test runner has no time skipping.

## Findings worth knowing before writing any of this

- **`@durable_step` curries.** A decorated call returns a closure and runs nothing. Without
  `context.step(...)` around it, the step never executes and nothing is checkpointed.
- **A step costs a blocking checkpoint.** Two per step, and the success one blocks the handler for a
  ~100ms batch wait plus a network round trip. Pure computation does not belong in one, and a log
  line never does.
- **Logs repeat because `context.logger` only suppresses while the context is replaying.** Past the
  last checkpointed operation the status is `NEW`. A log inside a step body cannot repeat at all.
- **A dataclass will not checkpoint.** The default codec carries a closed set of types and raises
  `SerDesError: Unsupported type` after the body has already run.
- **The local test runner drops `wait_for_condition` state.** The SDK sends it; the harness
  discards it. A poll that stops on accumulated state runs forever in tests — so this one decides
  from the world it just observed instead.
- **A conventional handler is fully testable.** `monkeypatch.setattr` on module attributes reaches
  every client and constant, so nothing about the handler changes to make it testable.

## Scope

Deployment, Terraform, versions and aliases, the S3 trigger wiring and the `durable_config` replace
behaviour are in the published guide at
[docs.ichrisbirch.com/aws/lambda-durable-functions](https://docs.ichrisbirch.com/aws/lambda-durable-functions/).
That half is not duplicated here.

`requires-python = ">=3.13"` is the Lambda runtime.
