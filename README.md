# lambda-durable-functions

A worked, tested reference for AWS Lambda durable functions in Python. Runnable code, a passing
test suite, and the SDK internals that are not in the documentation.

Read the docs at [docs.ichrisbirch.com/lambda-durable-functions](https://docs.ichrisbirch.com/lambda-durable-functions/).

## The example

`landing_zone` is a landing-zone trigger. An upstream feed writes objects into an S3 prefix over
several minutes; starting one job per object would be wasteful and would read a partial drop. So it
waits until nothing new has landed for a quiet period, freezes a manifest of what arrived, and
starts a single ingest job for it.

That shape exercises every piece worth knowing: leader election so the drop's many triggers collapse
into one run, a `wait_for_condition` poll, a dataclass that will not checkpoint without a custom
`SerDes`, and two side effects that must not repeat on replay.

| Path | Holds |
|---|---|
| `src/landing_zone/handler.py` | The durable Lambda, in conventional shape — module-scope clients, `lambda_handler(event, context)` at module level |
| `src/landing_zone/logic.py` | The pure decisions, importable and testable with no SDK |
| `src/landing_zone/serdes.py` | Checkpointing a dataclass, which the default codec refuses |
| `tests/` | Three layers: pure logic, orchestration through the real runtime, and replay behaviour |
| `tests/test_harness_limits.py` | What the local test runner cannot do, asserted rather than written down |

| Document | Answers |
|---|---|
| [docs/sdk-internals.md](docs/sdk-internals.md) | How `@durable_step` works, what a step costs, why logs repeat, what serializes, how `wait_for_condition` threads state |
| [docs/testing.md](docs/testing.md) | How to test a durable Lambda without changing its shape |
| [docs/typing-and-tooling.md](docs/typing-and-tooling.md) | Why basedpyright reports a missing parameter, and why mypy does not |

## Running it

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest -q         # 37 passed, 1 xfailed
.venv/bin/python -m ruff check .
.venv/bin/python -m basedpyright
```

Around 29 seconds, almost all of it real waits — the released test runner has no time skipping.

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
