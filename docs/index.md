# Lambda Durable Functions

A worked reference for AWS Lambda durable functions in Python. Every claim here is backed by code in
this repository that runs, and by the shipped SDK source rather than the documentation.

Measured against `aws-durable-execution-sdk-python` **1.7.0** and
`aws-durable-execution-sdk-python-testing` **1.2.1**.

Source: [datapointchris/lambda-durable-functions](https://github.com/datapointchris/lambda-durable-functions).

## Start here

| Page | Answers |
| --- | --- |
| [Concepts](concepts.md) | What a durable function is, why the handler body re-runs, and the determinism rules |
| [Steps](steps.md) | `context.step`, the three spellings and which to write, replay-safe logging, what a step costs, `StepConfig` in full |
| [Waits and Suspension](waits.md) | `wait`, `wait_for_condition`, `wait_for_callback`, backoff and jitter |
| [Fan-out and Composition](fan-out.md) | `map`, `parallel`, child contexts, `invoke`, and partial-failure tolerance |
| [Serialization](serialization.md) | Nested dataclasses through a checkpoint, and the same object in S3 without writing the conversion twice |
| [Testing](testing.md) | The local runner, the full assertion surface, driving callbacks, and the harness limits |
| [SDK Internals](sdk-internals.md) | What the shipped source actually does, read rather than documented |
| [Typing and Tooling](typing-and-tooling.md) | Why basedpyright reports a missing parameter and mypy stays silent |
| [Reference](reference.md) | Every method, config and exception as a lookup table, plus annotated external links |

Deployment, Terraform, versions and aliases, and the S3 trigger wiring are in the separate
[Lambda Durable Functions deployment guide](https://docs.ichrisbirch.com/aws/lambda-durable-functions/).
This site covers the SDK and the code; that one covers getting it into an account.

## Seven worked examples

Each is a complete, tested Lambda in the conventional shape — module-scope clients, module-scope
configuration, `lambda_handler(event, context)` at module level. None of them restructures the
handler to be testable.

| Example | Demonstrates |
| --- | --- |
| `landing_zone` | `wait_for_condition` settle polling, a dataclass that needs a custom `SerDes`, leader election across many triggers |
| `order_saga` | `AT_MOST_ONCE_PER_RETRY` vs the default, `run_in_child_context` as a failure unit, compensation |
| `batch_scoring` | `context.map`, `max_concurrency`, `CompletionConfig` failure tolerance, reading `BatchResult` |
| `approval_gate` | `wait_for_callback`, callback timeout and heartbeat, approve / reject / lapse |
| `flaky_api_sync` | Custom `retry_strategy`, retryable versus permanent errors, `with_retry`, backoff with jitter |
| `pipeline_chain` | `context.parallel`, `ParallelBranch`, `context.invoke`, nested operation history |
| `nested_payloads` | A nested `StrEnum` dataclass through a checkpoint and through S3 — hand-written `to_dict` and `from_dict` in `minimal.py`, the general codec beside it, and a `cattrs` variant |

## The shortest useful summary

Each rule links to the page that explains it.

- **Wrap every `@durable_step` call in `context.step(...)`**, or the step never runs. See
  [Steps](steps.md).
- **Write a step as a nested `def`, not a lambda**, so it can both do the work and log. See
  [Steps](steps.md).
- **Log inside a step body**, where a log cannot repeat. See [Steps](steps.md).
- **Keep pure computation and log lines out of a step**, because every step pays for checkpoints.
  See [Steps](steps.md).
- **Return a dict from a step, or give its dataclass a custom `SerDes`.** See
  [Serialization](serialization.md).
- **Assert on call counts**, because a side effect that escaped its step counts two. See
  [Testing](testing.md).

## Running it

The README holds the commands, with what each one does.
[Install](https://github.com/datapointchris/lambda-durable-functions#install) sets up the locked
environment.
[Starting from scratch](https://github.com/datapointchris/lambda-durable-functions#starting-from-scratch)
runs the suite and the checks, then builds and deploys an example.
