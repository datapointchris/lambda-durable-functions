# Lambda Durable Functions

A worked, tested reference for AWS Lambda durable functions in Python — what the SDK actually does,
read from the shipped source rather than the documentation, with runnable code and a passing test
suite behind every claim.

Source: [datapointchris/lambda-durable-functions](https://github.com/datapointchris/lambda-durable-functions).

## Start here

| Page | Answers |
|---|---|
| [SDK Internals](sdk-internals.md) | How `@durable_step` works, what a step costs, why logs repeat, what serializes, how `wait_for_condition` threads state |
| [Testing](testing.md) | How to test a durable Lambda without changing its shape |
| [Typing and Tooling](typing-and-tooling.md) | Why basedpyright reports a missing parameter, and why mypy stays silent about the same cause |

Deployment, Terraform, versions and aliases, and the S3 trigger wiring are in the separate
[Lambda Durable Functions deployment guide](https://docs.ichrisbirch.com/aws/lambda-durable-functions/). This site covers
the SDK and the code; that one covers getting it into an account.

## The example

`landing_zone` is a landing-zone trigger. An upstream feed writes objects into an S3 prefix over
several minutes. Starting one job per object would be wasteful and would read a partial drop, so it
waits until nothing new has landed for a quiet period, freezes a manifest of what arrived, and
starts a single ingest job for it.

```text
S3 object lands ──► one invocation per object
                         │
                    leader election ── not oldest? ──► exit
                         │
                    settle poll ── quiet for 5 min? ──┐
                         ▲                            │ no, wait and re-check
                         └────────────────────────────┘
                         │ yes
                    freeze manifest   ← dataclass, needs a custom SerDes
                         │
                    write manifest + start ingest job
```

Four things fall out of that shape, and each is a thing worth knowing:

- **Leader election**, because a drop of forty files triggers forty invocations that must collapse
  into one run. Reserved concurrency cannot express it — it throttles, so the extras retry into a
  DLQ instead of being ignored.
- **A `wait_for_condition` poll**, whose stop condition is computed from the listing's own
  timestamps rather than from accumulated state.
- **A dataclass return**, which the default checkpoint codec refuses until you supply a `SerDes`.
- **Two side effects that must not repeat**, which is what the replay tests actually assert.

## Findings

- `@durable_step` curries. A decorated call returns a closure and runs nothing without
  `context.step(...)` around it.
- A step costs two checkpoints and the success one blocks. Pure computation does not belong in one,
  and a log line never does.
- `context.logger` suppresses only while the context is replaying. Past the last checkpointed
  operation it prints on every pass.
- The default codec carries a closed set of types, and a dataclass is not among them.
- The released local test runner drops `wait_for_condition` polling state, so a poll that stops on
  accumulated state never terminates in tests.
- A conventional handler is fully testable. Nothing about its shape has to change.
