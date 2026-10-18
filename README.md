# lambda-durable-functions

A worked, tested reference for AWS Lambda durable functions in Python. Each example is a complete
Lambda with a passing test suite. The pages in [docs/](docs/index.md) carry what the SDK's shipped
source does where its documentation says nothing. Every claim in them is backed by code here that
runs.

It is read, never imported. No example imports another. Nothing here is published as a package.
Code that needs a durable function elsewhere is written by reading this one. Provisioning is the
other boundary. This repo builds and updates a function's code, and creates no function, alias or
trigger.

## Install

```bash
git clone https://github.com/datapointchris/lambda-durable-functions
cd lambda-durable-functions
uv sync --all-extras   # the SDK, its local test runner, cattrs, pytest, ruff, basedpyright and mypy, as uv.lock pins them
```

uv is the one prerequisite. `uv.lock` pins the SDK and the test runner every page was measured
against. `uv tree --frozen --depth 1` prints those pins. An install that skips the lock resolves the
newest SDK instead. The examples are written against the pinned one.

`requires-python = ">=3.13"` is the Lambda runtime's version, not a preference.

## Starting from scratch

```bash
uv run pytest -q                                          # every example end to end in the local runner, about two minutes
uv run ruff check .                                       # the lint CI runs, at the ruff version CI pins
uv run basedpyright                                       # zero errors, or the SDK is not installed where it looks

uv export --frozen --no-dev --no-emit-project \
  | uv pip install --target dist/lambda -r -              # the SDK and boto3, with their dependencies, as uv.lock pins them
cp -r src/landing_zone dist/lambda/                       # the example at the artifact's root, where its imports look
uv run python -m zipfile -c function.zip dist/lambda/*    # function.zip, the artifact deploy.sh reads by default

FUNCTION_NAME=landing-zone-trigger scripts/deploy.sh release  # publish a version and move the alias onto it

aws s3 cp drop.csv "s3://$LANDING_BUCKET/landing/"            # an object under the function's landing prefix fires the trigger
aws lambda list-durable-executions-by-function \
  --function-name landing-zone-trigger --statuses RUNNING     # the execution it started, RUNNING while the drop settles
```

The suite's two minutes are real waiting. The released test runner has no time skipping, so a slow
run is not a hang. One test is a strict expected failure. It pins the runner dropping
`wait_for_condition` state. It fails the suite once a locked runner fixes that.

basedpyright reports `Argument missing for parameter` on a correctly written step when the SDK is
not installed where it looks. mypy stays silent in the same case.
[docs/typing-and-tooling.md](docs/typing-and-tooling.md) traces the chain.

The artifact bundles the SDK rather than relying on the copy the Lambda runtime provides, because
that copy moves across managed-runtime patches. The handler string is
`landing_zone.handler.lambda_handler`. Its imports are absolute, as in
`from landing_zone.logic import is_quiet`, which is why the package sits at the zip's root. Any
other example packages the same way from its own directory under `src/`.

`deploy.sh` updates a function that already exists. It refuses before calling AWS when
`function.zip` is missing. What the function has to be before that first `release` is under
`scripts/deploy.sh` below. The deploy and the last two lines run the AWS CLI under whatever
credentials the shell holds, with `LANDING_BUCKET` set to the function's own bucket.

The last command is the proof. A durable function runs durably only when invoked through a
qualified ARN. A trigger on the bare function still invokes it, but no execution appears. An empty
list while the function is being invoked means the trigger targets the bare function rather than
the alias.

## Each example is a complete Lambda, tested without restructuring it

| Example | Demonstrates |
| --- | --- |
| `landing_zone` | `wait_for_condition` settle polling, a dataclass needing a custom `SerDes`, leader election |
| `order_saga` | `AT_MOST_ONCE_PER_RETRY` vs the default, `run_in_child_context` as a failure unit, compensation |
| `batch_scoring` | `context.map`, `max_concurrency`, `CompletionConfig` failure tolerance, `BatchResult` |
| `approval_gate` | `wait_for_callback`, callback timeout and heartbeat, approve / reject / lapse |
| `flaky_api_sync` | Custom `retry_strategy`, retryable versus permanent errors, `with_retry`, jittered backoff |
| `pipeline_chain` | `context.parallel`, `ParallelBranch`, `context.invoke`, nested operation history |
| `nested_payloads` | A nested `StrEnum` dataclass through a checkpoint and through S3 — hand-written `to_dict` and `from_dict` in `minimal.py`, the general codec beside it, and a `cattrs` variant |

Every handler keeps the conventional shape: module-scope clients, module-scope configuration, and
`lambda_handler(event, context)` at module level. The tests reach every seam with
`monkeypatch.setattr` on module attributes. No handler takes a factory, a class or an injected
client to be testable. That is a claim the examples exist to demonstrate.

Every example but `nested_payloads` keeps its decisions in a pure `logic.py`, tested with plain
pytest and no SDK. `handler.py` holds the clients and the durable glue, tested through fakes and the
local runner.

No two examples share a helper. The duplication between them is deliberate, because it lets each be
read alone.

## The expensive mistakes fail late or not at all

- **`@durable_step` curries.** A decorated call returns a closure and runs nothing. Without
  `context.step(...)` around it, the step never executes, nothing is checkpointed, and nothing
  errors.
- **A step costs two checkpoints, and the success one blocks.** The handler waits roughly 100ms for
  the checkpoint batcher, then pays a network round trip. Pure computation does not belong in a
  step. A log line never does.
- **`context.logger` suppresses only while replaying.** Past the last checkpointed operation the
  status is `NEW`, so a log there repeats. A log inside a step body cannot repeat at all.
- **A dataclass will not checkpoint.** The default codec carries a closed set of types. It raises
  `SerDesError: Unsupported type` after the step body has already run, so the work is done and then
  discarded. A custom `SerDes` is the fix. `landing_zone` and `nested_payloads` both show one.
- **The local test runner drops `wait_for_condition` state.** The SDK sends it. The runner discards
  it, so a poll that stops on accumulated state runs forever under test. `landing_zone` decides from
  what each check just observed instead. `tests/test_harness_limits.py` pins the limit.

## `docs/` — every claim backed by code here that runs, and by the SDK's shipped source

```bash
uv run --with mkdocs --with mkdocs-material mkdocs serve   # the pages rendered at http://127.0.0.1:8000
```

| Document | Answers |
| --- | --- |
| [docs/concepts.md](docs/concepts.md) | The execution model, replay, and the determinism rules |
| [docs/steps.md](docs/steps.md) | `context.step`, which spelling to write, replay-safe logging, `StepConfig` |
| [docs/waits.md](docs/waits.md) | `wait`, `wait_for_condition`, `wait_for_callback`, backoff and jitter |
| [docs/fan-out.md](docs/fan-out.md) | `map`, `parallel`, child contexts, `invoke`, partial failure |
| [docs/serialization.md](docs/serialization.md) | Nested dataclasses through a checkpoint, and the same object in S3 |
| [docs/testing.md](docs/testing.md) | The local runner, the full assertion surface, callbacks, harness limits |
| [docs/sdk-internals.md](docs/sdk-internals.md) | What the shipped source does, read rather than documented |
| [docs/typing-and-tooling.md](docs/typing-and-tooling.md) | The missing-parameter diagnostic, and mypy's silence |
| [docs/reference.md](docs/reference.md) | Every method and config as a lookup table, plus annotated links |

The pages use admonitions that GitHub shows as plain text, which is the reason to serve them. Every
measurement in them is from the local runner. Before concluding the SDK is at fault, check the
harness limits in [docs/testing.md](docs/testing.md).

## `scripts/deploy.sh` — updates a function that already exists, and creates nothing

```bash
FUNCTION_NAME=landing-zone-trigger scripts/deploy.sh draft    # $LATEST only; the alias, and the trigger behind it, stay put
FUNCTION_NAME=landing-zone-trigger scripts/deploy.sh release  # publish a version and move the alias onto it
scripts/deploy.sh                                             # usage, and exit 1
```

A durable function is invoked through a qualified ARN. Its trigger therefore follows an alias, which
points at an immutable version. Updating code alone writes `$LATEST` and leaves the trigger running
the old version. `release` is the update the trigger sees. `draft` iterates without moving the
alias.

`ARTIFACT` defaults to `function.zip` in the working directory. `ALIAS_NAME` defaults to `live`.
`FUNCTION_NAME` has a default that names no example here. Set it on every call.

The script is for iteration, and infrastructure as code owns the release. Its usage text names
Terraform, publishing through `publish = true` and `source_code_hash`. The next apply reconciles the
drift a direct deploy leaves.

The function has to exist before the first `release`. What `landing_zone` reads and calls decides
its shape:

- the Python 3.13 runtime, with handler `landing_zone.handler.lambda_handler`
- durable execution enabled, with the `DurableConfig` fields and bounds in
  [docs/concepts.md](docs/concepts.md)
- a published version, and an alias pointing at it, named `live` unless `ALIAS_NAME` says otherwise
- an S3 trigger on the landing prefix that targets the alias, never the bare function
- `LANDING_BUCKET` and `INGEST_JOB_NAME` set, with `LANDING_PREFIX` and `MANIFEST_PREFIX` defaulting
  to `landing/` and `manifests/`
- a Glue job named by `INGEST_JOB_NAME`
- a role allowing every call the code makes: S3 `ListObjectsV2` and `PutObject`, Glue `StartJobRun`,
  Lambda `ListDurableExecutionsByFunction`, and the SDK's own `CheckpointDurableExecution` and
  `GetDurableExecutionState`

The leader election reads `AWS_LAMBDA_FUNCTION_NAME` and `AWS_LAMBDA_FUNCTION_VERSION`, which Lambda
sets itself. Every other example reads its configuration at the top of its `handler.py`, so
`rg -n os.environ src/<example>/handler.py` lists what it needs.

None of the examples is verified against a deployed function.
