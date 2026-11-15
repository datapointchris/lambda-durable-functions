# lambda-durable-functions

Read the README first, and `docs/concepts.md` before touching any handler. The README's list of
expensive mistakes names the five that fail late or raise nothing.

## This is a reference, read as a guide

Seven worked examples, each a complete tested Lambda. Nothing here is imported by anything else,
and it imports nothing of its own. Code that needs a durable function elsewhere is written by
reading this, not by depending on it.

That shapes every change: an example exists to demonstrate one mechanism clearly. Do not merge two
examples, do not add a shared helper module across them, and do not factor out duplication between
them. The duplication is what lets each be read on its own.

## Handlers stay in the conventional shape

Module-scope clients, module-scope configuration, `lambda_handler(event, context)` at module level.
No example restructures its handler to be testable, and that is a demonstrated claim rather than a
convention — tests reach every seam with `monkeypatch.setattr` on module attributes.

A refactor that introduces a factory, a class, or dependency injection "for testability" breaks the
point being made. If something seems untestable, the reach is `monkeypatch.setattr`.

## The expensive mistakes, as rules

The README's "The expensive mistakes fail late or not at all" explains each one. Every change here
keeps to them:

- Wrap every `@durable_step` call in `context.step(...)`, or the step silently never runs.
- Keep pure computation and log lines out of a step, because each step pays a blocking checkpoint.
- Give a step that returns a dataclass a custom `SerDes`, as `landing_zone` and `nested_payloads` do.
- Log inside a step body rather than through `context.logger` past the last checkpoint.

## The local runner is not the SDK

It drops `wait_for_condition` state — the SDK sends it and the harness discards it. A poll that
stops on accumulated state runs forever under test. Write the condition to decide from what it just
observed instead, the way `landing_zone` does.

`docs/testing.md` has the full assertion surface and the harness limits. Check it before concluding
the SDK is at fault.

## Tests take about two minutes, almost all real waiting

```bash
uv sync --all-extras
uv run pytest -q
uv run ruff check .
uv run basedpyright
```

`uv sync` installs the SDK `uv.lock` pins. An install that skips the lock resolves the newest SDK,
which the examples are not written against.

The released runner has no time skipping, so the wall clock is genuine and not a hang. Do not add
sleeps-shortening hacks to the examples to speed it up.

An `Argument missing for parameter` error from basedpyright on a correct step means the SDK is
missing from the environment it checked, not that the step is wrong. mypy reports nothing in the
same case, because the shared `ignore_missing_imports` turns the import into `Any`.
`docs/typing-and-tooling.md` traces the chain.

## Scope boundary

Deployment, Terraform, versions and aliases, and the trigger wiring live in the published guide,
not in this repo. Do not add a Terraform directory or a deploy pipeline here — the split is
intentional and the other half is not duplicated.

`requires-python = ">=3.13"` matches the Lambda runtime and is not a preference.
