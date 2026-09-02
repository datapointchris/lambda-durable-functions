# CLAUDE.md

Guidance for Claude Code working in this repository.

Read the README first, and `docs/concepts.md` before touching any handler. The README's "Findings
worth knowing before writing any of this" list is the shortest path to not repeating an expensive
mistake.

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

## The four traps that cost the most

**`@durable_step` curries.** A decorated call returns a closure and runs nothing. Without
`context.step(...)` around it, the step never executes and nothing is checkpointed — and nothing
errors.

**A step costs two blocking checkpoints**, the success one blocking for a batch wait plus a network
round trip. Pure computation does not belong inside a step, and a log line never does.

**A dataclass will not checkpoint.** The default codec carries a closed set of types and raises
after the body has already run, so the work is done and then discarded. A custom `SerDes` is the
fix; `landing_zone` and `nested_payloads` both show it.

**`context.logger` only suppresses while replaying.** Past the last checkpointed operation the
status is `NEW`, so a log there repeats. A log inside a step body cannot repeat at all.

## The local runner is not the SDK

It drops `wait_for_condition` state — the SDK sends it and the harness discards it. A poll that
stops on accumulated state runs forever under test. Write the condition to decide from what it just
observed instead, the way `landing_zone` does.

`docs/testing.md` has the full assertion surface and the harness limits. Check it before concluding
the SDK is at fault.

## Tests take about two minutes, almost all real waiting

```bash
uv venv && uv pip install -e '.[dev]'
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m basedpyright
```

The released runner has no time skipping, so the wall clock is genuine and not a hang. Do not add
sleeps-shortening hacks to the examples to speed it up.

`basedpyright` is deliberate: mypy is silent on the missing-parameter diagnostic that matters here.
`docs/typing-and-tooling.md` explains which diagnostic and why.

## Scope boundary

Deployment, Terraform, versions and aliases, and the trigger wiring live in the published guide,
not in this repo. Do not add a Terraform directory or a deploy pipeline here — the split is
intentional and the other half is not duplicated.

`requires-python = ">=3.13"` matches the Lambda runtime and is not a preference.
