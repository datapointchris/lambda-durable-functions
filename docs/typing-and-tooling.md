# Type checking and linting durable handlers

Measured against SDK 1.7.0, with basedpyright, mypy and ruff.

## `Argument missing for parameter` means the SDK is not installed where the checker looks

The symptom is a call-arity error on a correctly written step:

```bash
reportCallIssue: Argument missing for parameter "timestamp"
```

The SDK is not at fault. It types `durable_step` with `Concatenate` and `ParamSpec` exactly as it
should, and it ships a `py.typed` marker. The chain is three steps, and only the last one is
visible:

```bash
Import "aws_durable_execution_sdk_python" could not be resolved   [reportMissingImports]
        │
        ▼  durable_step is Unknown
Untyped function decorator obscures type of function; ignoring    [reportUntypedFunctionDecorator]
        │
        ▼  your function keeps its declared two-parameter signature
Argument missing for parameter "timestamp"                        [reportCallIssue]
```

Reproduced, then cleared to `errors=0 warnings=0` on the same file by putting the SDK on the search
path. Nothing else changed.

**Why it goes missing.** The Lambda Python runtime provides the SDK, so a durable function runs
with nothing in its deployment package. Nothing puts it in the local environment. AWS also
recommends bundling it for production, because the runtime-provided copy moves under you across
managed-runtime patches. One action fixes both:

```bash
uv add aws-durable-execution-sdk-python
```

## mypy stays silent, so the two checkers disagree from one cause

The shared default sets `ignore_missing_imports = true`, which turns the unresolved import into
`Any` and suppresses the whole chain. Measured on the same file that basedpyright rejected:

```bash
Success: no issues found in 1 source file
```

That is worth knowing whenever a dependency is provided by the runtime rather than by the package,
because the two checkers then disagree and the quiet one is the one usually trusted.

It does not arise here. The SDK is a declared dependency, so `uv sync` installs it and `uv run`
syncs before it runs anything — which is how both the pre-commit hook and CI invoke mypy. Reaching
the silent case means running `mypy` by hand against an environment that never synced.

## No stub package is needed

An earlier reading of this concluded the SDK typed its decorator as identity-preserving
(`def deco(f: F) -> F`), which is the usual reason a signature-changing decorator confuses a
checker. That is not what it does. Writing a `-stubs` package for it would fix nothing and would
shadow a correctly typed dependency.

## ruff's ARG rules fire on the context parameter, if you enable them

`ARG` is not selected here. Enabling it produces two codes, both legitimate, neither needing a
suppression:

| Code | Fires on | Fix |
| --- | --- | --- |
| `ARG001` | `def build_manifest(step_context, ...)` where the body ignores it | prefix `_step_context` |
| `ARG005` | `lambda step_context: do_thing()` | use `lambda _:` |

Ruff's `dummy-variable-rgx` covers the underscore form. Measured: both disappear, and basedpyright
stays at zero. The decorator passes the context positionally, so the name is free.

Ruff does no call-arity analysis, so it is never the source of a missing-parameter complaint.

## What the checkers are configured to

```toml
[tool.pyright]              # named `pyright` so basedpyright and Pylance both read it
typeCheckingMode = "standard"

[tool.ruff.lint]
select = ["E", "F", "UP", "B", "SIM", "I"]
```

`standard` rather than basedpyright's `recommended` default, matching the shared default. The
`select` list is the shared default too — `PTH` would govern nothing, because nothing here touches
the filesystem.
