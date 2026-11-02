# Contributing to deephaven-massive

Thank you for helping. This page gets a development checkout running and points to the rules
every change follows. [`AGENTS.md`](AGENTS.md) holds those rules in full, for people and coding
agents alike; read it before your first pull request.

## Set up

The project is a Python library managed with [uv](https://docs.astral.sh/uv/). The test suite
needs JDK 17 or later. Run everything from the repository root:

```bash
uv sync --upgrade                          # create or update .venv with the newest versions
uv run pre-commit install --hook-type pre-commit --hook-type pre-push   # once per clone
./scripts/check.sh                         # full gate: lint, format, types, tests + coverage
./scripts/check.sh full --all              # the whole suite, even for a docs-only change
./scripts/check.sh fast                    # lint and types only; fixes what ruff can fix
```

Always go through `uv run`: bare `python`, `pytest`, `ruff`, and `mypy` use the wrong
environment.

## Propose a change

Every change lands through a pull request; only the owner merges. Branch names, commit and
title wording, and the pull request description follow [`AGENTS.md`](AGENTS.md) and
[`docs/dev/pull-requests.md`](docs/dev/pull-requests.md). The full check must pass, with 100%
coverage of statements and branches.

A release is one manual run of the Release workflow on `main`;
[`docs/dev/releasing.md`](docs/dev/releasing.md) gives the steps.

## Documentation

User documentation lives in one place, [`README.md`](README.md); `tests/test_docs.py` runs
every Python block in it against the fake Massive servers. The dataset reference under
[`docs/datasets/`](docs/datasets/README.md) is generated: never edit it by hand, regenerate it
with `uv run python scripts/render_reference.py`. Docstrings cite README sections as
`README.md#<anchor>` and dataset pages as `docs/datasets/<dataset>.md`, and the same test checks
that every cited anchor and page exists.

When Massive changes its documentation, [`docs/dev/spec-refresh.md`](docs/dev/spec-refresh.md)
explains how the generated schemas and the reference follow.

## Live integration tests

The tests in `tests/**/test_*_integration.py` call Massive itself with the owner's keys, so
neither `./scripts/check.sh` nor CI ever runs them. [`docs/live-tests.md`](docs/live-tests.md)
says how to run them and what each plan can verify.
