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
./scripts/check.sh                         # full gate: lint, format, types; tests + coverage only if the change can affect them
./scripts/check.sh full --all              # the whole suite, even for a docs-only change
./scripts/check.sh fast                    # lint and types only; fixes what ruff can fix
uv build                                   # sdist and wheel
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

User documentation has three parts:

- [`README.md`](README.md): the complete landing page, which takes a new user from "what is
  this?" through install to everyday use and the main recipes.
- [`docs/troubleshooting.md`](docs/troubleshooting.md): every error message and symptom with
  its fix, the diagnostics tables, the debug log, and how to report a bug.
- [`docs/datasets/`](docs/datasets/README.md): the generated dataset reference, one page per
  dataset with its call, arguments, plans, and columns. Never edit it by hand; regenerate it
  with `uv run python scripts/render_reference.py`. The rules of the arguments many datasets
  share come from `src/deephaven_massive/_datasets/argument_rules.py`, and each dataset's own
  line for an argument from its method's docstring.

[`docs/dev/user-docs.md`](docs/dev/user-docs.md) is the standard for every user page: the project
section (readers, owner requirements, install decision, page map), the rules, the AI tells, the
section models, the terminology, and the rubric and pass bar.
`tests/test_docs.py` runs every Python block on the user pages against the fake Massive servers.
Docstrings cite a page or section by its full GitHub URL, so the link opens from an IDE or
the console: `https://github.com/chipkent/deephaven-massive/blob/main/` followed by the page,
such as `docs/datasets/<dataset>.md` or `README.md#<anchor>`. The same test checks that every
citation is a URL and that every cited page and anchor exists.

When Massive changes its documentation, [`docs/dev/spec-refresh.md`](docs/dev/spec-refresh.md)
explains how the generated schemas and the reference follow.

The owner's decisions behind the public contract, and why, are in
[`docs/dev/decisions.md`](docs/dev/decisions.md). A change to a public signature, default,
column, or failure behavior adds an entry there. The facts about Massive that the documentation
does not settle, and the live tests that settle them, are in
[`docs/dev/verification-items.md`](docs/dev/verification-items.md).

## Agent instructions and skills

The repository carries every rule a coding agent needs; no agent should depend on notes kept
outside it.

- [`AGENTS.md`](AGENTS.md) holds only what every agent needs on every task. A rule for one kind
  of work goes in the skill for that work (`.claude/skills/`) or on a `docs/dev/` page.
- A skill states general rules. The facts of this project sit in one labeled place in the skill
  or its standard, never woven into the rules or fitted to the wording of its eval prompts.
- Skill evals are expensive. Weigh each round against what it gains: stop once the outputs meet
  the bar, use few prompts to compare models, skip baseline reruns you don't need, and report
  the eval cost beside the gain.

## Live integration tests

The tests in `tests/**/test_*_integration.py` call Massive itself with the owner's keys, so
neither `./scripts/check.sh` nor CI ever runs them. [`docs/live-tests.md`](docs/live-tests.md)
says how to run them and what each plan can verify.
