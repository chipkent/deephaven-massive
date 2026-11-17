# 131. CI checks a docstring-only pull request on one Python and stops a closed one's run

Date: 2026-11-17

Status: accepted

## Question

A pull request whose only Python change is a two-line docstring in `src/` ran the whole test
matrix: six jobs, about 92 runner-minutes. A run also kept going after its pull request closed.
Should CI check such a pull request on one Python version, and stop a closed one's run?

## Options

1. Keep both as they were.
2. Check such a pull request on Python 3.14 alone, and cancel a closed pull request's run.

## Ruling

Option 2, decided by the owner in the coordinating chat session on 2026-11-17. A pull request
into `main` whose changed Python files differ only in docstrings and comments runs the suite
with coverage on Python 3.14 alone and skips the build job; lint and the site build run as before. The reduction applies to pull requests only: once a change merges to `main`, the
run on `main` runs the full suite on every version, as before. Closing or merging a pull request
cancels its run still in progress.

## Reason

The owner gave none beyond approving the proposal, which argued that the 3.14 suite with
coverage still runs every check that reads docstrings, and that a closed pull request's run
spends runner time on a result nobody reads.

## Consequences

A docstring-only pull request is no longer checked on the other Python versions or the lowest
dependency versions, nor is its wheel built. A problem only those jobs catch shows after the
merge, in the run on `main`: for example, Python 3.13 and later strip a docstring's indentation
from `__doc__`, so a test reading it raw can pass on 3.14 and fail on 3.11. Any doubt about a
change, such as a new file or a changed `pragma` comment, runs everything.

## Rule

`.github/workflows/ci.yml`, the `lint` job's `Decide which jobs the change affects` step and the
`concurrency` setting, and `scripts/docstring_only_change.py`.
