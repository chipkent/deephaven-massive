---
name: pre-push-review
description: Review a branch locally, as a hostile top-1% reviewer, and fix what you find before any push that triggers CI and the review bots (opening a PR, or pushing a batch of fixes to one). Use it every time before such a push.
---

# Review locally before you push

Every push to a branch with an open PR runs the full CI matrix and the review bots, and both cost
money (AGENTS.md hard rule 15). Find the problems here, where they are free, so the push that
follows is the last one for that batch.

Run this review **after** `./scripts/check.sh` (full) is green and **before** the push that opens
the PR or delivers a batch of fixes. Fix every finding, rerun the full check, and then push once.

## 1. Read the whole diff as a reviewer, not as its author

```bash
git fetch origin
git diff origin/main...HEAD --stat
git diff origin/main...HEAD
```

Read it hunting for defects, with the spec open: the PR's `PLAN.md` section, every `DESIGN.md`
section it implements, and `AGENTS.md`. For each changed file ask:

- **Correctness.** Does every branch do what the spec says? Empty inputs, one element, many,
  `None`, boundaries (midnight, day zones, nanosecond limits), errors from the network and from
  Deephaven. Is any failure swallowed, logged without failing the table that needs it, or retried
  forever?
- **Contract.** Does every public signature, default, column and error match `DESIGN.md`
  exactly? A change to one updates `DESIGN.md` first.
- **Concurrency** (any code that runs off the caller's thread). The Deephaven invariants in
  `AGENTS.md`: execution context entered; update-graph lock before `registry_lock`, which stays a
  leaf; nothing blocking in a listener; results in their own liveness scope; no reliance on
  `dict`/`set` atomicity. Look for a state written before the operation that can still fail.
- **Layers and boundaries.** `tests/test_layout.py` passes, but also: does the change put logic
  in the layer that owns it, or work around a shared bug locally? Report shared bugs instead of
  patching around them.
- **Generated data.** No hand-written schema, no edit to `spec/*.json`; hand input only in the
  refresh hand-input files, each entry with a reason.
- **Secrets.** No key, secret or proxy password can reach a message, `repr` or log record.

## 2. Prove the tests would catch a regression

For each behaviour the diff adds or fixes, break the code on purpose (invert a condition, drop a
call, return early, swap an order) and confirm a test fails. Restore the code after each attempt.
A change that no test catches needs a test, or it is dead code to delete. Run new concurrency
tests several times, and on the free-threaded build when the code is threaded:

```bash
PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest <the new test files> -q
```

`UV_PROJECT_ENVIRONMENT` keeps the free-threaded build in its own environment, `.venv-314t`
(ignored by git). Without it, `uv run --python 3.14t` replaces the worktree's `.venv` with a
free-threaded one, and the next plain `uv run` or `./scripts/check.sh` replaces it back.

## 3. Check the words

- Docstrings meet the documentation standard in `AGENTS.md`: every argument, return, exception
  and side effect, and examples that run. They describe the code as it is now.
- `DESIGN.md`, `PLAN.md` and `AGENTS.md` say exactly what the code does.
- No planning references in code, comments, tests or messages.
- The PR description meets "Pull request descriptions" in `AGENTS.md`. Read it twice: as a
  very intelligent reviewer arriving with no context, and as a less experienced user reading it
  much later to learn what the feature does. Check that:
  - the Summary alone lets either reader explain the PR to someone else: it gives the big
    picture (what the library is for, which feature the PR is about, what a user does with it,
    and where the PR fits in the overall work), the background with every term defined, exactly
    what the PR does step by step (every user-visible change and every rule it adds or
    changes), why it matters with a concrete example, and what the reader should check or
    decide, with no file names, internal names, or decision numbers;
  - Summary, Why, What changes, Decisions, How it works, Verification, and Follow-ups are present
    in that order; Decisions lists every departure from the plan with its reason or says
    "None"; What changes shows before and after wherever the public API or a result changes;
    How it works carries the engineering detail (approach, main modules and functions, anything
    subtle), organized by idea;
  - it has no review-finding labels, agent identifiers, commit-by-commit narration,
    first-person process, stale counts, or undefined terms;
  - it describes the branch as it will be after this push, rewritten rather than appended to;
  - `Closes #<issue>` (or `Refs #<issue>`, or "No tracking issue.") sits on its own line, then
    the required footer.

  Fix the description with `gh pr edit <n> --body-file <file>` in the same batch as the push.

## 4. Fix, then push once

Fix every finding, rerun `./scripts/check.sh` (full), and push once. Put anything you
deliberately left alone, with its reason, in the PR description so the reviewer does not
rediscover it.
