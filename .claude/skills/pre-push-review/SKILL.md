---
name: pre-push-review
description: Review a branch locally, as a hostile top-1% reviewer, and fix what you find before any push that triggers CI and the review bots (opening a PR, or pushing a batch of fixes to one). Use it every time before such a push.
---

# Review locally before you push

Every push to a branch with an open PR runs the full CI matrix and the review bots, and both cost
money. Find the problems here, where they are free, so the push that follows is the last one for
that batch.

Run this review **after** `./scripts/check.sh` (full) is green and **before** the push that opens
the PR or delivers a batch of fixes. Fix every finding, rerun the full check, and then push once,
with the review fixes, any merge of `main`, and the description edits together. A branch with no
PR runs no CI, so pushing it only to keep work safe is fine; open its PR when it is ready.

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
  patching around them. Did the diff widen `tests/test_layout.py` to make itself pass? Undo that
  and ask the owner.
- **Generated data.** No hand-written schema, no edit to `spec/*.json`, `docs/datasets/`, or
  `vendor/massive/`; hand input only in the refresh hand-input files, each entry with a reason.
- **Secrets.** No key, secret or proxy password can reach a message, `repr` or log record.

## 2. Prove the tests would catch a regression

For each behaviour the diff adds or fixes, break the code on purpose (invert a condition, drop a
call, return early, swap an order) and confirm a test fails. Restore the code after each attempt.
A change that no test catches needs a test, or it is dead code to delete. Run new concurrency
tests several times, and on the free-threaded build when the code is threaded (its own
environment, `.venv-314t`, so `.venv` survives):

```bash
PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest <the new test files> -q
```

## 3. Check the words

- Docstrings meet the documentation standard in `AGENTS.md`: every argument, return, exception
  and side effect, and examples that run. They describe the code as it is now.
- `DESIGN.md`, `PLAN.md`, `AGENTS.md`, and `docs/dev/` say exactly what the code does.
- No planning references in code, comments, tests or messages.
- The PR description meets `docs/dev/pull-requests.md`: read that page, then go through its
  "Checklist before a push". Fix the description with `gh pr edit <n> --body-file <file>` in the
  same batch as the push.

## 4. Check the conversation

On a PR that already has reviews, every comment has a reply: the commit that fixes it and a
summary of the fix, or why nothing changes. Read the review bodies and the conversation comments
(`gh pr view <n> --comments`), then list the inline threads with their comment counts and last
authors:

```bash
gh api graphql -f query='query($o:String!,$r:String!,$n:Int!){repository(owner:$o,name:$r){pullRequest(number:$n){reviewThreads(first:100){nodes{isResolved comments(last:1){totalCount nodes{author{login} url}}}}}}}' \
  -F o=chipkent -F r=deephaven-massive -F n=<n> \
  --jq '.data.repository.pullRequest.reviewThreads.nodes[] | .comments | "\(.totalCount) \(.nodes[0].author.login) \(.nodes[0].url)"'
```

A thread with a single comment, or whose last comment is a bot's, still needs an answer, resolved
or not. `gh` posts under the owner's account, so a last comment with the owner's login may be
your reply or a new question from the owner: open it and check. Draft the replies now and post
them right after the push, naming the commit.

## 5. Fix, then push once

Fix every finding, rerun `./scripts/check.sh` (full), and push once. Put anything you
deliberately left alone, with its reason, in the PR description so the reviewer does not
rediscover it.
