---
name: pre-push-review
description: "Mandatory gate before any push that runs paid CI and the review bots. Use this skill whenever the user asks to open, create, or ship a PR, or to push commits to a branch that already has a PR. That includes pushing review fixes, addressing reviewer or bot comments (coderabbit, the owner), updating a PR description, or \"push the batch\". Invoke it even when the request only says \"push\", \"ship it\", or \"open the PR\", and even if check.sh is already green. The skill does a hostile self-review of the diff, checks that the tests would catch a regression, checks the docs, edits the PR description until it passes the writing checklist, drafts replies to review threads, fixes every finding, and only then pushes once. Skip it for pushing a work-in-progress branch that has no PR (backups), for reviewing someone else's PR, for debugging CI, and for general questions."
---

# Review locally before you push

Every push to a branch with an open PR runs the review bots and every CI job its changes affect
(the full test matrix whenever code or a test input changes), and both cost money. Find the
problems here, where they are free, so the push that follows is the last one for that batch.

Run this review **before** the push that opens the PR or delivers a batch of fixes. Fix every
finding and push once, with the review fixes, any merge of `main`, and the description edits
together; the pre-push hook runs `./scripts/check.sh`, so do not run it first. A branch with no
PR runs no CI, so pushing it only to keep work safe is fine; open its PR when it is ready.

A green check proves only what the tests already pin. This review looks for what they do not:
code in the wrong place, a choice hidden behind `None`, a test that passes for wrong code, a lock
whose scope changed, and words that no longer match the code. Write each finding down as you go
(file and line, what is wrong, the fix), so none is lost between reading and fixing.

## 1. Read the whole diff as a reviewer, not as its author

```bash
git fetch origin
git diff origin/main...HEAD --stat
git diff origin/main...HEAD
```

Read it hunting for defects, with the spec open: the PR's `PLAN.md` section, every `DESIGN.md`
section it implements, and `AGENTS.md`. In the `--stat`, look first at deleted lines under
`tests/`: a deleted or weakened test is a finding unless the diff adds one that pins the same
property. For each changed file ask:

- **Correctness.** Does every branch do what the spec says? Empty inputs, one element, many,
  `None`, boundaries (midnight, day zones, nanosecond limits), errors from the network and from
  Deephaven. Is any failure swallowed, logged without failing the table that needs it, or retried
  forever?
- **Contract.** Does every public signature, default, column and error match `DESIGN.md`
  exactly? A change to one updates `DESIGN.md` first. Does the new behavior match how the API
  already treats the same concept elsewhere (ranges, dates, missing data, errors)? An
  inconsistency with a sibling call is a finding even when `DESIGN.md` is silent.
- **Concurrency** (any code that runs off the caller's thread, and any change to a `with ...lock:`
  block). Write down what now runs under each lock the diff touches: a constructor that reads
  files or settings counts as blocking work. The Deephaven invariants in
  `AGENTS.md`: execution context entered; update-graph lock before `registry_lock`, which stays a
  leaf; nothing blocking in a listener; results in their own liveness scope; no reliance on
  `dict`/`set` atomicity. Look for a state written before the operation that can still fail.
  A lock change ships with a test that races two threads through it.
- **Layers and boundaries.** `tests/test_layout.py` passes, but also: does the change put logic
  in the layer that owns it, or work around a shared bug locally? Report shared bugs instead of
  patching around them. Did the diff widen `tests/test_layout.py` to make itself pass? Undo that
  and ask the owner.
- **Placement and organization.** For every function, class and module the diff adds or moves,
  assume it is in the wrong place until you can argue otherwise. Would a newcomer look for it in
  this file and package? Do the module's name and docstring describe everything in it, or is it a
  grab-bag of unrelated concerns? Does it duplicate a helper that already exists elsewhere? Is it
  in the layer and package that own the concept? Should the module be split, merged or renamed?
  Do the tests mirror the new location? Fix a misplaced item by moving it in the same PR, not by
  noting it.
- **Names.** Every new or renamed function, argument, column, environment variable, and pytest
  marker says what it is or does (`AGENTS.md` rule 18). After any rename, `git grep` the old
  name across code, docs, and `DESIGN.md`, and check the PR description and its linked issues'
  bodies; fix every hit.
- **Duplicates and private imports.** For every new helper, `git grep -n` for the concept (the
  format it parses, the rule it applies), not only for its name. An import of another module's
  private name (`from x import _helper`) is a sign the new code sits in the wrong module.
- **Simplicity.** Assume a simpler design exists until you have looked for one. Look for a
  workaround layer where a fix at the source would do, a second mechanism beside one that
  already exists, an option or setting nobody asked for, a hybrid that keeps the problems of
  both approaches, and a guard test or check that is brittle or only pins configuration text
  instead of testing behavior. Fix the root cause where it lives, or report it.
- **Explicit choices.** A parameter whose `None` selects a mode or a behavior (`convention=None`
  meaning "the default convention") should take an explicit enum member as its default instead.
  `None` means only a value that is genuinely absent.
- **Generated data.** No hand-written schema, no edit to `spec/*.json`, `docs/datasets/`, or
  `vendor/massive/`; hand input only in the refresh hand-input files, each entry with a reason.
- **Vendor data.** Does the change fill, correct, filter out, re-stamp, replace, or otherwise
  second-guess Massive's data beyond faithful conversion of units, types, column names, and
  symbols (`AGENTS.md` rule 23)? That is a blocker unless the owner approved it. When Massive's
  data is wrong, record the evidence and draft a report to Massive for the owner instead.
- **One bad symbol or row.** Does any failure of one symbol or row fail a whole table, or skip
  without logging, or lack an `OnError` setting (`AGENTS.md` rule 24)? If so, it is a blocker
  (rule 24's exceptions aside: a `RAISE` setting, a `str` or list passed as `symbols` in code,
  and problems that affect every row).
- **Secrets.** No key, secret or proxy password can reach a message, `repr` or log record.

## 2. Prove the tests would catch a regression

For each behaviour the diff adds or fixes, break the code on purpose (invert a condition, drop a
call, return early, swap an order) and confirm a test fails. Restore the code after each attempt.
For a function that computes a value, also return a wrong value of the right type (off by one,
two fields swapped): a test that only checks that something is returned, or that nothing
raises, passes for wrong code. A change that no test catches needs a test, or it is dead code to
delete. Run new concurrency tests several times, and on the free-threaded build when the code is
threaded (its own environment, `.venv-314t`, so `.venv` survives):

```bash
PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest <the new test files> -q
```

## 3. Check the words

- Docstrings meet the documentation standard in `AGENTS.md`: every argument, return, exception
  and side effect, and examples that run. They describe the code as it is now: reread the
  docstring and comments of every function whose body changed.
- `DESIGN.md`, `PLAN.md`, `AGENTS.md`, and `docs/dev/` say exactly what the code does.
- Every comment in a config file, pytest marker description, and CLI help text the diff adds or
  changes reads as edited prose for a reader new to the project: what it means, when to use
  it, and what each argument means, with no internal shorthand.
- No planning references in code, comments, tests or messages:
  `git diff origin/main...HEAD -- src tests scripts .github | grep -nE '^\+.*(DESIGN|PLAN|§)'`.
- The PR title and every commit subject on the branch (`git log origin/main..HEAD --format=%s`)
  meet the Title section of `docs/dev/pull-requests.md`: a type, then one specific idea in the
  imperative, at most 60 characters, the effect rather than the technique, and no internal
  names or jargon. Count with `printf %s "<title>" | wc -c`. Fix the PR title with
  `gh pr edit <n> --title`, and the last unpushed commit subject with `git commit --amend`.
- **An open question in the description is a finding** ("should this return X?", "happy to
  switch"). Answer it from `AGENTS.md`, `DESIGN.md` and the code, and record the choice and its
  reason under Decisions; if only the owner can decide, or you are unsure whether you may
  decide (section 1 of the `writing-to-the-owner` skill), stop and ask before the push, using
  that skill. Never answer one by overturning an `AGENTS.md` rule, such as dates being ISO
  strings.

## 4. Edit the PR description

This step must pass before the push. A description that is padded, narrated, hard to scan, or
hides a fact in a paragraph is a finding like any bug. The owner's verdict on such text:
"rambly", "agent slop". Length is not the test: a complex PR may need more words, and no fact
the reader needs is ever cut.

1. Write the description to a file, or fetch the current one:
   `gh pr view <n> --json body --jq .body > <file>`. Rewrite it to the branch's final state.
2. Run the self-edit checklist in `docs/dev/pull-requests.md` on the file, every item, and
   edit until each passes. Item 1 comes first: skim only the headings and the first words of
   each bullet, and check that every fact is there. Item 2 next: read it as an O'Reilly
   technical editor would, and fix what they'd mark.
3. Grep for narration and hedges, as the checklist's banned-pattern item shows:
   `grep -nE '\b(I|we|We)\b|[Nn]ote that|should probably|might|[Ff]irst version|then re-?ran' <file>`.
   Fix every hit that is narration, a hedge, or filler.
4. Check that the body names the `DESIGN.md` decision or verification-item number it takes, if
   any.
5. Apply it with `gh pr edit <n> --body-file <file>` in the same batch as the push, then check
   `gh pr view <n> --json closingIssuesReferences`.

## 5. Check the conversation

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
your reply or a new question from the owner: open it and check. Draft the replies now; section 7
posts them.

## 6. Fix, then push once

Fix every finding and push once; the pre-push hook runs `./scripts/check.sh` and must pass. Put
anything you deliberately left alone, with its reason, in the PR description so the reviewer
does not rediscover it.

## 7. After the push: post the replies and prove they landed

1. Post each drafted reply in its thread. A reply that claims a fix names the commit (short
   SHA) that makes it; a reply that changes nothing says why.
2. `git fetch`, then check that each commit a reply names is on the remote branch and fixes
   what the reply claims: `git branch -r --contains <sha>` and `git show <sha>`. A merge of
   `main` fixes nothing. For a reply that changes nothing, reread the code it cites and confirm
   the reason still holds.
3. When CI and the review bots have finished on the pushed commit, rerun the thread query from
   section 5: every thread's last comment is your reply. Answer any new one the same way. Only
   then report the PR as addressed or ready.
