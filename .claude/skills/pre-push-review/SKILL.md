---
name: pre-push-review
description: "Mandatory gate before any push that runs paid CI and the review bots. Use it whenever you are about to open a PR, or to push to a branch that already has one: review fixes, answers to reviewer or bot comments (coderabbit, codex, the owner), a merge of main, or \"push the batch\". Use it even when the request only says \"push\", \"ship it\", or \"open the PR\", and even if check.sh is green. It attacks the diff as a hostile senior reviewer would (only a typo or one-word doc fix skips it), proves the tests catch a regression, checks the words and the PR description, drafts the review-thread replies, and pushes once. Not for a branch with no PR (a backup push), for reviewing someone else's PR, or for debugging CI."
---

# Review locally before you push

Every push to a PR branch runs paid CI and the review bots. This review exists so that they run
as rarely as possible: find here, for free, every problem they or the owner would find, and push
once per finished batch. A green check proves only what the tests already pin. This review looks
for what they do not: code in the wrong place, a test that passes for wrong code, a lock whose
scope changed, and words that no longer match the code.

**Be adversarial.** You are not the author checking your work; you are a hostile senior
reviewer whose job is to reject this change. Assume it is wrong until the diff proves otherwise.
Attack it: hunt for the input that breaks it, the thread interleaving that deadlocks it, the
test that would still pass if the code were wrong, the simpler design it missed, and the rule it
quietly bends. A review that finds nothing on a non-trivial change has usually not looked hard
enough, so look again before you conclude it is clean.

**Only a trivial change skips this review**: a typo, or a one-word fix in a comment or a doc,
that changes no code, test, configuration, public name, or documented behavior. Anything else,
however small it looks, gets the full review.

A risky change (concurrency, connections, TLS, or the public contract) may also get a second,
separate reviewer agent. That review comes in addition to this one, never instead of it.

Scope it to what is new. The push that opens the PR reviews the whole branch
(`git diff origin/main...HEAD`); a later push reviews the commits since the last push and how
they interact with the rest. Keep a findings list (file and line, what is wrong, the fix) as you
go, and fix every item before the push.

## 1. Review the diff

Attack it as the reviewer who has to defend the codebase, with the PR's issue and `AGENTS.md`
open. Start from `git diff --stat`: a deleted or weakened test is a finding unless the diff adds
one that pins the same property. Then look for:

- **Wrong behavior.** Empty, one, and many inputs; boundaries (midnight, day zones, nanosecond
  limits); network and Deephaven errors. A failure swallowed, logged without failing what needs
  it, or retried forever. State written before an operation that can still fail.
- **Contract drift.** A user-visible change without a `decisions.md` entry and matching docs and
  tests (rule 9), or behavior inconsistent with how a sibling call treats the same concept.
- **Concurrency**, for anything off the caller's thread or inside a `with ...lock:` block.
  Write down what now runs under each lock the diff touches (a constructor that reads files
  counts as blocking) and check it against the Deephaven invariants. A lock change ships with a
  test that races two threads through it; run it several times, and on the free-threaded build
  (`PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest <files> -q`).
- **Design smells.** Assume a simpler design exists until you have looked: a workaround layer
  where a fix at the source would do, a second mechanism beside an existing one, an option
  nobody asked for, a helper that duplicates one elsewhere (`git grep` the concept, not only the
  name), an import of another module's private name, a guard test that pins configuration text
  instead of behavior, a standard file (the GitHub `.gitignore` template above its marked
  project section, a tool's documented default) trimmed or rewritten without a stated purpose,
  and a new generated output without one command that owns its whole directory and a test that
  fails on a stale, missing, or orphaned file. Assume every new function and module is
  misplaced until you can say why a newcomer would look for it there (rule 18); move it in this
  PR rather than noting it.
- **The `AGENTS.md` rules reviews most often catch**: a `None` that selects a mode (19), a
  vendor value filled or corrected (23), one bad symbol or row that fails a table or skips
  silently (24), a hand edit to generated files (17), a widened `tests/test_layout.py` (16), a
  secret that can reach a message, `repr`, or log (22, 25), a new name that is an abbreviation
  (18). After a rename, `git grep` the old name across code, docs, the PR body, and its issue;
  past entries in the append-only `docs/dev/decisions.md` and `verification-items.md` keep it.

## 2. Prove the tests catch a regression

For each behavior the diff adds or fixes, break the code on purpose (invert a condition, drop a
call, return a wrong value of the right type) and confirm a test fails; restore it after each
try. A test that only checks that something returns, or that nothing raises, passes for wrong
code. A change no test catches needs a test, or it is dead code to delete. Spend this on logic;
a docs or config change has nothing to break.

## 3. Check the words

- Docstrings and comments of every function whose body changed still describe it (the
  `writing-docstrings` skill).
- For each behavior the diff changes (a default, a limit, an error, a column), find every page
  that describes it: `git grep -n '<name or message>' -- README.md docs/`. A diff that touches
  a user page follows steps 5 and 6 of the `writing-user-docs` skill: the calibrated editor's
  grade and the final fact check.
- No decision or planning references in code (rule 21):
  `git diff origin/main...HEAD -- src tests scripts .github | grep -nE '^\+.*([Dd]ecision [0-9]|DESIGN|PLAN|§)'`.
- The PR title and every commit subject meet the Title section of `docs/dev/pull-requests.md`;
  `uv run python scripts/check_pr_title.py "<title>"` checks the mechanical part. Retitle with
  `gh pr edit <n> --title`.

## 4. The PR description

Write or rewrite it to the branch's final state with the `writing-a-pr-description` skill. An
open question in it ("should this return X?") is a finding: answer it from `AGENTS.md`,
`decisions.md`, and the code and record the answer under Decisions, or, if it is the owner's
call, stop and ask with the `writing-to-the-owner` skill before pushing.

## 5. Review threads

On a PR with reviews, read the reviews and conversation (`gh pr view <n> --comments`), then the
inline threads:

```bash
gh api graphql -f query='query($o:String!,$r:String!,$n:Int!){repository(owner:$o,name:$r){pullRequest(number:$n){reviewThreads(first:100){nodes{isResolved comments(last:1){totalCount nodes{author{login} url}}}}}}}' \
  -F o=chipkent -F r=deephaven-massive -F n=<n> \
  --jq '.data.repository.pullRequest.reviewThreads.nodes[] | .comments | "\(.totalCount) \(.nodes[0].author.login) \(.nodes[0].url)"'
```

Every thread needs an answer, resolved or not, including a bot's. `gh` posts as the owner, so a
last comment under the owner's login may be your reply or a new question: open it. Draft each
reply now: the fix and the commit that makes it, or why nothing changes. A reply that cites a
rule (a log level, a lock order, a decision) quotes it from its source after reading it.

## 6. Push once, then close the loop

1. Fix every finding. Anything deliberately left alone goes in the description with its reason.
2. Push once, with the fixes, any merge of `main`, and the description
   (`gh pr edit <n> --body-file <file>`). The pre-push hook runs `./scripts/check.sh`; don't run
   it first.
3. Post each reply. After `git fetch`, confirm that each commit a reply names is on the remote
   branch and makes the claimed fix (`git branch -r --contains <sha>`, `git show <sha>`); a
   merge of `main` fixes nothing.
4. When CI and the bots finish, rerun the thread query and answer anything new. Only then call
   the PR addressed.
