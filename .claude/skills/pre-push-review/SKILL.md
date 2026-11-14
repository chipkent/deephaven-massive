---
name: pre-push-review
description: "Mandatory gate before any push that runs paid CI and the review bots. Use it whenever you are about to open a PR, or to push to a branch that already has one: review fixes, answers to reviewer or bot comments (coderabbit, codex, the owner), a merge of main, or \"push the batch\". Use it even when the request only says \"push\", \"ship it\", or \"open the PR\", and even if check.sh is green. It attacks the diff as a hostile senior reviewer would (only a typo or one-word doc fix skips it): scope against the request, the public API surface, module cohesion, code bent to satisfy a rule, and tests that catch a regression; then it checks the words and the PR description, drafts the review-thread replies, and pushes once. Not for a branch with no PR (a backup push), for reviewing someone else's PR, or for debugging CI."
---

# Review locally before you push

Every push to a PR branch runs paid CI and the review bots. This review exists so that they run
as rarely as possible: find here, for free, every problem they or the owner would find, and push
once per finished batch. A green check proves only what the tests already pin. This review looks
for what they do not: code in the wrong place, a test that passes for wrong code, a lock whose
scope changed, a public type that leaks a private one, and words that no longer match the code.

**Be adversarial.** You are not the author checking your work; you are a hostile senior
reviewer whose job is to reject this change. Assume it is wrong until the diff proves otherwise.
Attack it: hunt for the input that breaks it, the thread interleaving that deadlocks it, the
test that would still pass if the code were wrong, the simpler design it missed, and the rule it
quietly bends. Each finding names the input, the interleaving, or the rule that makes the code
wrong. On a non-trivial change, finding nothing is a reason to look once more, not a reason to
invent something.

**Only a trivial change skips this review**: a typo, or a one-word fix in a comment or a doc,
that changes no code, test, configuration, public name, or documented behavior. Anything else,
however small it looks, gets the full review.

A risky change (concurrency, connections, TLS, or the public contract) may also get a second,
separate reviewer agent. That review comes in addition to this one, never instead of it.

Scope it to what is new. The push that opens the PR reviews the whole branch
(`git diff origin/main...HEAD`, or the PR below in a stack); a later push reviews the commits
since the last push and how they interact with the rest. Keep a findings list (file and line,
what is wrong, the fix) as you go. Fix every finding inside the change before the push. A
finding that would add work the request does not need (a feature, tooling, a refactor of code
the change does not touch) goes under Follow-ups in the description, with an issue.

## 1. Review the diff

Attack it as the reviewer who has to defend the codebase, with the request (the owner's words,
or the issue's stated problem and acceptance list) and `AGENTS.md` open. Start from
`git diff --stat`: a deleted or weakened test is a finding unless the diff adds one that pins the
same property. Then look for:

- **Scope.** Every changed file serves the request, and nothing it needs is missing. Regenerated
  output the change did not cause stays out.
- **Wrong behavior.** Empty, one, and many inputs; boundaries (midnight, day zones, nanosecond
  limits); network and Deephaven errors. A failure swallowed, logged without failing what needs
  it, or retried forever. State written before an operation that can still fail.
- **Contract drift.** A public change (`AGENTS.md`, **A public change needs the owner's request
  or approval**) without matching docs and tests, or without a `decisions.md` entry when it sets or changes a
  rule or contract; from `0.1.0`, a change that
  breaks code written to the last release without the owner's permission and the next version
  named; behavior inconsistent with how a sibling call treats the same concept.
- **The public API surface**, for any change to what a user can call or reach. Write the call a
  user types, from `import deephaven_massive as dm` to the result, and name the type every
  public signature on that path declares: each parameter, return, property, and documented
  `Raises`. Each must be public (importable by a path with no underscore-prefixed part) and
  documented. A public signature the change adds or alters that declares a private type is a
  blocker, whoever wrote it; one that already did is fixed at its source when the request
  names it, and otherwise listed under Follow-ups.
  A private implementation behind a public declared type is fine, and so are members a public
  class inherits from a private base, which are part of the public class. For every new public
  name, write the user code that needs it; a name no user needs is a blocker. A new public
  module is judged on the same test, never on whether a list allows it.
- **Concurrency**, for anything off the caller's thread or inside a `with ...lock:` block.
  Write down what now runs under each lock the diff touches (a constructor that reads files
  counts as blocking) and check it against the Deephaven invariants. A lock change ships with a
  test that races two threads through it; run it several times, and on the free-threaded build
  (`PYTHON_GIL=0 UV_PROJECT_ENVIRONMENT=.venv-314t uv run --python 3.14t pytest <files> -q`).
- **Cohesion.** For each module the diff creates or grows, state its subject in one line. Name
  any cluster of definitions that never calls the rest of the module, is tested with separate
  fixtures, or is imported by a different set of modules: that cluster is a second subject and
  belongs in its own module. Length alone is no finding; when a reviewer flags size alone, answer
  with the module's subject line and this test.
- **Code shaped to pass a tool.** Search the diff for `if TYPE_CHECKING:` blocks holding
  anything but imports, and for each new `cast`, `Any`, `# type: ignore`, `# noqa`,
  `# pragma: no cover`, `getattr` with a constant name, or code that exists only so ruff, mypy, or
  coverage pass. Each is a blocker unless the owner accepted it: fix the design so the tools see
  the true API.
- **A closed set handled by a catch-all** (`AGENTS.md`, **Every choice over a closed set
  handles each case by name**). Search the added lines:
  `git diff origin/main...HEAD -- src | grep -nE '^\+.*(\belse:|case _:|\.get\(|isinstance|except\b)'`.
  For each hit that chooses over an enum, a union, the forms of `symbols`, or a fixed set of
  values, name the member or type a later change could add and what the code would do with it.
  A default branch, `dict.get` default, or type-filtered drop that takes it silently is a
  finding: name every case and end in `assert_never` or `BrokenGuaranteeError`. A fallback
  passes only with a comment saying why; a new lookup table over an enum needs its every-member
  test.
- **The library doing Massive's job.** A check that refuses a value Massive could judge (a
  ticker's shape, an allowed parameter value, a valid combination) is a finding unless it is a
  wrong Python type, a value the library cannot express, or one with evidence that Massive
  would return wrong data silently (`AGENTS.md`, Massive owns its data and its rules); and an error that reinterprets or hides Massive's answer instead of reporting it with the call that caused it.
- **Docstrings that inventory.** Read the first paragraph of every docstring the diff adds or
  changes: it must say what the thing is for and when to use it, not list what it contains
  (`writing-docstrings`).
- **Design smells.** Assume a simpler design exists until you have looked: a workaround layer
  where a fix at the source would do, a second mechanism beside an existing one, an option
  nobody asked for, a public module that only re-exports a private one (a public twin), a helper that duplicates one elsewhere (`git grep` the concept, not only the
  name), an import of another module's private name, a guard test that pins configuration text
  instead of behavior, a standard file (the GitHub `.gitignore` template above its marked
  project section, a tool's documented default) trimmed or rewritten without a stated purpose,
  and a new generated output without one command that owns its whole directory and a test that
  fails on a stale, missing, or orphaned file. Assume every new function and module is
  misplaced until you can say why a newcomer would look for it there; move code this PR adds in
  this PR, and record misplaced code it does not touch as a Follow-up.
- **Code bent to satisfy a rule.** The rules are not beyond review. Code is bent when it meets a
  rule's words while defeating its intent (a docs tool configured to show a private class under
  a public name; an upward import tucked behind a callback), or takes a worse shape (an
  indirection, a duplicate, a configured exception, an allow-list entry added only so the
  code passes, a raised lint limit) only to pass a rule. Code a rule prescribes (pass objects
  down, a new `OnError` setting, ISO date text, a decision entry) is compliance, and facts of Python, Deephaven, and Massive are constraints. Bent code
  is a finding against the rule as well as the code: raise it with the owner as `AGENTS.md`
  says for a conflict, naming the rule, what it cost here, and the edit you propose to it.
- **The `AGENTS.md` rules reviews most often catch**: a `None` that selects a mode, a vendor
  value filled or corrected, a failure hard-coded or a silent skip where a `dm.OnError` setting
  or argument should decide (one bad symbol or row, a partial refusal), a hand
  edit to generated files, a check loosened without the owner (a widened `tests/test_layout.py`
  rule, a raised lint limit, a rule turned off), a suppression without its reason or the owner's acceptance, a new dataset column that
  gives a field another dataset already names a second name, a secret
  that can reach a message, `repr`, or log, and a new name that is an abbreviation. After a
  rename, `git grep` the old name across code, docs, the PR body, and its issue; past entries
  in the append-only `docs/dev/decisions.md` and `verification-items.md` keep it.

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
- No decision or planning references in code (`AGENTS.md`):
  `git diff origin/main...HEAD -- src tests scripts .github | grep -nE '^\+.*([Dd]ecision [0-9]|DESIGN|PLAN|§)'`.
- The PR title and every commit subject meet the Title section of `docs/dev/pull-requests.md`;
  `uv run python scripts/check_pr_title.py "<title>"` checks the mechanical part. Retitle with
  `gh pr edit <n> --title`.

## 4. The PR description

Write or rewrite it to the branch's final state with the `writing-a-pr-description` skill. An
open question in it ("should this return X?") is a finding: answer it from `AGENTS.md`,
`decisions.md`, and the code and record the answer under Decisions. If it is the owner's call,
ask with the `writing-to-the-owner` skill and don't build the contested part. A PR complete
except for that part is not work in progress: push it with the question under Check before
merging, where it blocks the merge, not the push.

## 5. Review threads

On a PR with reviews, read the reviews and conversation (`gh pr view <n> --comments`), then the
inline threads:

```bash
gh api graphql -f query='query($o:String!,$r:String!,$n:Int!){repository(owner:$o,name:$r){pullRequest(number:$n){reviewThreads(first:100){nodes{isResolved comments(last:1){totalCount nodes{author{login} url}}}}}}}' \
  -F o=chipkent -F r=deephaven-massive -F n=<n> \
  --jq '.data.repository.pullRequest.reviewThreads.nodes[] | .comments | "\(.totalCount) \(.nodes[0].author.login) \(.nodes[0].url)"'
```

Every thread needs an answer, resolved or not, including a bot's: the commit that fixes it and
how, or why nothing changes. Never resolve a thread without a reply. `gh` posts as the owner, so
a last comment under the owner's login may be your reply or a new question: open it. Draft each
reply now. A reply that cites a rule (a log level, a lock order, a decision) quotes it from its
source after reading it.

## 6. Push once, then close the loop

1. Fix every in-scope finding, and record each Follow-up with its issue. Anything deliberately
   left alone goes in the description with its reason.
2. Push once, with the fixes, any merge of `main`, and the description
   (`gh pr edit <n> --body-file <file>`). Push once when the PR opens, then once per finished
   batch of fixes; never push work in progress or to see whether CI passes. The pre-push hook
   runs `./scripts/check.sh`: ruff, mypy, and the tests of the changed files, without
   coverage; CI runs the whole suite. Don't run it first.
3. Post each reply. After `git fetch`, confirm that each commit a reply names is on the remote
   branch and makes the claimed fix (`git branch -r --contains <sha>`, `git show <sha>`); a
   merge of `main` fixes nothing.
4. When CI and the bots finish, rerun the thread query and answer anything new. Only then call
   the PR addressed.
