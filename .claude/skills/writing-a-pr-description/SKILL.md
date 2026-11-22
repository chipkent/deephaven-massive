---
name: writing-a-pr-description
description: "Use this skill whenever you write or rewrite a pull request description, an issue body, a bug report to Massive or Deephaven (wrong data, a defect), or a report another agent or the owner will read as a record of work: opening a PR, rewriting a description after a push, filing an issue, or when the owner calls a description rambly, padded, machine-sounding, or hard to scan. It picks the facts a reader needs, drafts them into the structure of docs/dev/pull-requests.md in an editor's prose, runs that page's self-edit checklist, and has a fresh editor grade it, at most three times. Not for a PR title alone (the Title section of the same page), review replies, or questions and status messages to the owner (writing-to-the-owner)."
---

# Writing a PR description

The standard is `docs/dev/pull-requests.md`: read it first, including the Title section and the
self-edit checklist. Its test: a reader who skims only the headings, the first words of each
bullet, and the table rows knows what changed, why, the choices made, how it was checked, and
what is left. The owner reads every description to decide whether to merge, and has rejected
two opposite failures: padded narration ("rambly", "agent slop"), and text so packed with
detail that it reads like a machine wrote it.

## 1. Choose the facts

From the diff, the issue, and your notes, collect what the reader needs:

- what a user or developer can do, or sees, differently after the merge;
- why: the problem, with one concrete case, and any term the change depends on;
- each choice the PR makes, with its reason;
- each check run and its result, and what was not run;
- what is left, with its issue: Follow-ups, including review findings that add work;
- under Check before merging: whether the change breaks code written to the last release and,
  if so, the next version; each new dependency or raised floor; each new check entry; and
  each approval the PR relies on (a suppression, a public change, a dropped protection, a
  reversed decision, a rule edit), as the decision that records it (`decision 97`) or, before
  one exists, the question with the owner's dated answer (`AGENTS.md`, an approval is a
  decision file or the owner's answer; an import after a Deephaven server start needs none).
  A rule edit always shows the question with the owner's dated answer, since a decision
  citation alone does not approve one, and a decision the PR itself adds approves nothing.
  A PR that adds a decision file shows the file's text, so the owner reads the wording. A question
  for the owner is not a fact for the description: it goes to the owner first, and the PR
  waits for the answer (`pre-push-review` §4).

Then select for one reader, the owner deciding whether to merge. A fact earns its place by
changing that decision. A true fact that doesn't (an internal constant, a helper's name, a
test count, an argument for a choice the owner already made, how the branch got here) is
noise, and noise is how a description starts to read as machine output. The budget is about
600 words; a description that can't fit holds the wrong facts or is two PRs.

Write only what you know. When a fact the reader needs is missing from your sources (what a
test covers, what the old behavior was), check it in the code or flag it to whoever asked;
never fill the gap with a plausible guess.

## 2. Draft

Copy the sections from `.github/pull_request_template.md` (`gh pr create --body-file` never
shows it). End with the issue link that the "Close the issue" section of
`docs/dev/pull-requests.md` gives (`Closes #<issue>` on its own line for a full delivery) and the
attribution footer your session requires. Write the Summary last, as two to four plain sentences
that say what changes for a user and why, for a reader with no context, in words a library
user knows. A **Check before merging** list
holds the notices and quoted approvals from section 1, for the owner to see, and nothing the
owner has not settled (`AGENTS.md`, Ask, and keep working). `uv run python
scripts/check_pr_body.py` checks it before you publish.
Use bullets and tables where the reader scans (What changes, Decisions, Verification); use
sentences where the reader follows reasoning (Summary, Why, How it works).

Write to [How an editor writes](../../../docs/dev/user-docs.md#method): the rule first, one
case per sentence, three or more cases as a table, the subject and verb up front. Telegraphic
compression (stacked parentheticals, semicolon chains, six identifiers in one clause) and
exhaustive case lists are what make a description sound generated. Link to a doc page
instead of repeating it.

## 3. Edit

Run the self-edit checklist at the end of `docs/dev/pull-requests.md` and fix the draft until
every item passes. The item that matters most is reading it as an O'Reilly technical editor
would. Two passes help:

- **Skim pass**: headings and first words only. Is every needed fact visible?
- **Read-aloud pass**: read the Summary and Why aloud. Anything you would not say to a colleague
  (a hedge, a "Note that", a colon reveal, an internal name, a so-chain) gets rewritten. The
  banned patterns of `docs/dev/pull-requests.md` and the AI tells in
  `docs/dev/user-docs.md#ai-tells` list the usual offenders; the grep in the checklist finds
  the narration.

## 4. Grade

The author reads its own text in the code's vocabulary, so a fresh editor grades it. Spawn a
grader with no other context when the PR opens, and again only when a push changes what the PR
does. A push that only merges `main` or fixes review findings gets no new grade: update the
affected lines yourself. An issue body or report is graded once it is written, before it is
posted, against the sources it cites in place of a diff. A `test:`, `chore:`, or small `fix:` PR
gets one grade. `<base>` is the PR's base branch: `origin/main`, or the branch below it in a
stack. The three-dot diff leaves out what a merge of that base brought in.

```text
Read <repo>/.claude/skills/writing-user-docs/agents/editor.md and follow it exactly, with its
section "Grading a PR description". Grade <worktree>/.git-pr-body.md against the diff from
`git diff <base>...HEAD` in <worktree>. Return the score sheet.
```

Rewrite from its sheet and grade again with a fresh grader until the description passes, with
at most three fresh grades, or one for a PR that gets one (decision 130). The limit is per PR,
not per push: once a PR has had its grades, later pushes get none, and you update the affected
lines yourself. When the last grade allowed doesn't pass, push the description anyway and send
the grader's remaining objections to the owner verbatim (`writing-to-the-owner`).

## 5. Publish

Write the body to `.git-pr-body.md` at the root of your own worktree: `.gitignore` holds that
name, so `git add` never stages it, and two agents never overwrite each other's draft.

- New PR: `gh pr create --title "<title>" --body-file <file>`.
- Existing PR: `gh pr edit <n> --body-file <file>`, in the same batch as the push it describes.
- Confirm the issue link: `gh pr view <n> --json closingIssuesReferences`.

## A bug report to Massive or Deephaven

Report only a clear error that would mislead a user. First read Massive's wording in its market
context: "the price per share of the underlying" is right for an option premium, since a
contract usually covers 100 shares, and the same words in two datasets can mean different
things in each. A copy-pasted description that misleads no one, or a type that differs by asset
class because the data does (fractional crypto sizes), isn't worth a report.

A report of wrong vendor data (`AGENTS.md`, Supply Massive's data), a Deephaven defect, or any
text for another project goes to a maintainer who knows nothing of this library, and an unclear
report is declined or ignored. It carries facts only. Lead with the problem they can see, in one
or two plain sentences. Then give a minimal reproduction (the exact request, or symbol and date,
and the versions involved), the expected and the actual result, the evidence, and the cause
where you know it. Propose no fix unless the owner asks for one: the maintainers decide the fix.
Run the reproduction before sending, exactly as written; one that needs Massive's API with a key
is the owner's to run (`AGENTS.md`, Limits). Use short paragraphs, define every term, and leave
out this project's internal names. Have a Fable agent with no other context critique it for
clarity, and post nothing until the owner has approved the text.
