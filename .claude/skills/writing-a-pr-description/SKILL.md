---
name: writing-a-pr-description
description: "Use this skill whenever you write or rewrite a pull request description, an issue body, a report of wrong data to Massive, or a report another agent or the owner will read as a record of work: opening a PR, rewriting a description after a push, filing an issue, or when the owner calls a description rambly, padded, machine-sounding, or hard to scan. It picks the facts a reader needs, drafts them into the structure of docs/dev/pull-requests.md in an editor's prose, and runs that page's self-edit checklist. Not for a PR title alone (the Title section of the same page), review replies, or questions and status messages to the owner (writing-to-the-owner)."
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
- what is left, with its issue.

Then select. Every fact must earn its place by changing what the reader understands or decides.
A true fact the reader has no use for (an internal constant, a helper's name, a count of test
cases, a step you took to get there) is noise, and noise is how a description starts to read as
machine output. The rule is every fact the reader needs to understand or decide, and nothing
they don't. Leave out how you got there: attempts, review rounds,
re-runs, earlier versions of the branch.

Write only what you know. When a fact the reader needs is missing from your sources (what a
test covers, what the old behavior was), check it in the code or flag it to whoever asked;
never fill the gap with a plausible guess.

## 2. Draft

Copy the sections from `.github/pull_request_template.md` (`gh pr create --body-file` never
shows it), and end with `Closes #<issue>` and the attribution footer your session requires.
Write the Summary last, as two to four plain sentences that say what the PR does and why for a
reader with no context. A **Check before merging** list holds only what the owner has not
already settled. Use bullets and tables where the reader scans (What changes,
Decisions, Verification); use sentences where the reader follows reasoning (Summary, Why, How
it works).

Write the way a good engineer explains a change to a colleague: whole sentences, one idea each,
with the subject and verb up front. Telegraphic compression (stacked parentheticals, semicolon
chains, a list of six identifiers in one clause) is the main thing that makes a description
sound generated. If a sentence needs a parenthesis inside a parenthesis, split it. Link to a doc
page instead of repeating it.

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

## 4. Publish

- New PR: `gh pr create --title "<title>" --body-file <file>`.
- Existing PR: `gh pr edit <n> --body-file <file>`, in the same batch as the push it describes.
- Confirm the issue link: `gh pr view <n> --json closingIssuesReferences`.

## A report to Massive

A report of wrong vendor data (`AGENTS.md` rule 23) or any text for another project goes to a
maintainer who knows nothing of this library, and an unclear report is declined or ignored.
Lead with the problem they can see, in one or two plain sentences. Then give a minimal
reproduction (the exact request, or symbol and date), the expected and the actual result, the
evidence, and a proposed fix if there is one. Use short paragraphs, define every term, and leave
out this project's internal names. Have a Fable agent with no other context critique it for
clarity, and post nothing until the owner has approved the text.
