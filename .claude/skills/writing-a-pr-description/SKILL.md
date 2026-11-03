---
name: writing-a-pr-description
description: "Use this skill whenever you write or rewrite a pull request description or an issue body: opening a PR, rewriting a description after a push, or when the owner calls a description rambly, padded, or hard to scan. It drafts the text from the facts, in the scannable structure docs/dev/pull-requests.md sets, and runs that page's self-edit checklist before the text goes to GitHub. Not for PR titles alone (the Title section of the same page covers those), not for review replies, and not for messages to the owner (the writing-to-the-owner skill covers those)."
---

# Writing a PR description

The standard is `docs/dev/pull-requests.md`; read it first. Its goal: a reader skimming only
the headings and the first words of each bullet gets every fact that matters. The owner has
rejected padded, narrated descriptions as "rambly" and "agent slop"; this skill exists to stop
them. Length is not the goal: as long as the content needs and no longer. An issue body meets
the same bar, with the sections it needs.

## 1. Gather the facts, not the story

From the diff (`git diff origin/main...HEAD`), the issue, and your notes, list in a scratch
file:

- what a user or developer can do or sees differently after the merge;
- why it exists: what prompted it and the problem it solves, with one concrete example, plus
  any term a reviewer new to the PR needs to follow it;
- each choice the PR makes, with its reason;
- each check run and its result, and anything not yet run;
- what is left, with its issue.

Leave out how you got there: attempts, review rounds, re-runs, earlier versions of the branch.

## 2. Draft into the skeleton

Copy the sections from `.github/pull_request_template.md` and fill each from the list. Write
the Summary last, so its first sentences say what the PR does and why. Use bullets and tables;
keep a paragraph only where prose reads better. Link to a doc page instead of repeating it.

## 3. Edit

Run the self-edit checklist at the end of `docs/dev/pull-requests.md`, every item, and fix the
draft until each passes. Its core is item 2: read the draft as an O'Reilly technical editor
would, and fix what they'd mark. Sweep too for the banned patterns of
`docs/dev/user-docs.md` (meta-commentary, filler negation, coined jargon, precision theater,
so-chains, hype words, the tricolon formula, colon reveals): they read as machine-written in a
PR as much as on a user page. Its narration and hedge grep:

```bash
grep -nE '\b(I|we|We)\b|[Nn]ote that|should probably|might|[Ff]irst version|then re-?ran' <file>
```

## 4. Publish

- New PR: `gh pr create --title "<title>" --body-file <file>`.
- Existing PR: `gh pr edit <n> --body-file <file>`, in the same batch as the push it describes.
- Then `gh pr view <n> --json closingIssuesReferences` shows the issue.
