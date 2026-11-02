---
name: writing-a-pr-description
description: "Use this skill whenever you write or rewrite a pull request description or an issue body: opening a PR, rewriting a description after a push, or when the owner calls a description too long, rambly, or hard to read. It drafts the text from the facts, fits it to the section budgets in docs/dev/pull-requests.md, and runs that page's self-edit checklist before the text goes to GitHub. Not for PR titles alone (the Title section of the same page covers those), not for review replies, and not for messages to the owner (the writing-to-the-owner skill covers those)."
---

# Writing a PR description

The standard is `docs/dev/pull-requests.md`; read it first. Its goal: a reader skimming only
the headings and the first words of each bullet gets every fact that matters. The owner has
rejected long, narrated descriptions as "rambly" and "agent slop"; this skill exists to stop
them. An issue body meets the same bar, with the sections it needs.

## 1. Gather the facts, not the story

From the diff (`git diff origin/main...HEAD`), the issue, and your notes, list in a scratch
file:

- what a user or developer can do or sees differently after the merge;
- the problem it solves, with one concrete example;
- each choice the PR makes, with its reason;
- each check run and its result, and anything not yet run;
- what is left, with its issue.

Leave out how you got there: attempts, review rounds, re-runs, earlier versions of the branch.

## 2. Draft into the skeleton

Copy the sections from `.github/pull_request_template.md` and fill each from the list, within
its budget. Write the Summary last, in 2–4 sentences. Use bullets and tables; keep a paragraph
only where prose reads better. Link to a doc page instead of repeating it.

## 3. Edit

Run the self-edit checklist at the end of `docs/dev/pull-requests.md`, every item, and fix the
draft until each passes. Then:

```bash
wc -w <file>   # about 350 for an ordinary PR, 650 for a large one
grep -nE '\b(I|we|We)\b|[Nn]ote that|should probably|might|[Ff]irst version|then re-?ran' <file>
```

## 4. Publish

- New PR: `gh pr create --title "<title>" --body-file <file>`.
- Existing PR: `gh pr edit <n> --body-file <file>`, in the same batch as the push it describes.
- Then `gh pr view <n> --json closingIssuesReferences` shows the issue.
