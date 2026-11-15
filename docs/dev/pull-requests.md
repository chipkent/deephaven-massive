# Pull request titles and descriptions

**A PR description gives the owner what it takes to decide whether to merge, and nothing
else**: what changes for a user, what the owner approved and where, the merge order, what
was checked and what wasn't, and what is left. A reader skimming the headings, the first
words of each bullet, and the table rows gets all five. It reads as an O'Reilly editor's
work: [How an editor writes](user-docs.md#method) applies, and its budget is under about
600 words. A description that can't fit holds facts the owner doesn't need, or describes two
PRs.

The owner reads every description to decide whether to merge; a reviewer with no context and a
user reading months later must both understand it. Issue bodies and agents' reports meet the
same bar, with the same banned patterns and checklist.

`.github/pull_request_template.md` holds the skeleton; the `writing-a-pr-description` skill
walks through a draft.

## Title

The title is the line most readers ever see: in `git log`, the release notes, and the PR list.
The owner squash-merges, so it also becomes the commit subject on `main`, and these rules apply
to every commit subject.

- **Type, then the imperative.** A Conventional Commits type (`feat:`, `fix:`, `docs:`,
  `test:`, `refactor:`, `ci:`, `chore:`), then a lowercase verb as an instruction: "add",
  "stop", not "adds" or a noun phrase. No trailing period. The only scope is `chore(deps):`.
- **The type follows what changes.** Library code with no behavior change is `refactor:`; only
  tests is `test:`, only CI `ci:`, only docs `docs:`, only tooling `chore:`.
- **Short.** Aim for 50 characters, never more than 60, not counting the ` (#123)` GitHub adds.
- **Specific.** A reader knows which behavior changed without opening the PR. "update",
  "improve", and "handle" need a concrete object.
- **One idea.** No comma chains, no "and" or "so" joining two actions.
- **The effect, not the technique.** "stop a failed live table from crashing the server", not
  "deliver a stream failure once".
- **Plain words.** No internal names or jargon, except public API a user calls (`history()`).
- **Current.** Retitle when the scope changes: `gh pr edit <n> --title`.

CI checks what a program can (the type, the scope, the lowercase start, the period, the length)
and comments on the PR when a rule breaks. Dependabot's own pull requests are exempt from the
length rule and may start the summary with a capital letter, since Dependabot sets both. Run it
first:

```bash
uv run python scripts/check_pr_title.py "fix: stop a failed live table from crashing the server"
```

| Before | After |
|---|---|
| `fix: deliver a stream failure once, so a released table cannot exit the process` | `fix: stop a failed live table from crashing the server` |
| `feat: each read gives up on its own, reference data has its own limit, learned limits end with the key's sessions` | `feat: stop one busy session from failing another's calls` |
| `fix: refuse parallel runs from the tests they select, not the -m text` | `test: never run live tests in parallel` |
| `feat: the wire clients log each exchange and the rows they cannot read` | `feat: log each request to Massive and its reply` |

## Description

### Shape

Prefer scannable structure: short headings, bullets, numbered steps, and tables. Each bullet is
one idea, short, and parallel to its neighbors. Use a paragraph only where prose reads better:
the Summary, or one decision that needs a sentence of reasoning. No walls of text.

### Sections

| Section | Content |
|---|---|
| Summary | What the PR does and why, in two to four sentences, for a reader with no context; then, if needed, **Check before merging**: at most seven one-line items. An approval is the owner's date and a quote of fifteen words or fewer, plus the question it answers, in full. No file or internal names, no history of the branch, no table of differences from `main`. |
| Why | The problem, with a concrete example a user would hit. |
| What changes | One bullet per user-visible change, with before and after wherever the API or a result changes. |
| Decisions | Each choice the PR makes or applies, and each departure from the plan, with its reason. A table works well. "None" when there are none. |
| How it works | The approach a reviewer needs, by idea, never file by file. |
| Verification | Each check and its result; what is not yet run, and how to run it. |
| Follow-ups | Work left out, with its issue or reason. "None" when there is none. |

In the Summary and Why, assume the reviewer knows nothing about why this PR exists. Give the
context and terms they need to follow the change, and no more; don't explain code the change
doesn't depend on.

Then `Closes #<issue>` on its own line, then the required footer.

### Banned patterns

| Pattern | Bad | Good |
|---|---|---|
| Process narration | "The first version released on a tag push; after review it was changed to a manual run." | "A release starts by hand, never from a tag push." |
| First person | "I added a dry run, then re-ran the tests." | "A dry run shows the notes without releasing." |
| File-by-file tour | "`release.yml` adds four jobs; `ci.yml` gains `workflow_call`; `cliff.toml` groups titles." | "A release reuses the CI workflow, so it runs every job a PR runs." |
| Hedging | "This should probably fix most of the flakiness." | "The reconnect test no longer races." |
| Filler openers | "Note that the dry run does not tag." | "The dry run does not tag." |
| Duplicated lists | A Summary that restates every bullet of What changes. | The Summary names the change once; What changes lists the details once. |
| Internal jargon | "The governor's reference lane now has its own bucket." | "Reference-data calls now have their own rate limit." |
| Unexplained acronyms | "Publishes via OIDC TP." | "Publishes with PyPI's trusted publishing: a short-lived token GitHub signs for the job." |
| Bold-label bullet walls | "- **One run does everything (owner review).** The first version…", then 80 more words | A table row: "The workflow tags the commit itself \| No tag can point at untested code" |
| Restating the docs | The setup steps from the releasing page, pasted in. | "Steps: [releasing page](…)." |
| Chained sentences | "It reads the version, refuses if malformed (a version is …), runs the tests, and …" | Separate sentences, or a numbered list. |
| Undecodable references | Review labels (H1, M2), agent names, decision numbers, counts that will go stale. | The fact itself, in words. |
| Branch history | "The earlier form of this PR removed the grammar; the owner's rulings restore each." | The description describes the branch as it is; what an earlier push did is not a fact about it. |
| Quote walls | Three sentences of the owner's words under one bullet, repeated in a table below. | One quote per approval, fifteen words or fewer, once; a question it answers is quoted in full. |
| Case stacking | "A stream raises at the call, and a `fetch()` fails its table, or raises with `wait=True`, unless no pair split." | One case per sentence, or a before-and-after table. |

A bold label on a one-line bullet (`- **Dry run:** shows the notes without releasing.`) is
fine. A bold label heading a paragraph is a wall in disguise.

### A good Summary

> This PR makes a release one manual step. A GitHub Actions workflow, started by hand on
> `main`, runs the full test suite; only if every job passes does it tag the tested commit and
> publish a GitHub Release with the built package and generated release notes. Today testers
> install from a git tag that nothing checks, so a tag can point at untested code.
>
> **Check before merging:**
>
> - The note headings: Features, Fixes, Docs, Tests, Refactors, Chores, and Other.

A decision only the owner can make goes to the owner, in full, before the pull request opens
(`writing-to-the-owner`; a subagent asks through its coordinator); the description is not a
way to ask, and a question in it means the work was not ready. Record the answer under
Decisions once given.

## Keep it current

The description describes the branch as it is. When a push changes the PR, rewrite the
description to the final state in the same batch (`gh pr edit <n> --body-file <file>`); never
append to it.

## Close the issue

`Closes #<issue>` on its own line closes the issue on merge. `Refs #<issue>` marks a partial
delivery and names what remains; "No tracking issue." says there is none. Never close an issue
by hand to make up for a missing link. Check the link before asking for review:

```bash
gh pr view <n> --json closingIssuesReferences
```

## Self-edit checklist

Run it on every draft, and before every push that changes the PR:

1. **Skim only the headings and the first words of each bullet: do you have every fact?** What
   changed, why, the decisions, how it was checked, what is left. Promote anything buried in a
   paragraph to a bullet or a row.
2. **Read it as an O'Reilly technical editor would, and fix what they'd mark** under
   [How an editor writes](user-docs.md#method) and [Sentences](user-docs.md#sentences).
3. **Read the first three sentences alone.** A reader with no context can say what the PR does
   and why.
4. **A reviewer new to the PR knows why it exists and can follow every section without
   asking.**
5. **Only what the merge decision needs.** Delete every sentence that is padding, repetition,
   narration, branch history, or a detail the owner has no use for (a private name, a test
   count, an argument for a choice the owner already made). Over about 600 words, something
   here doesn't belong, or this is two PRs.
6. **Sweep for the banned patterns** above, starting with a grep for narration and hedges, and
   fix every hit that is one:

   ```bash
   grep -nE '\b(I|we|We)\b|[Nn]ote that|should probably|might|[Ff]irst version|then re-?ran|earlier form|restored|back to .main.|, or [a-z]+ when|, unless' <file>
   ```

7. **Check the frame**: the sections in order, "None" in an empty Decisions or Follow-ups, a
   title that meets the rules, text that describes the branch after this push, and the issue in
   `closingIssuesReferences`.
8. **Run the description check**: `uv run python scripts/check_pr_body.py` on the draft at
   `.git-pr-body.md`. It fails on an item under Check before merging that asks the owner
   anything, on an approval claimed without the owner's dated quote, and on a rule change
   without its record; CI runs it on the published description.
