---
name: writing-user-docs
description: "Use this skill whenever you write, rewrite, edit, restructure, or review any text a user of deephaven-massive reads: README.md (its hook, Install, Configuration, examples, next steps), docs/troubleshooting.md, docs/tester-guide.md, or the hand-written text of docs/datasets/ (its intro and the shared argument rules in _datasets/argument_rules.py). Use it for a whole page or one sentence, for requests like 'write the Install section', 'improve the hook', 'add an example', 'make the README sell', 'fix this paragraph', 'this reads like AI', or 'grade this page', and whenever a code change alters something a user page describes (a default, a limit, an error, a column, an install step), even if the request never mentions docs. It researches the readers and their pains, gets the owner's sign-off on the outline, writes to docs/dev/user-docs.md, runs the checker, and grades the draft with the calibrated editor in agents/editor.md until it passes. Not for docstrings (writing-docstrings), PR descriptions or issue bodies (writing-a-pr-description), or messages to the owner (writing-to-the-owner)."
---

# Writing user docs

The owner scored a README 4 out of 10 that our own editors had scored 8. Its sentences were
mostly fine. It failed as a whole: a hook that sold nothing in 20 seconds, invented durations, a
command that failed in a new terminal, sections nobody asked for, coined words, examples that
narrated code instead of teaching, and asides in nearly every paragraph. Every reader came away
thinking the library was harder than it is.

[`docs/dev/user-docs.md`](../../../docs/dev/user-docs.md) is the standard, and
[`agents/editor.md`](agents/editor.md) is the grader that enforces it. Read the standard whole
before your first draft in a session; it's short.

## Workflow

### 1. Research the readers and the facts

Start from the standard's "This project" section: the four readers, their job-level pains, the
owner's requirements, and the page map. For a page or section of a kind you haven't written
before, find how the best projects do it (the standard cites its sources) before you invent a
shape.

Then gather every fact the text will state, each with its source: `src/deephaven_massive/`,
`docs/datasets/`, `vendor/massive/`, or Deephaven's docs. Keep the list as you go, claim by
claim. A fact you can't source doesn't go in. Run each command you'll print, or trace each part
of it, from the state the previous step leaves the reader in.

### 2. Outline, and stop for the owner

For a new page, a new section, or a restructure, write the outline first: the headings, one line
on what each section delivers, and for examples the progression table (title, goal, the one new
idea, plan). Check it against the owner's list of sections and the page map, then send it to
the owner (or the coordinator who spawned you) and wait for sign-off before you write prose.
Restructuring prose that's already written is where the cost is. An edit inside an approved
outline skips this step, and so does a task whose request already fixes the outline.

### 3. Write

Write to the standard. These are the faults that sank the last README; check for each as you
go:

- The hook: who, what, and outcome in the first sentence; the reader's job-level pain; about 90
  words before the sample or call to action; no asides or caveats.
- Numbers: none without a source. No setup times, memory minimums, or "about a minute".
- Commands: each runs as written where the last step left the reader. A new terminal loses the
  activated environment.
- Steps: nothing needless. No variable that never changes, no key set in a second place, no
  "do A", a digression, then "or B".
- Placement: settings under the object they belong to (`MassiveConfig`); environment variables
  described by what the reader gets.
- Examples: the goal before the code, one new idea each, what to notice after it, never a
  narration of the code.
- Asides: delete each appositive and parenthetical and see whether the reader misses it.

### 4. Run the checker

```bash
uv run python .claude/skills/writing-user-docs/scripts/check_user_doc.py <file>
```

Fix every FAIL. Judge every WARN: it's a hint, and some are fine (a numbered install step, a
sourced "15 minutes late"). The checker can't see whether the page works as a whole; the
editor does that.

### 5. Grade with the editor, and revise

Spawn a fresh subagent, which hasn't seen your draft being written, with this brief:

```text
Read <repo>/.claude/skills/writing-user-docs/agents/editor.md and follow it exactly. Grade
<file> (scope: <the whole page | the hook | Install | ...>), with <repo> as the repository
root for sources. Write the score sheet to <path> and return it.
```

Revise from the sheet. Fix each fault's pattern everywhere it occurs, not only the quoted line,
and rewrite a paragraph with several faults rather than patching it. Answer every reader's "no"
on the simplicity question: that verdict alone fails the page. Then grade again with a new
subagent, giving it the previous sheet to check against.

Stop after two revision rounds (three reads). A draft still short of the pass bar goes to the
owner with the last sheet and a plain list of what's missing. Never call a failing draft done.
If you can't spawn a subagent, grade it yourself with `editor.md` and say the grade is your own.

### 6. Check the facts one last time

Compare your claim list with the editor's claims table. Every UNSOURCED or FALSE claim is cut or
sourced, and every command marked FAILS is fixed. For `README.md` or `docs/troubleshooting.md`,
run `uv run pytest tests/test_docs.py -q`. If you moved an anchor, rewrite its citations in
`src/` docstrings and on the other pages.

### 7. Show the owner

Hand over the page with the final score sheet, the list of facts only a run with a Massive key
can confirm (for the owner's test plan), and anything the editor flagged that you chose to keep,
with why.

## Evals

`evals/` holds the calibration cases: the 4/10 README must fail and surface the owner's
complaints, a known-good first screen must pass, and writing cases exercise the workflow. When
you change the standard or the editor, rerun the two calibration cases before trusting a grade.
