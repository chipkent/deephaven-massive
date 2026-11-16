---
name: writing-a-record
description: "Use this skill whenever you add or change a file in docs/dev/decisions/ or docs/dev/verification-items/: recording an owner ruling, superseding a decision, adding, settling, or retiring a verification item. That includes 'record this decision', 'add a verification item', 'write this ruling down', and a live test that needs an item number. It gives what a record holds, how its ruling is stated without quoting the owner, what it leaves out, and when not to write one. Not for the PR body's Decisions table (writing-a-pr-description), for asking the owner (writing-to-the-owner), or for where the rule itself lives (AGENTS.md, Workflow)."
---

# Writing a record

A record exists so the next agent neither blindly keeps nor blindly reverses a choice. The rule
itself lives in `AGENTS.md`, a user doc or docstring, or the `docs/dev/` page on its subject;
the record is its evidence and links nowhere else. Each folder's `README.md` has the format,
and `tests/test_dev_records.py` checks it.

## When not to write one

- A fact about Python, Deephaven, or Massive: a doc sentence, or a verification item if only a
  live read settles it.
- Your own choice: the PR body's Decisions table.
- A private name or layout, or what a pull request did: the PR body.
- A rule already recorded, with no new owner ruling behind it: link the record that has it.
- A verification item whose answer would leave the code as it is: record the answer in the test
  that reads it.

## A decision record

At most 300 words under the headings, in the README's sections. The title is the choice in
plain words a reader can agree or disagree with, under 90 characters, with no code.

- **Question**: the case, concrete: a call a user makes and what they see.
- **Options**: the options that were weighed, one line each, with what the user sees and what
  it costs. Never invent a loser: if one path was proposed, write "None recorded: one path was
  proposed."
- **Ruling**: the owner's ruling in plain words, with its date and where it was decided (the
  chat session, an issue, or a pull request). No quotation: a quote read later without its
  question is stretched or misread (decision 120). State what the owner decided, no wider and
  no narrower than the question they answered. Read the wording back to the owner before you
  write the file (a subagent, through the agent that started it).
- **Reason**: why, as the owner or the evidence gave it. If no reason was given, say so; never
  supply one.
- **Consequences**: what gets worse or is given up, and what a user must now do.
- **Rule**: one line, where the rule lives. Never restate the rule there.

State a principle generally, with its reason, never as the list of cases it was met in. The
cases go in the Question as examples.

## Changing a decision

A changed ruling is a new record. The old record's `Status:` line names it (`superseded by`,
or `partly superseded: <the part> by`), and nothing else in the old file changes. A record
whose ruling now lives entirely in another record is `absorbed by [N](...)`; one whose rule
lives only on its subject's page is `moved to <page, section>`.

## A verification item

The title is the question, ending in a question mark. The body says what the library does
under each answer, what it does until the item is read, and which test reads it, with the plan
it needs. Settling it changes its `Status:` line to the answer, with the date; the body keeps the
evidence. Retiring it says why on the `Status:` line.

## Before pushing

Read the record as the next agent: from this file alone, can they tell what was chosen, what
was not, who decided and when, and where the rule lives? Then
`uv run pytest tests/test_dev_records.py -q`, and show each new record's text in the pull
request description.
