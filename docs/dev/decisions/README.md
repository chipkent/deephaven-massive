# Decisions

The owner's rulings behind `deephaven-massive`, one file each: the question, the ruling, its
reason, and its status. The rules themselves live in `AGENTS.md`, the user docs and docstrings,
and the `docs/dev/` page on their subject; a record is the evidence behind a rule. The
`writing-a-record` skill says when to write one and how.

## Find a decision

List every decision with its status, one line each:

```bash
grep -m1 '^Status:' docs/dev/decisions/0*.md
```

The file names say what each decision is about. Read every entry whose name touches your
subject before you cite a decision or ask the owner something: a later entry may have changed
an earlier one, and its `Status:` line says so.

## Format

Each decision is `NNNN-<slug>.md`: its number, zero-padded to four digits, then a slug.

```markdown
# N. <the choice, in plain words, under 90 characters, with no code>

Date: YYYY-MM-DD

Status: <status>

## Question

<the case, concrete: a call a user makes and what they see>

## Options

<the options weighed, one line each, or "None recorded: one path was proposed.">

## Ruling

<the owner's ruling in plain words, with its date and where it was decided>

## Reason

<why, as the owner or the evidence gave it>

## Consequences

<what gets worse or is given up, and what a user must now do>

## Rule

<one line: where the rule lives>
```

The body is at most 300 words. It states the ruling with no quotation: a quotation read later
without its question is stretched or misread. Read the wording back to the owner before writing
the file, and show its text in the pull request (decision 120).

`Status:` is one of:

- `accepted`: the decision holds as written.
- `superseded by [M](<M's file>)`: decision M replaces all of it; read M instead.
- `partly superseded: <the part> by [M](<M's file>)`: decision M replaces the part named; the
  rest holds.
- `absorbed by [M](<M's file>)`: decision M now holds this ruling; the file is a pointer.
- `moved to <page, section>`: the rule lives on that page, and the file is a pointer.

A pointer file has only its heading, `Date:`, and `Status:` lines. A `moved to` pointer names
each place its rule now lives, as a path in backticks with the section or phrase there in double
quotes, joined by `; `. A record superseded whole may also be a pointer.

## The slug

The slug tells a reader whether to open the file, in the words they would search for: the
setting, behavior, column, or method the decision governs
(`0097-websocket-connection-taken-max-connections-fail-or-reconnect.md`, not `0097-connections.md`).
It has at least three words, and generic words (`update`, `misc`, `change`, `fix`, `decision`,
`item`) don't count toward them. `tests/test_dev_records.py` checks this, that each number is
used once and matches the file's heading, that every file has a `Status:` line, that every
decision a status names exists, and that a record has the sections above within its limits.

## Adding and changing a decision

A new decision takes the number the coordinator reserved, or else the next after the highest on
`main` and in every open pull request. Two pull requests that take the same number at once are
caught on `main` by the test, and a follow-up pull request renumbers one of them.

Never rename or delete an entry, except to renumber one of two that landed with the same
number. A changed decision is a new file, and the old file's `Status:` line names it; nothing
else in the old file changes.

## History

Decisions 1 to 94 are condensed from the design documents these files replaced. Their full text
is in git history: `git show 5bbf017c^:plans/rework/design/22-decisions.md`. Where an entry
carried no date, its `Date:` line is the day its text first appeared in those documents.
Numbers 88 and 89 were never assigned. Until 2026-11-13 all decisions were one file,
`docs/dev/decisions.md`.

On 2026-11-15 the records were rewritten in the form above, as the owner ruled (decision 120):
the records that hold a ruling were restated in plain words, and the rest became pointers to the
record or page that now holds their rule. The owner confirmed each restated ruling on
2026-11-16. Their earlier text is in git history:
`git show f902fcf1:docs/dev/decisions/<file>`. Decisions 58, 66, 81, 90, 93, 99, 103, and 107 keep
their earlier form until the pull request that changes the symbol rules lands.
