# Decisions

The owner's decisions behind `deephaven-massive`, one file each: the decision, its reason, and
its status. `README.md` and the public docstrings are the user contract; these files record why
the contract is what it is.

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
# N. <title>

Date: YYYY-MM-DD

Status: <status>

<body: the decision and its reason, in whole sentences>
```

`Status:` is one of:

- `accepted`: the decision holds as written.
- `superseded by [M](<M's file>)`: decision M replaces all of it; read M instead.
- `partly superseded: <the part> by [M](<M's file>)`: decision M replaces the part named; the
  rest holds.

## The slug

The slug tells a reader whether to open the file, in the words they would search for: the
setting, behavior, column, or method the decision governs
(`0097-websocket-connection-taken-max-connections-fail-or-reconnect.md`, not `0097-connections.md`).
It has at least three words, and generic words (`update`, `misc`, `change`, `fix`, `decision`,
`item`) don't count toward them. `tests/test_dev_records.py` checks this, that each number is
used once and matches the file's heading, that every file has a `Status:` line, and that every
decision a status names exists.

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
