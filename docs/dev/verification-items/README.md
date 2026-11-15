# Verification items

Facts about Massive (and two about Deephaven) that the documentation does not settle, one file
each. Each one gets the conservative handling until a live check settles it. Live tests name the
items they answer with `@pytest.mark.verifies("N")` (the test asserts the answer the library
assumes) or `@pytest.mark.settles("N")` (the test records evidence that a person reads), and
the live-run report lists items by number. `tests/live/test_summary.py` fails when a marker
names a number no file here has. [`docs/live-tests.md`](../../live-tests.md) says how the live
tests run.

## Find an item

List every item with its status, one line each:

```bash
grep -m1 '^Status:' docs/dev/verification-items/0*.md
```

## Format

Each item is `NNNN-<slug>.md`: its number, zero-padded to four digits, then a slug.

```markdown
# N. <the question, or for an older settled item the fact it settled>

Date: YYYY-MM-DD

Status: <status>

<body: three short paragraphs. "Unknown:" what is unverified. "Meanwhile" what the library
does until it is settled. "Settled by" or "Verified by" the test, its file, and what it
records>
```

`Status:` is one of:

- `open`: the question stands, and the body's handling applies.
- `settled YYYY-MM-DD: <the answer, in one line>`: a live check settled it; the body keeps the
  evidence.
- `retired`: the question no longer matters, and the body or the title says why.

## The slug

The slug tells a reader whether to open the file, in the words they would search for: the
dataset, field, channel, or behavior the item is about
(`0016-missing-flat-file-key-404-nosuchkey.md`). It has at least three words, and generic words
(`update`, `misc`, `change`, `fix`, `decision`, `item`) don't count toward them.
`tests/test_dev_records.py` checks this, that each number is used once and matches the file's
heading, and that every file has a `Status:` line.

## Adding and settling an item

A new item takes the number the coordinator reserved, or else the next after the highest on
`main` and in every open pull request. Two pull requests that take the same number at once are
caught on `main` by the test, and a follow-up pull request renumbers one of them. Otherwise
numbers are permanent: never rename or delete an item. Settling an item changes its `Status:`
line, and may add the evidence to its body.

## History

Items 1 to 81 are condensed from the design documents these files replaced. Their full evidence
is in git history: `git show 5bbf017c^:plans/rework/design/23-open-verification-items.md`. Each
`Date:` line is the day the item first appeared in those documents or in
`docs/dev/verification-items.md`, which held every item until 2026-11-13.
