# 100. Fetch and history return at once and fill in the background

Date: 2026-11-11

Status: accepted

## Question

Should `fetch()` and `history()` block until the data arrives, or return a table at once that
fills in the background?

## Options

1. Block by default, with `wait=False` to opt into a background read (decision 68).
2. Read in the background by default, with `wait=True` to block.

## Ruling

By default, `wait=False`, `q.fetch()` and `q.history(start, end)` return an empty refreshing
table at once; the whole result lands in one update, and a later refusal from Massive fails the
table, which `m.streams()` shows as `FAILED`. With `wait=True` the call blocks, a refusal from
Massive raises, and a fixed symbol list gives a static table; a live symbol table follows
decision 82. The owner ruled on 2026-11-11 that `wait=False` is the default; recorded in PR
#588. This absorbs decision 68.

## Reason

Waiting blocks every download and makes startup slow. A Deephaven table cannot turn static
later, so the default result stays refreshing though it never changes again.

## Consequences

An argument error or a closed session still raises at the call; what Massive answers later
fails the table, logged and shown in `m.streams()` `Error`. A script that reads rows on the
next line gets none, unless it passes `wait=True`. A `try`/`except` around a default call no
longer catches Massive's answers. Every background read runs on the session's read pool of
`max_concurrency` threads.

## Rule

The `fetch()` and `history()` docstrings, `wait`.
