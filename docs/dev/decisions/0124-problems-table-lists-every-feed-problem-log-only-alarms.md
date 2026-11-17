# 124. Every problem with the feed is a row of the problems table; the log only alarms

Date: 2026-11-16

Status: accepted

## Question

A trader's live table loses rows Massive sent that the library cannot read, or its connection
drops, or a watchlist value is not Massive's ticker. Where does the user see each problem, and
how loudly does the log report it?

## Options

1. Keep the reports where they were: running notes in `m.streams()` `Error`, its `Gaps` and
   `SchemaDrift`, `m.connections()` `SchemaDrift`, and a log line per distinct value. No one
   place can be queried.
2. One live keyed table, `m.problems()`, that holds every problem of every table, with one
   WARNING per table and cause pointing at it.

## Ruling

Option 2, decided by the owner in the coordinating chat session: the design approved on
2026-11-14 and 2026-11-15, and this wording on 2026-11-16. Every problem with the feed is a row
of `m.problems()`, and the log is only its alarm. The table has one row per table, cause,
symbol, field, and start time. A lasting problem gets an end time when it clears. Its columns
never depend on a setting. Only a real failure is a problem: valid data, such as a null Massive
sent, never is. The log warns once per table and cause, with details at DEBUG, and the default
setting stays `LOG`. A named symbol whose last 100 rows were all withheld is marked stale, and
its stream keeps running instead of failing. `m.streams()` loses its `Gaps` and `SchemaDrift`
columns, and `m.connections()` loses `SchemaDrift`.

## Reason

The owner wants one clear place to see problems with the feed, in a Deephaven table a user can
query, and no log noise for problems the table already holds.

## Consequences

A user who read `Error` notes, `Gaps`, or `SchemaDrift` reads `m.problems()` instead. A stream
of named symbols keeps running while one of its symbols stays unreadable.

## Rule

`AGENTS.md`, "Every problem is a row of `m.problems()`, and only a real one" and "The log is
an alarm".
