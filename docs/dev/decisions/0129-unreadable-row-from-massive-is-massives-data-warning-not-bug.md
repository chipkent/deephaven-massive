# 129. A row Massive sent that the library can't read is Massive's data, not a library bug

Date: 2026-11-17

Status: accepted

## Question

A trader's `history()` from flat files meets a line that doesn't split into the header's fields,
a `poll()` cycle gets a row with a null key, or Massive's crypto ticker list gives a pair whose
two currencies don't make up its ticker. The library withholds the row, or fails the table under
`RAISE`. Whose problem does the message say it is, and at what level is it logged?

## Options

1. Massive's data: the message names the `dm.ErrorPolicy` setting, with no bug-report sentence;
   `source` is `MASSIVE`, `is_bug` false, and the level WARNING.
2. The library's defect: the message keeps the bug-report sentence; `LIBRARY`, a bug, ERROR.

## Ruling

Option 1, decided by the owner in the coordinating chat session on 2026-11-17, for the three
examples above, logged at WARNING. Under the owner's standing rule that a case unlike its
siblings is made to match them, the ruling also covers every other cause of a withheld row on
every transport, a flat file whose every line is malformed, and a WebSocket frame that isn't
JSON. The owner confirmed this scope when the record was read back on 2026-11-17.

## Reason

The owner asked which level suits the trader using the library, and accepted WARNING, the level
of the other causes that skip one value.

## Consequences

These errors have `source` `MASSIVE` and `is_bug` false, log at WARNING, and show Source
`massive` in `m.problems()`, whose fix names the setting. A read or stream whose every row is
withheld still fails, but asks for no bug report. This supersedes the statement in decision 99
that such a failure is a library defect.

## Rule

The `RowDataError` docstring, the `WireFailure.UNREADABLE_ROWS` docstring, and
`docs/troubleshooting.md`, "Read the problems table".
