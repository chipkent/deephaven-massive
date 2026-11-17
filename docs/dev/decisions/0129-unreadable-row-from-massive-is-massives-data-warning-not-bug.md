# 129. A row Massive sent that the library can't read is Massive's data, not a library bug

Date: 2026-11-17

Status: accepted

## Question

A trader's `history()` from flat files meets a line that doesn't split into the header's fields,
a `poll()` cycle gets a row with a null key, or Massive's crypto ticker list gives a pair whose
two currencies don't make up its ticker. The library withholds the row, or fails the table under
`RAISE`. Whose problem does the message say it is, and at what level is it logged?

## Options

1. Massive's data: the message says so and names the `dm.ErrorPolicy` setting as the fix, with
   no bug-report sentence; the error's `source` is `MASSIVE`, `is_bug` is false, and the log
   level is WARNING.
2. The library's defect: the message keeps the bug-report sentence and drops the setting; the
   error stays `LIBRARY`, a bug, logged at ERROR.

## Ruling

Option 1, decided by the owner in the coordinating chat session on 2026-11-17. A row Massive
sent that the library can't read is Massive's data. In a follow-up that day, the owner agreed
that malformed flat-file lines log at WARNING, like the other causes that skip one value.

## Reason

The owner asked which level suits the trader using the library, and accepted WARNING, the level
of the other causes that skip one value.

## Consequences

A handler that branched on `ErrorSource.LIBRARY` or `is_bug` to catch these rows sees `MASSIVE`
and false instead, a log watched for ERROR no longer shows them, and `m.problems()` gives them
Source `massive`. A change in Massive's format that leaves every row unreadable still fails the
read or stream, but no longer asks for a bug report. This supersedes the statement in decision
99 that such a failure is a library defect.

## Rule

The `RowDataError` docstring, and `docs/troubleshooting.md`, "Read the problems table".
