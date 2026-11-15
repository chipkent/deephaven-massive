# 35. Data that differs from Massive's description is served where it fits and reported

Date: 2026-10-30

Status: accepted

## Question

When Massive's data differs from its specification at run time (an extra field, a value that
does not fit its column, a row whose key cannot be read), what does the table do?

## Options

1. Fail the table on any difference.
2. Serve what is right and report the rest.

## Ruling

A value that does not fit its column becomes null, with one WARNING per dataset and field. An
unreadable flat-file line follows `MassiveConfig.malformed_rows`: `LOG`, the default, drops it
and logs it once with a count; `IGNORE` drops it with no log line; `RAISE` fails the read. An
extra field is dropped and logged. A REST response or flat file with no convertible row fails
its read; the record does not say what happens to a WebSocket message with no convertible row.
The owner ruled on 2026-10-30 that the choices are to fail, to log and go on, or to go on
silently, with logging the default; recorded in PR #514.

## Reason

One odd value must not take down a live table, and nothing is hidden by default.

## Consequences

A column can hold nulls Massive did not send as nulls; the WARNING and the counts say where.

## Rule

`AGENTS.md`, "Failure policy: per value in the config, per request on the call" and "Supply
Massive's data; never repair it".
