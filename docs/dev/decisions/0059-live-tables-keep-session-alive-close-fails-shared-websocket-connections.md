# 59. A live table keeps its session alive, and closing the session fails it

Date: 2026-10-28

Status: accepted

## Question

What keeps a live table running, and what happens to live tables when their session closes?

## Options

None recorded: one path was proposed.

## Ruling

While a live table is alive, a filled background `fetch()` result included, the connection and
session it needs are not collected, even if the user dropped `m`. An explicit `m.close()` fails
every live table of that session with a clear message, so no table looks live while receiving
nothing. The owner ruled on 2026-10-28 against surprise silent failures of any kind; recorded in
PR #514.

## Reason

Silent stalls are forbidden.

## Consequences

A user who closes a session sees its live tables fail, and must open a new session to stream
again.

## Rule

The `MassiveSession.close` docstring.
