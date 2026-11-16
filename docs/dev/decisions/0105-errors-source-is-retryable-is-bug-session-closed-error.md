# 105. Every error says where it came from, whether to retry, and whether it is a bug

Date: 2026-11-12

Status: accepted

## Question

`MassiveError.cause` collided with Python's `__cause__`, and one value mixed two questions:
whose move it is, and whether to log at ERROR. What should an error tell a handler?

## Options

None recorded beyond the design: one path was proposed and approved.

## Ruling

A `MassiveError` carries `source`, one of `MASSIVE`, `NETWORK`, `USER`, or `LIBRARY`, where
`USER` means only the user's code, settings, or environment can fix it; `is_retryable`, true for
HTTP 429, 500, 502, 503, and 504, for a timeout or dropped connection with no response, and for
page two or later of a paged read answering 404, since a new call gets a new cursor; and
`is_bug`, true for every `LIBRARY` error and the only case logged at ERROR. A call on a closed
session raises `SessionClosedError`. The owner ruled on 2026-11-12 for a four-valued source and
the two flags, and approved the errors design; recorded in PR #614. This absorbs decision 109: a
broken guarantee is a `LIBRARY` `MassiveError`.

## Reason

A handler can decide what to do without matching status codes or message text.

## Consequences

One failure has one source on every transport, and more sources may come, so handlers keep a
default case. Every `LIBRARY` message ends with how to report it, and every `USER` message
names its fix.

## Rule

`docs/troubleshooting.md`, "Handle errors in code", and the `MassiveError` docstring.
