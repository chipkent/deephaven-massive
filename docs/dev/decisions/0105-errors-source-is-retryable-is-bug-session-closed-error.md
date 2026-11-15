# 105. An error says what stopped the call, whether to retry it, and whether a bug caused it

Date: 2026-11-12

Status: accepted

Owner, approving the design: "I approve the rest of that design", and for the names, "Q1: errors
design is approved." `MassiveError.cause` and `dm.ErrorCause` are replaced by three attributes,
because the old `cause` collided with Python's `__cause__` and one value mixed two questions: whose
move it is, and whether to log at ERROR.

- `source`, a `dm.ErrorSource`: `MASSIVE` only where Massive answered with a refusal or an error (an
  HTTP error status from Massive or S3, a rejected key, a plan without the data, a WebSocket status
  message, another connection taking the WebSocket connection, a list of Massive's that lacks a
  pair); `NETWORK` when no answer came through the library's retries (a TLS handshake that failed
  for another reason than the certificate, and a proxy that could not reach the server, included),
  or a stream or `poll()` cycle got no data for `max_outage`; `USER` when the user's code or
  settings stopped it (a missing API key, delayed data `delayed_subscriptions` refuses, a closed
  session, a failing symbol table, a certificate, proxy credential, or AWS profile setting, a read
  the user stopped); `LIBRARY` for a bug, Massive data the library cannot read and a failure inside
  a wire client included. One failure has one source on every transport. More members may come, so
  handlers keep a default case.
- Every wire error carries exactly one `WireFailure`, which its client must choose (refused, no
  answer, local settings, stopped, unreadable, client defect), so no failure becomes Massive's
  answer by default. A stop the library chooses itself (its other requests after one fails, a failed
  poll or followed read) is never surfaced: the failure that caused it is.
- `is_retryable`: true for HTTP 429, 500, 502, 503, and 504, for no answer (`NETWORK`), and for two
  answers that mean try again: HTTP 404 on a later page of a paged REST read (`CutShortError`), and
  S3's `RequestTimeout` (HTTP 400) on a flat file. Both stay `MASSIVE`, since Massive or S3
  answered. The owner, on 2026-11-12: "Fable should research the cases and determine the proper
  behavior." The research found that a new call of a cut-short read starts over with a new cursor,
  as a followed read already does, and the message already says to retry; why the later page answers
  404 (a cursor's lifetime) is unverified, and item 117 records it. AWS documents `RequestTimeout`
  as a connection that stalled, and botocore's standard, adaptive, and legacy retry modes all count
  it transient. The wire error marks it (`means_retry`), so the public layer never matches on status
  400. False for every other status, authentication, entitlement, a closed session, and a taken
  connection, and always for a bug. A flat file not yet published and pages that disagree on
  `adjusted` are retryable too: neither is an HTTP status, and both messages say to retry. A read
  that fails because every symbol failed is retryable only when every symbol's error is, and raises
  the first bug among them when there is one. The library has already retried a server failure or a
  lost connection before it raises, and it raises a rate limit as soon as waiting it out would take
  the read past its patience (10 minutes of pauses, or 90 seconds for a console command).
- `is_bug`: true for every `LIBRARY` error and for a user's bug the library caught (a failing symbol
  table, a parameter a path does not take, a symbol table row that is not Massive's ticker, a
  session made outside a server). The library logs a failure at ERROR exactly when `is_bug` is true.

A call on a closed session, and a live table whose session closes, raise or fail with the new public
`dm.SessionClosedError` (source `USER`). A wrong argument type stays a `TypeError`, and a wrong
value a `ValueError`; any other exception escaping a query method is a bug, logged at ERROR with its
traceback and raised as a `MassiveError` (`LIBRARY`) chained to it, so every failure inside a query
method is a `MassiveError`. Every `LIBRARY` error's message ends with how to report it, and every
`USER` error's message names its fix: a closed session says to open a new `dm.MassiveSession()`, and
a stopped read says why it stopped and to call the method again.

A broken guarantee (`required` and `required_instance`, decision 109) is a `MassiveError` too, whose
source is `LIBRARY`, so it is a bug. This replaces decision 109's choice of an exception outside the
hierarchy, which would break this decision's promise that every failure inside a query method is a
`MassiveError`. Since a handler can no longer tell a bug by its class, every handler that absorbs
one symbol's or one cycle's failure logs a bug at ERROR with its traceback (`log_failure` attaches
the traceback itself), and a handler that drops a failure quietly (a futures watchlist change that
meets a closing session) catches only `SessionClosedError`, so a bug there raises.
