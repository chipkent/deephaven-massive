# 6. Tables are freed automatically and by hand, and queries have no stop

Date: 2026-10-23

Status: accepted

## Question

How does a user stop a stream or free a table: by garbage collection, by an explicit call, or
both?

## Options

1. Explicit only: a `stop()` on each query.
2. Automatic only: garbage collection releases everything.
3. Both: garbage collection releases a result, and `m.release(table)` and `m.close()` give
   deterministic control.

## Ruling

Option 3. The owner chose it on 2026-10-28, from the design documents of 2026-10-23 (PR #514). A
live table nothing in Python references any more is released by garbage collection, since each
result has its own liveness scope; `m.release(table)` frees one by hand, and `m.close()` ends
the session. A query has no `stop()`. With `t = q.append_only()` and `t2 = t.where(...)`,
dropping `t` does not end the stream while `t2`, or a console variable, still lives.

## Reason

A long-running job must keep ticking when an earlier reference is dropped, and idle connections
must not be wasted.

## Consequences

A stream ends only when every table built on it is gone or released, which can be later than the
user expects.

## Rule

`AGENTS.md`, Deephaven invariants: "Every live result has its own liveness scope" and "Streams
tear down from the blink table's `on_shutdown_callback`".
