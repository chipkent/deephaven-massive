# 5. The library never starts a Deephaven server of its own

Date: 2026-10-23

Status: accepted

## Question

Should the library start a Deephaven server itself when none is running?

## Options

1. Start an embedded server on demand.
2. Require an existing `ExecutionContext` and fail fast without one.

## Ruling

The library has no embedded server. `dm.MassiveSession()` on a thread with no Deephaven
execution context raises `MassiveError` at once, telling the user to start a server or work in a
Deephaven console. `deephaven-server` is a development dependency only. From the design
documents, condensed into records in PR #514, dated 2026-10-23; no owner words are on record.

## Reason

A second JVM beside the console's would collide with it; the owner confirmed this reason on
2026-11-16.

## Consequences

A user who imports the library in plain Python gets an error at once and must start a server
first. `deephaven-server` is a development dependency only.

## Rule

The `MassiveSession` docstring and `README.md`, Install.
