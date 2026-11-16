# 21. Sessions and queries show nothing about themselves but a label

Date: 2026-10-23

Status: accepted

## Question

Should sessions and queries expose introspection attributes, such as a session's `name` or
`config`, or a query's `dataset`?

## Options

1. Expose them, for convenience in the console.
2. Expose none; show the label only in reprs, warnings, and errors.

## Ruling

A session has no `name`, `config`, or `dataset` attribute to read back, and neither does a
query. Printing a session shows `<MassiveSession #1 open>`, and printing a query shows the call
it stands for; neither shows the API key. A user who wants the settings reads their own
`MassiveConfig`. The owner ruled on 2026-10-23 that a user should not see what they do not need;
the design documents of that day (PR #514) applied it.

## Reason

It keeps the public surface minimal and the key out of objects left in console globals.

## Consequences

A user who wants to know a session's settings reads their own `MassiveConfig`; a repr shows the
label but never the key.

## Rule

`AGENTS.md`, "No secret leaves the session".
