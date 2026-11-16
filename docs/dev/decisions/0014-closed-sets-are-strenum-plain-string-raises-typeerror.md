# 14. Fixed choices are enums, and a plain string is refused

Date: 2026-10-23

Status: accepted

## Question

How does a user pass a value from a closed set, such as a bar size or an error policy: a string
or an enum member?

## Options

1. Plain strings, checked at run time.
2. A `StrEnum` with Massive's wire strings as values, and a plain string refused.

## Ruling

Where a parameter takes a value from a fixed set, such as a bar size or an error policy, the
user passes an enum member: `dm.Timespan.DAY`, not `"day"`. A string, even one that reads right,
raises `TypeError`. From the design documents, condensed into records in PR #514, dated
2026-10-23; no owner words are on record.

## Reason

One spelling per value, tab completion in the console, and typos caught by the type checker and
at run time.

## Consequences

Users type `dm.Timespan.DAY` rather than `"day"`; a string that reads right is still refused.

## Rule

`AGENTS.md`, "No `None` mode sentinels", and the enums' docstrings in `deephaven_massive`.
