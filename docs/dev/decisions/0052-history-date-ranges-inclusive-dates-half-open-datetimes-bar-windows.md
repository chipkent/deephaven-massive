# 52. History takes two dates or two datetimes, and a bar must lie whole inside

Date: 2026-10-28

Status: partly superseded: its refusal of unexpected crypto stamps by [70](0070-unexpected-bar-timestamp-warns-never-drops-bars.md)

## Question

What range does `history(start, end)` read, when users pass dates, datetimes, or a mix, and
bars cross the range's edges?

## Options

None recorded: one path was proposed.

## Ruling

Two dates mean whole trading days, both ends included; two datetimes with a time zone mean start
included, end excluded; one of each is refused. A bar appears only when its whole window is
inside the range and has ended, so a range ending mid-bar drops that bar. Forex and crypto days
are UTC days. The owner ruled on 2026-10-28 that datetimes remove the inclusive-or-exclusive
question, that a bar of several periods must lie whole within the range, and agreed to the forex
and crypto bar plan; recorded in PR #514.

## Reason

Datetimes remove the inclusive-or-exclusive ambiguity, and a bar that straddles the edge would
show data from outside the range.

## Consequences

A range that ends mid-bar drops that bar. Futures trades and quotes named by dates are read one
session at a time. This record's refusal of unexpected crypto stamps was replaced by decision
70.

## Rule

The `history()` docstrings and the dataset pages' shared argument rules.
