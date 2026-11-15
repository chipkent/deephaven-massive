# 56. A day-or-longer bar's timestamps bound the period its stamp falls in

Date: 2026-10-28

Status: partly superseded: its refusal of unexpected stamps by [70](0070-unexpected-bar-timestamp-warns-never-drops-bars.md), and their handling by [85](0085-unexpected-period-timestamp-kept-null-date-no-relabel.md)

## Question

A daily, weekly, or monthly bar arrives stamped differently by different endpoints and flat
files. Where does it start and end, and what is its date?

## Options

None recorded: one path was proposed.

## Ruling

A daily, weekly, or monthly bar whose stamp is one Massive documents for that source is placed
in the period the stamp falls in, in the class's day zone, New York for stocks, options, and
indices and UTC for forex and crypto: a stock day bar for 2026-11-10 gets `TimestampStart`
2026-11-10 00:00 and `TimestampEnd` 2026-11-11 00:00 New York from every endpoint and flat file
alike. A stamp Massive does not document is kept as sent (decision 70). Futures bars take
Massive's own window start and end as the two timestamps. `Date` is null for a bar spanning more
than one day. The owner ruled on 2026-10-28 that a bar of several sessions has no date and that
a bar has both a start and an end timestamp; recorded in PR #514.

## Reason

One `history()` call gives the same rows from any source, and a bar stamped at its start joins
and sorts like intraday bars.

## Consequences

This record first dropped oddly stamped bars; decision 70 replaced that part.

## Rule

The bars datasets' docstrings and dataset pages.
