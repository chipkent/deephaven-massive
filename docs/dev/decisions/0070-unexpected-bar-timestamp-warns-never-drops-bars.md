# 70. A bar stamped at an unexpected time is served as sent, with a warning

Date: 2026-10-30

Status: accepted

## Question

A bar or period arrives stamped at a time its class does not expect, such as a crypto day not
cut at UTC midnight. Is it refused, moved, or served?

## Options

1. Refuse the bar, as decisions 52 and 56 first did.
2. Move it to the expected stamp.
3. Serve it as Massive stamped it, with a WARNING.

## Ruling

A bar stamped at a time Massive does not document for that source, such as a crypto day bar at
04:00 UTC where Massive documents 00:00, keeps Massive's timestamp, gets a null `Date`, ends one
period after its stamp, and is never moved to a guessed day or dropped; a documented stamp is
placed by decision 56. The log says so once per distinct offset. The owner ruled on 2026-10-30
that bars are served and never excluded; the record names no place. The null `Date` was added on
2026-11-02 without owner words (PR #514), and a crypto day summary not in UTC is served the same
way (decision 77).

## Reason

Users must see what Massive sent; moving a bar to a guessed day could cause look-ahead.

## Consequences

A table can hold rows with a null `Date`, and the WARNING says which stamps.

## Rule

`AGENTS.md`, "Supply Massive's data; never repair it".
