# 132. An index bar of a day or longer starts at Massive's own stamp, midnight in Chicago

Date: 2026-11-17

Status: accepted

## Question

Massive stamps every index bar of a day or longer at 00:00 Chicago time (01:00 New York time)
on every source: the aggregates range, `/prev`, the day flat file, and the indicators. The
owner's live run of 2026-11-17 showed the bar covers the Chicago day: for `I:AAVE100`, which
updates around the clock, the Chicago day rebuilt from minute bars matches Massive's bar. The
library moved that stamp to 00:00 New York time, so `history()` showed the bar for 2026-11-09
starting at 05:00 UTC, an hour before the window it covers.
Should an index day bar carry Massive's stamp?

## Options

- Keep moving the stamp to 00:00 New York time: an index bar starts like a stock's, an hour
  before its data.
- Carry Massive's stamp: the window is the one Massive's bar covers.

## Ruling

On 2026-11-17, in a chat session, the owner ruled that an index bar of a day or
longer carries Massive's own stamp, for now, rather than one the library moves to 00:00 New
York time. This replaces, for indices only, the part of decision 56 that places such a bar in
the New York day.

## Reason

Relaying Massive's timestamp is consistent with the library's intent to supply Massive's data
as sent.

## Consequences

An index day bar starts and ends an hour later than a stock's for the same date, so a join of
index and stock day bars on `TimestampStart` or `TimestampEnd` does not match; join them on
`Date`. An index bar's `TimestampEnd` and `Date` keep their values. A stamp Massive documents at
a session close, such as its `/prev` sample's 16:00 New York time, still moves to midnight in
Chicago, so only a midnight stamp reaches `TimestampStart` as sent. If Massive changes its
stamps, this is revisited.

## Rule

The `TimestampStart` description of the index datasets in `docs/datasets/`, from
`_datasets/column_descriptions.py`.
