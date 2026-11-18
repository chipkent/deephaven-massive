# 60. Whether an index bar of a day or longer covers the New York or the Chicago day

Date: 2026-10-29

Status: settled 2026-11-17: the Chicago day, stamped at its start on every source

It ends at 01:00 New York time. Settled with item 52.

The owner's live run of 2026-11-17 (`test_index_day_and_longer_bar_stamps_from_rest`,
`test_index_day_bar_stamps_of_the_day_file`) read `I:SPX` and `I:AAVE100`: the range's 1-day,
week, month, quarter, and year bars, `/prev`, and `us_indices/day_aggs_v1` all stamp a bar at
00:00 Chicago time (06:00 UTC in standard time, 05:00 UTC in daylight time) on its first day,
and each day bar equals the Chicago day rebuilt from minute bars. `I:SPX`, computed only during
the stock session, cannot tell the two days apart; `I:AAVE100`, computed around the clock,
matches only the Chicago day. The library keeps that stamp as `TimestampStart` and ends the bar
at 00:00 Chicago time after its last day.
