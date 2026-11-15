# 57. Every row says when it happened, its trading day, and when it arrived; one `history()` has one split basis

Date: 2026-10-28

Status: accepted

Ticks have `Timestamp`; bars and summaries `TimestampStart` and an exclusive `TimestampEnd`; tick
and bar tables a `Date` (ISO text); every dataset ends with `TimestampInsert`. Ends are conservative
where Massive's labels are unverified, and every aggregates page's `adjusted` echo is checked.
