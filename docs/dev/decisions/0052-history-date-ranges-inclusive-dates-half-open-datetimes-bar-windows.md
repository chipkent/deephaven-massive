# 52. Date ranges: two kinds, whole bar windows, UTC days for forex and crypto

Date: 2026-10-28

Status: partly superseded: its refusal of unexpected crypto stamps by [70](0070-unexpected-bar-timestamp-warns-never-drops-bars.md)

`history()` takes two dates (whole days or sessions, inclusive) or two aware datetimes (half-open),
never one of each. A bar is returned only when its whole window is inside the range and has ended.
Futures trades and quotes named by dates are read one session at a time. *Its refusal of unexpected
crypto stamps is superseded by 70.*
