# 56. Every bar of a day or longer is stamped at its window's start

Date: 2026-10-28

Status: partly superseded: its refusal of unexpected stamps by [70](0070-unexpected-bar-timestamp-warns-never-drops-bars.md), and their handling by [85](0085-unexpected-period-timestamp-kept-null-date-no-relabel.md)

Every bar of a day or longer is stamped at its window's start: 00:00 of its first day in the class
day zone (New York for stocks, options, and indices; UTC for forex and crypto), so one `history()`
returns the same rows from any source. Futures keep `window_start` and `session_end_date`. *Its
refusal of unexpected stamps is superseded by 70, and their handling by 85.*
