# 110. Every calendar-date column stores ISO `yyyy-MM-dd` text

Date: 2026-10-28

Status: accepted

Every calendar-date column stores ISO `yyyy-MM-dd` text, a `String`, never a `LocalDate` or a
value computed on read. That holds for the library's own `Date` and for vendor dates such as
`ExDividendDate` alike, so any two date columns compare and join as text with no conversion.
Instants stay `Instant`s. Every dataset table is built by `_tables/columns.py` `dataset_table`,
which holds one shared `String` per distinct date, and the library owns the column names `Date`
and `Timestamp*`.

The rule has stood in `AGENTS.md` since 2026-10-28 and in the design documents before it, but no
entry recorded it: decision 57 names ISO text only for `Date`. This entry, recorded on
2026-11-13, states it for every date column.
