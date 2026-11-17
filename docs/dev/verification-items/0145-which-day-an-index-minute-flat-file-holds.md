# 145. Which day does an index minute flat file hold?

Date: 2026-11-17

Status: settled 2026-11-17: the Chicago day of its date, 01:00 to 01:00 New York time

Unknown until then: whether Massive's `us_indices/minute_aggs_v1` file of a date holds that
date's New York day, as REST and the library's documented index days do, or another day.
Massive's flat-file pages say nothing of where a file's day begins. Only an index that updates
around the clock shows it, since the stock-hours indices have no rows near midnight.

Evidence: the owner's live run of 2026-11-17. For I:AAVE100, I:AAVE10RP, I:BTC10RP, I:BTC1RP,
I:ETH100RP, and I:ETH200RP, the files of 2026-09-08 (daylight time) and 2026-11-09, 2026-11-12,
and 2026-11-13 (standard time) each hold 1,440 minutes, from 00:00 Chicago (01:00 New York) to
23:59 Chicago (00:59 New York the next day). REST holds all 1,440 minutes of each New York day,
so each file lacks that day's first hour, which lies in the previous date's file.

The library reads an index minute file of a date as its Chicago day: a window of New York days
also reads the file of the day before, keeps only the window's rows, and reads a missing file
from REST as the instants it would hold. Verified by `test_an_index_minute_file_holds_its_chicago_day`
in `tests/namespaces/test_indices_integration.py` (Indices Starter and S3), which fails if a
file holds a row outside its Chicago day.
