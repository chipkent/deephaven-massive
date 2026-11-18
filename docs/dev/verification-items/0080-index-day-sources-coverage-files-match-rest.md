# 80. Which day each index day source covers across many indices, and whether index files match REST

Date: 2026-11-02

Status: settled 2026-11-17: every source covers the Chicago day, and each day file but the newest matches REST

Items 52 and 60 stand until read. Settled by `tests/namespaces/test_indices_integration.py` (Indices
Starter and S3).

The owner's live run of 2026-11-17 (`test_index_day_windows_and_files_across_many_indices`)
read eleven indices (`I:SPX`, `I:NDX`, `I:DJI`, `I:RUT`, `I:VIX`, and six AAVE, BTC, and ETH
indices that update around the clock) on five days. The range's 1-day bars, the daily open and
close, and `/prev` covered the Chicago day for all six around-the-clock indices on every day,
with no error, and so did the day file on every day but the most recent; the five
regular-hours indices hold the same minutes in both days. The day file matched the range bar on
every day but the most recent (2026-11-16), whose file matched no window for ten of the eleven
indices, its values off by up to 4 percent.
