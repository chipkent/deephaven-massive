# 50. Stock 1-day bar stamps per source

Date: 2026-10-28

Status: settled 2026-11-17: range and day file 00:00, grouped daily 16:00, `/prev` 04:00 New York time (16:00 until at least 2026-11-02)

Settled 2026-11-02: range and day file 00:00, grouped daily and `/prev` 16:00 New York time.

Settled again 2026-11-17: the live run of `tests/namespaces/test_stocks_integration.py`
(`test_stock_day_and_longer_bar_stamps_from_rest`) found `/prev` stamping AAPL's 2026-11-16 bar
at 2026-11-16T09:00:00Z, 04:00 New York time, with the values the range serves for that day at
00:00 (largest relative error 1.7e-08); grouped daily still stamps 16:00. The library accepts
04:00 for a stock 1-day bar beside 00:00, 13:00, and 16:00, and moves it to 00:00 of its day.
