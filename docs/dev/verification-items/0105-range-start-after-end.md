# 105. A range whose start is after its end

Date: 2026-11-11

Status: settled 2026-11-12: the aggregates range answered HTTP 400, the dividends' `ex_dividend_date` HTTP 200 with no rows; still refused, since `history()` turns a range into the bars or days inside it and a reversed one has none to send; recorded by `tests/datasets/test_dates_integration.py`

Settled 2026-11-12: the aggregates range answered HTTP 400, the dividends' `ex_dividend_date` HTTP
200 with no rows; still refused, since `history()` turns a range into the bars or days inside it and
a reversed one has none to send; recorded by `tests/datasets/test_dates_integration.py`.
