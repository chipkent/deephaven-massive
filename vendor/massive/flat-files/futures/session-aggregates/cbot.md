> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Session Aggregates - CBOT

**Endpoint:** `S3 /futures/session-aggregates/cbot`

**Description:**

Candlestick data for CBOT futures with open, high, low, close, and volume aggregated from all trades during each trading session.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `close` | number | The close price for the symbol in the given time period. |
| `dollar_volume` | number | The total dollar volume of the transactions that occurred within the timeframe. |
| `high` | number | The highest price for the symbol in the given time period. |
| `low` | number | The lowest price for the symbol in the given time period. |
| `open` | number | The open price for the symbol in the given time period. |
| `session_end_date` | string | Also known as the trading date, the date of the end of the trading session, in YYYY-MM-DD format. |
| `settlement_price` | number | The official settlement price set by the exchange for the contract at the end of the trading session. |
| `ticker` | string | The exchange symbol that this item is traded under. |
| `transactions` | integer | The number of transactions in the aggregate window. |
| `volume` | number | The trading volume of the symbol in the given time period. |
| `window_start` | timestamp - integer | The Unix nanosecond timestamp for the start of the aggregate window. |


## Example Data

| ticker | session_end_date | window_start | open | high | low | close | volume | dollar_volume | transactions | settlement_price |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10YN6 | 2026-07-14 | 1783979100000000000 | 4.620000000 | 4.620000000 | 4.535000000 | 4.589000000 | 818 | 3747.056000000 | 339 | 4.583000000 |
| 10YN6-10YQ6 | 2026-07-14 | 1783979100000000000 | -0.002000000 | 0.001000000 | -0.002000000 | -0.002000000 | 72 | -0.026000000 | 48 | -0.002000000 |
| 10YQ6 | 2026-07-14 | 1783979100000000000 | 4.612000000 | 4.617000000 | 4.540000000 | 4.582000000 | 78 | 358.076000000 | 63 | 4.585000000 |
| 45UQ6 | 2026-07-14 | 1783979100000000000 | 95.078125000 | 95.078125000 | 95.078125000 | 95.078125000 | 50 | 4753.906250000 | 1 | 95.078125000 |
| 45UU6 | 2026-07-14 | 1783979100000000000 | 94.960937500 | 94.992187500 | 94.960937500 | 94.992187500 | 52 | 4939.531250000 | 2 | 94.992187500 |
| AWU6 | 2026-07-14 | 1783979100000000000 | 130.310000000 | 130.470000000 | 129.720000000 | 130.200000000 | 6265 | 815214.830000000 | 1171 | 130.060000000 |
| AWZ6 | 2026-07-14 | 1783979100000000000 | 130.100000000 | 130.120000000 | 130.100000000 | 130.100000000 | 14 | 1821.460000000 | 3 | 130.060000000 |
| BIB 03-02 U6 | 2026-07-14 | 1783979100000000000 | 0.187500000 | 0.218750000 | 0.093750000 | 0.125000000 | 88 | 12.437500000 | 31 | 0.000000000 |
| BOB 02-01 U6 | 2026-07-14 | 1783979100000000000 | -0.062500000 | 0.406250000 | -0.125000000 | 0.093750000 | 1893 | 296.593750000 | 398 | 0.000000000 |
| BUB 01-01 U6 | 2026-07-14 | 1783979100000000000 | 0.093750000 | 0.093750000 | 0.093750000 | 0.093750000 | 3 | 0.281250000 | 1 | 0.000000000 |


## Plan Access

**Plan Access:** Included in select Futures plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Futures Basic | Not included |
| Futures Starter | Included |
| Futures Developer | Included |
| Futures Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Futures Business CBOT | Included |

## Plan Recency

**Plan Recency:** Updated at 11a ET to include the previous day

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Futures Basic | Not included |
| Futures Starter | End-of-day |
| Futures Developer | End-of-day |
| Futures Advanced | End-of-day |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Futures Business CBOT | End-of-day |

## Plan History

**Plan History:** Records date back to April 3, 2017

#### Individual Plans

| Plan | History |
| --- | --- |
| Futures Basic | Not included |
| Futures Starter | 2 years |
| Futures Developer | 5 years |
| Futures Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Futures Business CBOT | All history |
