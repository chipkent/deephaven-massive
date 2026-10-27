> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Minute Aggregates - NYMEX

**Endpoint:** `S3 /futures/minute-aggregates/nymex`

**Description:**

Candlestick data for NYMEX futures with minute-by-minute open, high, low, close, and volume aggregated from all trades.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `close` | number | The close price for the symbol in the given time period. |
| `dollar_volume` | number | The total dollar volume of the transactions that occurred within the timeframe. |
| `exchange` | integer | The exchange ID. See Exchanges (https://massive.com/docs/rest/futures/market-operations/exchanges) for Massive's mapping of exchange IDs. |
| `high` | number | The highest price for the symbol in the given time period. |
| `low` | number | The lowest price for the symbol in the given time period. |
| `open` | number | The open price for the symbol in the given time period. |
| `session_end_date` | string | Also known as the trading date, the date of the end of the trading session, in YYYY-MM-DD format. |
| `ticker` | string | The exchange symbol that this item is traded under. |
| `transactions` | integer | The number of transactions in the aggregate window. |
| `volume` | number | The trading volume of the symbol in the given time period. |
| `window_start` | timestamp - integer | The Unix nanosecond timestamp for the start of the aggregate window. |


## Example Data

| ticker | exchange | session_end_date | window_start | open | high | low | close | volume | dollar_volume | transactions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BZU6 | 1 | 2026-07-14 | 1783980000000000000 | 83.290000000 | 83.340000000 | 83.040000000 | 83.040000000 | 29 | 2411.950000000 | 24 |
| BZU6-BZV6 | 1 | 2026-07-14 | 1783980000000000000 | 0.680000000 | 0.680000000 | 0.680000000 | 0.680000000 | 6 | 4.080000000 | 6 |
| BZV6 | 1 | 2026-07-14 | 1783980000000000000 | 82.640000000 | 82.660000000 | 82.640000000 | 82.660000000 | 3 | 247.960000000 | 2 |
| CL:BF Q6-U6-V6 | 1 | 2026-07-14 | 1783980000000000000 | -0.540000000 | -0.540000000 | -0.540000000 | -0.540000000 | 1 | -0.540000000 | 1 |
| CL:BF U6-V6-X6 | 1 | 2026-07-14 | 1783980000000000000 | -0.140000000 | -0.140000000 | -0.140000000 | -0.140000000 | 1 | -0.140000000 | 1 |
| CL:BF V6-X6-Z6 | 1 | 2026-07-14 | 1783980000000000000 | 0.040000000 | 0.050000000 | 0.040000000 | 0.050000000 | 2 | 0.090000000 | 2 |
| CL:BF Z6-H7-M7 | 1 | 2026-07-14 | 1783980000000000000 | 0.510000000 | 0.510000000 | 0.510000000 | 0.510000000 | 1 | 0.510000000 | 1 |
| CL:BZ U6-V6 | 1 | 2026-07-14 | 1783980000000000000 | 0.090000000 | 0.090000000 | 0.060000000 | 0.060000000 | 8 | 0.600000000 | 5 |
| CL:BZ U6-Z6 | 1 | 2026-07-14 | 1783980000000000000 | 0.040000000 | 0.050000000 | 0.040000000 | 0.040000000 | 2 | 0.090000000 | 2 |
| CL:BZ V6-X6 | 1 | 2026-07-14 | 1783980000000000000 | -0.020000000 | -0.020000000 | -0.020000000 | -0.020000000 | 1 | -0.020000000 | 1 |


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
| Futures Business NYMEX | Included |

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
| Futures Business NYMEX | End-of-day |

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
| Futures Business NYMEX | All history |
