> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Session Aggregates - NYMEX

**Endpoint:** `S3 /futures/session-aggregates/nymex`

**Description:**

Candlestick data for NYMEX futures with open, high, low, close, and volume aggregated from all trades during each trading session.


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
| B0F7 | 2026-07-14 | 1783979100000000000 | 0.780000000 | 0.780000000 | 0.780000000 | 0.780000000 | 5 | 3.900000000 | 1 | 0.776670000 |
| B0Q6 | 2026-07-14 | 1783979100000000000 | 0.760000000 | 0.760000000 | 0.760000000 | 0.760000000 | 5 | 3.800000000 | 1 | 0.762080000 |
| B0Z6 | 2026-07-14 | 1783979100000000000 | 0.780000000 | 0.780000000 | 0.780000000 | 0.780000000 | 5 | 3.900000000 | 1 | 0.777080000 |
| BZ:BF F7-G7-H7 | 2026-07-14 | 1783979100000000000 | 0.060000000 | 0.150000000 | 0.060000000 | 0.080000000 | 1161 | 137.280000000 | 472 | 0.070000000 |
| BZ:BF G7-H7-J7 | 2026-07-14 | 1783979100000000000 | 0.070000000 | 0.120000000 | 0.070000000 | 0.080000000 | 816 | 79.930000000 | 400 | 0.080000000 |
| BZ:BF H7-J7-K7 | 2026-07-14 | 1783979100000000000 | 0.060000000 | 0.090000000 | 0.060000000 | 0.080000000 | 292 | 22.700000000 | 49 | 0.070000000 |
| BZ:BF J7-K7-M7 | 2026-07-14 | 1783979100000000000 | 0.060000000 | 0.070000000 | 0.060000000 | 0.060000000 | 52 | 3.290000000 | 14 | 0.060000000 |
| BZ:BF K7-M7-N7 | 2026-07-14 | 1783979100000000000 | 0.060000000 | 0.060000000 | 0.060000000 | 0.060000000 | 88 | 5.280000000 | 26 | 0.060000000 |
| BZ:BF M7-N7-Q7 | 2026-07-14 | 1783979100000000000 | 0.060000000 | 0.060000000 | 0.050000000 | 0.060000000 | 39 | 2.330000000 | 8 | 0.060000000 |
| BZ:BF U6-V6-X6 | 2026-07-14 | 1783979100000000000 | -0.180000000 | 0.250000000 | -0.210000000 | 0.160000000 | 231 | 4.440000000 | 159 | 0.110000000 |


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
