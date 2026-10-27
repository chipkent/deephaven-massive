> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Minute Aggregates - CME

**Endpoint:** `S3 /futures/minute-aggregates/cme`

**Description:**

Candlestick data for CME futures with minute-by-minute open, high, low, close, and volume aggregated from all trades.


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
| `volume` | number | The trading volume of the symbol in the given time period. |
| `transactions` | integer | The number of transactions in the aggregate window. |
| `window_start` | timestamp - integer | The Unix nanosecond timestamp for the start of the aggregate window. |


## Example Data

| ticker | exchange | session_end_date | window_start | open | high | low | close | volume | dollar_volume | transactions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ETHN6 | 4 | 2026-07-14 | 1783976520000000000 | 1759.000000000 | 1759.000000000 | 1759.000000000 | 1759.000000000 | 1 | 1759.000000000 | 1 |
| MBTN6 | 4 | 2026-07-14 | 1783976520000000000 | 62215.000000000 | 62250.000000000 | 61860.000000000 | 62250.000000000 | 29 | 1796595.000000000 | 9 |
| MBTQ6 | 4 | 2026-07-14 | 1783976520000000000 | 62625.000000000 | 62625.000000000 | 62625.000000000 | 62625.000000000 | 2 | 125250.000000000 | 1 |
| METN6 | 4 | 2026-07-14 | 1783976520000000000 | 1750.000000000 | 1750.000000000 | 1742.000000000 | 1746.000000000 | 45 | 78472.000000000 | 5 |
| MBTN6 | 4 | 2026-07-14 | 1783976580000000000 | 62200.000000000 | 62200.000000000 | 62200.000000000 | 62200.000000000 | 1 | 62200.000000000 | 1 |
| MBTN6 | 4 | 2026-07-14 | 1783976700000000000 | 62295.000000000 | 62295.000000000 | 62295.000000000 | 62295.000000000 | 1 | 62295.000000000 | 1 |
| MSLN6 | 4 | 2026-07-14 | 1783977000000000000 | 74.500000000 | 74.500000000 | 74.500000000 | 74.500000000 | 5 | 372.500000000 | 1 |
| SOLN6 | 4 | 2026-07-14 | 1783977000000000000 | 74.650000000 | 74.650000000 | 74.600000000 | 74.600000000 | 2 | 149.250000000 | 2 |
| ETHN6 | 4 | 2026-07-14 | 1783977060000000000 | 1765.000000000 | 1765.000000000 | 1765.000000000 | 1765.000000000 | 1 | 1765.000000000 | 1 |
| MBTN6 | 4 | 2026-07-14 | 1783977060000000000 | 62150.000000000 | 62150.000000000 | 61885.000000000 | 61885.000000000 | 33 | 2044255.000000000 | 15 |


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
| Futures Business CME | Included |

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
| Futures Business CME | End-of-day |

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
| Futures Business CME | All history |
