> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Minute Aggregates - COMEX

**Endpoint:** `S3 /futures/minute-aggregates/comex`

**Description:**

Candlestick data for COMEX futures with minute-by-minute open, high, low, close, and volume aggregated from all trades.


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
| 1OZQ6 | 2 | 2026-07-14 | 1783980000000000000 | 4000.250000000 | 4020.000000000 | 4000.250000000 | 4013.500000000 | 100 | 401104.000000000 | 45 |
| GCG7-GCJ7 | 2 | 2026-07-14 | 1783980000000000000 | -30.900000000 | -30.900000000 | -30.900000000 | -30.900000000 | 2 | -61.800000000 | 2 |
| GCQ6 | 2 | 2026-07-14 | 1783980000000000000 | 4005.900000000 | 4015.500000000 | 4005.800000000 | 4012.800000000 | 132 | 529584.700000000 | 88 |
| GCQ6-GCG7 | 2 | 2026-07-14 | 1783980000000000000 | -90.000000000 | -90.000000000 | -90.000000000 | -90.000000000 | 1 | -90.000000000 | 1 |
| GCQ6-GCJ7 | 2 | 2026-07-14 | 1783980000000000000 | -121.000000000 | -121.000000000 | -121.000000000 | -121.000000000 | 1 | -121.000000000 | 1 |
| GCQ6-GCU6 | 2 | 2026-07-14 | 1783980000000000000 | -13.300000000 | -13.300000000 | -13.300000000 | -13.300000000 | 2 | -26.600000000 | 2 |
| GCQ6-GCV6 | 2 | 2026-07-14 | 1783980000000000000 | -27.100000000 | -27.100000000 | -27.100000000 | -27.100000000 | 1 | -27.100000000 | 1 |
| GCQ6-GCX6 | 2 | 2026-07-14 | 1783980000000000000 | -42.400000000 | -42.400000000 | -42.400000000 | -42.400000000 | 1 | -42.400000000 | 1 |
| GCQ6-GCZ6 | 2 | 2026-07-14 | 1783980000000000000 | -58.100000000 | -58.100000000 | -58.100000000 | -58.100000000 | 1 | -58.100000000 | 1 |
| GCU6 | 2 | 2026-07-14 | 1783980000000000000 | 4021.900000000 | 4022.000000000 | 4021.900000000 | 4022.000000000 | 2 | 8043.900000000 | 2 |


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
| Futures Business COMEX | Included |

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
| Futures Business COMEX | End-of-day |

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
| Futures Business COMEX | All history |
