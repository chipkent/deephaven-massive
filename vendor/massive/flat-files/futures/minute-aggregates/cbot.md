> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Minute Aggregates - CBOT

**Endpoint:** `S3 /futures/minute-aggregates/cbot`

**Description:**

Candlestick data for CBOT futures with minute-by-minute open, high, low, close, and volume aggregated from all trades.


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
| 10YN6 | 3 | 2026-07-14 | 1783980000000000000 | 4.620000000 | 4.620000000 | 4.620000000 | 4.620000000 | 5 | 23.100000000 | 1 |
| BOB 02-01 U6 | 3 | 2026-07-14 | 1783980000000000000 | -0.062500000 | -0.062500000 | -0.062500000 | -0.062500000 | 8 | -0.500000000 | 3 |
| FIX 02-01 U6 | 3 | 2026-07-14 | 1783980000000000000 | -0.007812500 | -0.007812500 | -0.007812500 | -0.007812500 | 13 | -0.101562500 | 3 |
| FYN 01-01 U6 | 3 | 2026-07-14 | 1783980000000000000 | 0.000000000 | 0.000000000 | 0.000000000 | 0.000000000 | 1 | 0.000000000 | 1 |
| FYT 03-02 U6 | 3 | 2026-07-14 | 1783980000000000000 | -0.023437500 | -0.023437500 | -0.023437500 | -0.023437500 | 180 | -4.218750000 | 6 |
| MYMU6 | 3 | 2026-07-14 | 1783980000000000000 | 52732.000000000 | 52757.000000000 | 52726.000000000 | 52751.000000000 | 93 | 4905087.000000000 | 67 |
| NCB 03-02 U6 | 3 | 2026-07-14 | 1783980000000000000 | 0.000000000 | 0.000000000 | 0.000000000 | 0.000000000 | 2 | 0.000000000 | 2 |
| NOB 02-01 U6 | 3 | 2026-07-14 | 1783980000000000000 | 0.015625000 | 0.015625000 | 0.015625000 | 0.015625000 | 62 | 0.968750000 | 7 |
| NUB 02-01 U6 | 3 | 2026-07-14 | 1783980000000000000 | -0.046875000 | -0.046875000 | -0.046875000 | -0.046875000 | 1 | -0.046875000 | 1 |
| TEX 07-05 U6 | 3 | 2026-07-14 | 1783980000000000000 | 0.015625000 | 0.015625000 | 0.015625000 | 0.015625000 | 12 | 0.187500000 | 2 |


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
