> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Session Aggregates - CME

**Endpoint:** `S3 /futures/session-aggregates/cme`

**Description:**

Candlestick data for CME futures with open, high, low, close, and volume aggregated from all trades during each trading session.


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
| 6A:XF:AUDUSD:Q6 | 2026-07-14 | 1783979100000000000 | -0.000370000 | -0.000370000 | -0.000390000 | -0.000370000 | 6 | -0.002240000 | 3 | -0.000400000 |
| 6A:XF:AUDUSD:U6 | 2026-07-14 | 1783979100000000000 | -0.000710000 | -0.000700000 | -0.000790000 | -0.000770000 | 7985 | -6.119300000 | 324 | -0.000770000 |
| 6AQ6 | 2026-07-14 | 1783979100000000000 | 0.691400000 | 0.698350000 | 0.691350000 | 0.696750000 | 67 | 46.598800000 | 58 | 0.697050000 |
| 6AU6 | 2026-07-14 | 1783979100000000000 | 0.691100000 | 0.698650000 | 0.690600000 | 0.696650000 | 90536 | 62982.308000000 | 21404 | 0.696700000 |
| 6AU6-6AQ6 | 2026-07-14 | 1783979100000000000 | -0.000320000 | -0.000320000 | -0.000400000 | -0.000400000 | 18 | -0.006600000 | 14 | -0.000360000 |
| 6AU7-6AU6 | 2026-07-14 | 1783979100000000000 | -0.002680000 | -0.002680000 | -0.002800000 | -0.002680000 | 3 | -0.008220000 | 3 | -0.003300000 |
| 6AV6-6AQ6 | 2026-07-14 | 1783979100000000000 | -0.000700000 | -0.000700000 | -0.000700000 | -0.000700000 | 8 | -0.005600000 | 1 | -0.000760000 |
| 6AV6-6AU6 | 2026-07-14 | 1783979100000000000 | -0.000380000 | -0.000380000 | -0.000380000 | -0.000380000 | 5 | -0.001900000 | 1 | -0.000400000 |
| 6AZ6 | 2026-07-14 | 1783979100000000000 | 0.690200000 | 0.697450000 | 0.690200000 | 0.695800000 | 64 | 44.463950000 | 35 | 0.695700000 |
| 6AZ6-6AU6 | 2026-07-14 | 1783979100000000000 | -0.000880000 | -0.000880000 | -0.001080000 | -0.001040000 | 95 | -0.089360000 | 24 | -0.001000000 |


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
