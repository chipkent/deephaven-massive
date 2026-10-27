> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Session Aggregates - COMEX

**Endpoint:** `S3 /futures/session-aggregates/comex`

**Description:**

Candlestick data for COMEX futures with open, high, low, close, and volume aggregated from all trades during each trading session.


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
| 1OZG7 | 2026-07-09 | 1783547100000000000 | 4175.000000000 | 4239.250000000 | 4175.000000000 | 4222.250000000 | 10 | 41953.500000000 | 8 | 4234.000000000 |
| 1OZJ7 | 2026-07-09 | 1783547100000000000 | 4200.000000000 | 4212.000000000 | 4200.000000000 | 4212.000000000 | 2 | 8412.000000000 | 2 | 4265.500000000 |
| 1OZQ6 | 2026-07-09 | 1783547100000000000 | 4089.250000000 | 4148.500000000 | 4063.500000000 | 4132.750000000 | 39027 | 160599488.250000000 | 19778 | 4140.750000000 |
| 1OZQ6-1OZG7 | 2026-07-09 | 1783547100000000000 | -83.000000000 | -83.000000000 | -100.000000000 | -85.250000000 | 10 | -869.000000000 | 7 | -93.250000000 |
| 1OZQ6-1OZJ7 | 2026-07-09 | 1783547100000000000 | -114.000000000 | -114.000000000 | -114.000000000 | -114.000000000 | 1 | -114.000000000 | 1 | -124.750000000 |
| 1OZQ6-1OZQ7 | 2026-07-09 | 1783547100000000000 | -168.500000000 | -166.500000000 | -168.500000000 | -166.500000000 | 2 | -335.000000000 | 2 | -189.750000000 |
| 1OZQ6-1OZV6 | 2026-07-09 | 1783547100000000000 | -28.750000000 | -28.250000000 | -29.250000000 | -28.750000000 | 1633 | -46548.500000000 | 275 | -29.000000000 |
| 1OZQ6-1OZZ6 | 2026-07-09 | 1783547100000000000 | -59.500000000 | -59.250000000 | -60.750000000 | -60.750000000 | 199 | -11960.250000000 | 129 | -60.750000000 |
| 1OZQ7 | 2026-07-09 | 1783547100000000000 | 4280.000000000 | 4300.000000000 | 4280.000000000 | 4300.000000000 | 2 | 8580.000000000 | 2 | 4330.500000000 |
| 1OZV6 | 2026-07-09 | 1783547100000000000 | 4115.000000000 | 4176.750000000 | 4095.000000000 | 4160.250000000 | 1740 | 7239002.000000000 | 309 | 4169.750000000 |


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
