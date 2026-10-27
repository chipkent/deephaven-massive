> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Trades Aggregates - CME

**Endpoint:** `S3 /futures/trades/cme`

**Description:**

Tick-level trades with nanosecond timestamps for CME futures, delivered as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `correction` | integer | The trade correction indicator. |
| `price` | number | The price of the trade. This is the actual dollar value per whole contract of this trade. A trade of 100 contracts with a price of $2.00 would be worth a total dollar value of $200.00. |
| `size` | integer | The total number of contracts exchanged between buyers and sellers on a given trade. |
| `exchange` | integer | The exchange ID. See Exchanges (https://massive.com/docs/rest/futures/market-operations/exchanges) for Massive's mapping of exchange IDs. |
| `sequence_number` | integer | The sequence number represents the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). Values reset after each trading session/day. |
| `report_sequence` | integer | The reporting sequence number. |
| `session_end_date` | string | Also known as the trading date, the date of the end of the trading session, in YYYY-MM-DD format. |
| `ticker` | string | The exchange symbol that this item is traded under. |
| `timestamp` | timestamp - integer | The time when the trade was generated at the exchange to nanosecond precision. |


## Example Data

| ticker | timestamp | sequence_number | report_sequence | price | size | correction | exchange | session_end_date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 06EU6 | 1783987388611835091 | 23858396 | 368 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |
| 06EU6 | 1784009282938687847 | 29063922 | 381 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |
| 06EU6 | 1784013646170597177 | 30214753 | 391 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |
| 06EU6 | 1784017684423823769 | 31522531 | 401 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |
| 06EU6 | 1784021698559367005 | 32728614 | 411 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |
| 06EU6 | 1784025710109652095 | 33718608 | 421 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |
| 06EU6 | 1784029800664579879 | 34845262 | 431 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |
| 06EU6 | 1784034161876219411 | 38495324 | 441 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |
| 06EU6 | 1784037913997602209 | 41426883 | 451 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |
| 06EU6 | 1784041728300980685 | 44320813 | 461 | 0.000100000 | 1 | 0 | 4 | 2026-07-14 |


## Plan Access

**Plan Access:** Included in select Futures plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Futures Basic | Not included |
| Futures Starter | Not included |
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
| Futures Starter | Not included |
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
| Futures Starter | Not included |
| Futures Developer | 5 years |
| Futures Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Futures Business CME | All history |
