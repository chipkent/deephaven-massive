> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Quotes Aggregates - CBOT

**Endpoint:** `S3 /futures/quotes/cbot`

**Description:**

Top-of-book quotes with nanosecond timestamps for CBOT futures, delivered as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `ask_timestamp` | integer | The time when the ask price was submitted to the exchange. |
| `ask_price` | number | The ask price. |
| `ask_size` | number | The ask size. This represents the number of futures contracts available at the given ask price. An ask size of 2 means there are 2 contracts available to purchase at the given ask price. |
| `bid_timestamp` | integer | The time when the bid price was submitted to the exchange. |
| `bid_price` | number | The bid price. |
| `bid_size` | number | The bid size. This represents the number of futures contracts available at the given bid price. A bid size of 2 means there are 2 contracts available to purchase at the given bid price. |
| `exchange` | integer | The exchange ID. See Exchanges (https://massive.com/docs/rest/futures/market-operations/exchanges) for Massive's mapping of exchange IDs. |
| `sequence_number` | integer | The unique sequence number assigned to this quote by the exchange. |
| `report_sequence` | integer | The reporting sequence number. |
| `session_end_date` | string | Also known as the trading date, the date of the end of the trading session, in YYYY-MM-DD format. |
| `ticker` | string | The exchange symbol that this item is traded under. |
| `timestamp` | timestamp - integer | The time when the quote was generated at the exchange to nanosecond precision. |


## Example Data

| ticker | timestamp | sequence_number | report_sequence | ask_timestamp | ask_price | ask_size | bid_timestamp | bid_price | bid_size | exchange | session_end_date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 00CN6 | 1783987682968520237 | 15895228 | 60 | 1783987682968520237 | 100.000000000 | 1 |  |  | 0 | 3 | 2026-07-14 |
| 00CN6 | 1783987683328542151 | 15895254 | 62 | 1783987683328542151 | 100.000000000 | 2 |  |  | 0 | 3 | 2026-07-14 |
| 00CN6 | 1783987755297597617 | 15912264 | 63 | 1783987755297597617 |  | 0 |  |  | 0 | 3 | 2026-07-14 |
| 00CN6 | 1784009940914380245 | 18416823 | 64 | 1784009940914380245 | 100.000000000 | 1 |  |  | 0 | 3 | 2026-07-14 |
| 00CN6 | 1784009941398113291 | 18416833 | 65 | 1784009941398113291 | 100.000000000 | 2 |  |  | 0 | 3 | 2026-07-14 |
| 00CN6 | 1784010014456774657 | 18422788 | 66 | 1784010014456774657 |  | 0 |  |  | 0 | 3 | 2026-07-14 |
| 00CN6 | 1784013822322125533 | 18910349 | 67 | 1784013822322125533 | 100.000000000 | 1 |  |  | 0 | 3 | 2026-07-14 |
| 00CN6 | 1784013822657480811 | 18910357 | 68 | 1784013822657480811 | 100.000000000 | 2 |  |  | 0 | 3 | 2026-07-14 |
| 00CN6 | 1784013894734435203 | 18918096 | 69 | 1784013894734435203 |  | 0 |  |  | 0 | 3 | 2026-07-14 |
| 00CN6 | 1784017997840196199 | 19526634 | 70 | 1784017997840196199 | 100.000000000 | 1 |  |  | 0 | 3 | 2026-07-14 |


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
| Futures Business CBOT | Included |

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
| Futures Business CBOT | End-of-day |

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
| Futures Business CBOT | All history |
