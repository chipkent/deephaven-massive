> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Quotes Aggregates - CME

**Endpoint:** `S3 /futures/quotes/cme`

**Description:**

Top-of-book quotes with nanosecond timestamps for CME futures, delivered as a daily downloadable S3 file.


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
| 06EU6 | 1783983643808176661 | 23537600 | 359 | 1783983643808176661 | 0.000100000 | 1 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |
| 06EU6 | 1783983644002846437 | 23537619 | 361 | 1783983644002846437 | 0.000100000 | 2 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |
| 06EU6 | 1783983678853817205 | 23541535 | 362 | 1783983678853817205 |  | 0 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |
| 06EU6 | 1783984223184740647 | 23560800 | 364 | 1783984223184740647 | 0.000100000 | 1 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |
| 06EU6 | 1783984245372235589 | 23561614 | 365 | 1783984245372235589 | 0.000100000 | 2 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |
| 06EU6 | 1783984245750430854 | 23561625 | 366 | 1783984245750430854 |  | 0 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |
| 06EU6 | 1783987388493271987 | 23858227 | 367 | 1783987388493271987 | 0.000100000 | 1 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |
| 06EU6 | 1783987388611835091 | 23858397 | 370 | 1783987388611835091 |  | 0 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |
| 06EU6 | 1783987552869680251 | 23920533 | 374 | 1783987552869680251 | 0.000100000 | 1 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |
| 06EU6 | 1783987553098434753 | 23920579 | 375 | 1783987553098434753 | 0.000100000 | 2 | 1783874006509146837 |  | 0 | 4 | 2026-07-14 |


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
