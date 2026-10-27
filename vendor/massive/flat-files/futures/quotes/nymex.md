> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Quotes Aggregates - NYMEX

**Endpoint:** `S3 /futures/quotes/nymex`

**Description:**

Top-of-book quotes with nanosecond timestamps for NYMEX futures, delivered as a daily downloadable S3 file.


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
| 0CJJ9 | 1783986627760561947 | 1884313 | 121 | 1783986627760561947 | 1722.600000000 | 1 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |
| 0CJJ9 | 1783986748492493631 | 1885982 | 123 | 1783986748492493631 | 1722.600000000 | 2 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |
| 0CJJ9 | 1783986911954408159 | 1888412 | 124 | 1783986911954408159 |  | 0 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |
| 0CJJ9 | 1783990804494464567 | 2036977 | 125 | 1783990804494464567 | 1724.300000000 | 1 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |
| 0CJJ9 | 1783991209214319767 | 2066523 | 126 | 1783991209214319767 |  | 0 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |
| 0CJJ9 | 1784013136718896565 | 2466000 | 127 | 1784013136718896565 | 1724.300000000 | 1 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |
| 0CJJ9 | 1784013257445517777 | 2468607 | 128 | 1784013257445517777 | 1724.300000000 | 2 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |
| 0CJJ9 | 1784013421580541055 | 2470867 | 129 | 1784013421580541055 |  | 0 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |
| 0CJJ9 | 1784016927360859945 | 2536634 | 130 | 1784016927360859945 | 1722.600000000 | 1 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |
| 0CJJ9 | 1784017048066801223 | 2538415 | 131 | 1784017048066801223 | 1722.600000000 | 2 | 1783873974661867609 |  | 0 | 1 | 2026-07-14 |


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
| Futures Business NYMEX | Included |

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
| Futures Business NYMEX | End-of-day |

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
| Futures Business NYMEX | All history |
