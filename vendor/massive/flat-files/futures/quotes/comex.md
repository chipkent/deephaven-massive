> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Quotes Aggregates - COMEX

**Endpoint:** `S3 /futures/quotes/comex`

**Description:**

Top-of-book quotes with nanosecond timestamps for COMEX futures, delivered as a daily downloadable S3 file.


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
| 1OZG7 | 1783981251455827709 | 31565732 | 1524725 | 1783954047240668033 |  | 0 | 1783981251455827709 | 4093.250000000 | 1 | 2 | 2026-07-14 |
| 1OZG7 | 1783981878810299059 | 31595885 | 1525767 | 1783981878810299059 | 4116.750000000 | 1 | 1783981251455827709 | 4093.250000000 | 1 | 2 | 2026-07-14 |
| 1OZG7 | 1783987644066630309 | 32509445 | 1562747 | 1783981878810299059 | 4116.750000000 | 1 | 1783987644066630309 |  | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783989259514900979 | 33421553 | 1605264 | 1783989259514900979 | 4108.500000000 | 1 | 1783987644066630309 |  | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783990992809346041 | 34361248 | 1645581 | 1783989259514900979 | 4108.500000000 | 1 | 1783990992809346041 | 4095.750000000 | 1 | 2 | 2026-07-14 |
| 1OZG7 | 1783991065714862849 | 34416941 | 1648338 | 1783989259514900979 | 4108.500000000 | 1 | 1783991065714862849 | 4098.000000000 | 1 | 2 | 2026-07-14 |
| 1OZG7 | 1783991166085529677 | 34486566 | 1651255 | 1783989259514900979 | 4108.500000000 | 1 | 1783991166085529677 |  | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783995287372760171 | 36434109 | 1730202 | 1783995287372760171 | 4116.750000000 | 1 | 1783991166085529677 |  | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783996150391575385 | 36673889 | 1742232 | 1783995287372760171 | 4116.750000000 | 1 | 1783996150391575385 | 4107.250000000 | 1 | 2 | 2026-07-14 |
| 1OZG7 | 1783997515900361049 | 37054669 | 1759143 | 1783995287372760171 | 4116.750000000 | 1 | 1783997515900361049 | 4107.250000000 | 8 | 2 | 2026-07-14 |


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
| Futures Business COMEX | Included |

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
| Futures Business COMEX | End-of-day |

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
| Futures Business COMEX | All history |
