> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Trades Aggregates - CBOT

**Endpoint:** `S3 /futures/trades/cbot`

**Description:**

Tick-level trades with nanosecond timestamps for CBOT futures, delivered as a daily downloadable S3 file.


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
| 00NH7 | 1783985187597433207 | 14375102 | 365 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |
| 00NH7 | 1783989483572038535 | 15011685 | 378 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |
| 00NH7 | 1784011822526425901 | 16895548 | 388 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |
| 00NH7 | 1784015631964412325 | 17736958 | 398 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |
| 00NH7 | 1784019858003072565 | 18555120 | 408 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |
| 00NH7 | 1784023799417308999 | 19130305 | 418 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |
| 00NH7 | 1784027995448370803 | 19724340 | 428 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |
| 00NH7 | 1784032056776984601 | 21068434 | 438 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |
| 00NH7 | 1784036018056397287 | 26471618 | 448 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |
| 00NH7 | 1784039888217423503 | 29944711 | 458 | 105.000000000 | 1 | 0 | 3 | 2026-07-14 |


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
