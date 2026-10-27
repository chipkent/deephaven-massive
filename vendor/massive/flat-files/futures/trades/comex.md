> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Trades Aggregates - COMEX

**Endpoint:** `S3 /futures/trades/comex`

**Description:**

Tick-level trades with nanosecond timestamps for COMEX futures, delivered as a daily downloadable S3 file.


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
| 1OZG7 | 1783981030862095779 | 31549862 | 1524195 | 4117.000000000 | 1 | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783981078121268557 | 31551009 | 1524254 | 4117.000000000 | 1 | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783987644066630309 | 32509443 | 1562745 | 4093.250000000 | 1 | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783989775590273025 | 33644398 | 1615312 | 4092.500000000 | 1 | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783991166085529677 | 34486565 | 1651253 | 4098.000000000 | 1 | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783995287372760171 | 36434108 | 1730200 | 4108.500000000 | 1 | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783998844717500395 | 37380504 | 1771606 | 4115.750000000 | 3 | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783998844717500395 | 37380504 | 1771607 | 4116.000000000 | 3 | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1783999078659758929 | 37448684 | 1774049 | 4115.250000000 | 1 | 0 | 2 | 2026-07-14 |
| 1OZG7 | 1784007683366932571 | 39362241 | 1850283 | 4121.750000000 | 1 | 0 | 2 | 2026-07-14 |


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
