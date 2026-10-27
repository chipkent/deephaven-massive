> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Futures

### Trades Aggregates - NYMEX

**Endpoint:** `S3 /futures/trades/nymex`

**Description:**

Tick-level trades with nanosecond timestamps for NYMEX futures, delivered as a daily downloadable S3 file.


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
| 0CUG7 | 1783986927673449267 | 136087877 | 211 | 74.870000000 | 1 | 0 | 1 | 2026-07-14 |
| 0CUG7 | 1784017232155701189 | 194586428 | 222 | 74.870000000 | 1 | 0 | 1 | 2026-07-14 |
| 0CUG7 | 1784033712064000867 | 242364948 | 232 | 74.870000000 | 1 | 0 | 1 | 2026-07-14 |
| 0CUG7 | 1784049837527976421 | 304678875 | 239 | 74.870000000 | 1 | 0 | 1 | 2026-07-14 |
| 0CUG7 | 1784053830540008303 | 315855502 | 243 | 74.870000000 | 1 | 0 | 1 | 2026-07-14 |
| 0NGH39 | 1783986254237464703 | 12234802 | 304 | 3.245000000 | 1 | 0 | 1 | 2026-07-14 |
| 0NGH39 | 1783990473453889077 | 12579319 | 317 | 3.245000000 | 1 | 0 | 1 | 2026-07-14 |
| 0NGH39 | 1784012771159781307 | 13723825 | 327 | 3.245000000 | 1 | 0 | 1 | 2026-07-14 |
| 0NGH39 | 1784016602741114975 | 14194814 | 337 | 3.245000000 | 1 | 0 | 1 | 2026-07-14 |
| 0NGH39 | 1784020818425852999 | 14599558 | 347 | 3.245000000 | 1 | 0 | 1 | 2026-07-14 |


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
