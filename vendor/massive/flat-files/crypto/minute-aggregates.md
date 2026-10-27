> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Crypto

### Minute Aggregates

**Endpoint:** `S3 /crypto/minute-aggregates`

**Description:**

Candlesticks with open, high, low, close, and volume at per minute granularity across an expansive set of crypto pairings as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `close` | number | The close price for the symbol in the given time period. |
| `high` | number | The highest price for the symbol in the given time period. |
| `low` | number | The lowest price for the symbol in the given time period. |
| `open` | number | The open price for the symbol in the given time period. |
| `ticker` | string | The exchange symbol that this item is traded under. |
| `transactions` | integer | The number of transactions in the aggregate window. |
| `volume` | number | The trading volume of the symbol in the given time period. |
| `window_start` | timestamp - integer | The Unix nanosecond timestamp for the start of the aggregate window. |


## Example Data

| ticker | volume | open | close | high | low | window_start | transactions |
| --- | --- | --- | --- | --- | --- | --- | --- |
| X:BTCUSD | 35.81103311999990 | 27142.85 | 27139.64 | 27145.22 | 27098.1 | 1679961600000000000 | 394 |
| X:BTCUSD | 31.57025359000000 | 27139.65 | 27169.39 | 27179.44 | 27122 | 1679961660000000000 | 599 |
| X:BTCUSD | 13.402186900000000 | 27169.39 | 27143.01 | 27173.83 | 27128 | 1679961720000000000 | 371 |
| X:BTCUSD | 6.25858773 | 27145.13 | 27112 | 27145.13 | 27112 | 1679961780000000000 | 257 |
| X:BTCUSD | 24.929628180000000 | 27127.57 | 27136.18 | 27145.22 | 27110 | 1679961840000000000 | 482 |
| X:BTCUSD | 5.86241233 | 27133.99 | 27137.39 | 27145.83 | 27112 | 1679961900000000000 | 291 |
| X:BTCUSD | 5.10747737 | 27137.36 | 27130.71 | 27140.53 | 27110 | 1679961960000000000 | 262 |
| X:BTCUSD | 2.383374780000000 | 27130.51 | 27126.15 | 27139.58 | 27106 | 1679962020000000000 | 214 |
| X:BTCUSD | 6.954726290000000 | 27124.73 | 27115.79 | 27125.06 | 27101 | 1679962080000000000 | 318 |
| X:BTCUSD | 12.066101710000000 | 27115.79 | 27105.67 | 27115.79 | 27089 | 1679962140000000000 | 596 |


## Plan Access

**Plan Access:** Included in select Currencies plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Currencies Basic | Not included |
| Currencies Starter | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Currencies Business | Included |

## Plan Recency

**Plan Recency:** Updated at 11a ET to include the previous day

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Currencies Basic | Not included |
| Currencies Starter | End-of-day |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Currencies Business | End-of-day |

## Plan History

**Plan History:** Records date back to November 1, 2013

#### Individual Plans

| Plan | History |
| --- | --- |
| Currencies Basic | Not included |
| Currencies Starter | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Currencies Business | All history |
