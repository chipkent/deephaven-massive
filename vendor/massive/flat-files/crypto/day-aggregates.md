> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Crypto

### Day Aggregates

**Endpoint:** `S3 /crypto/day-aggregates`

**Description:**

Candlesticks with open, high, low, close, and volume at per day granularity across an expansive set of crypto pairings as a daily downloadable S3 file.


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
| X:LINKUSD | 1.44164687756887E+06 | 6.876 | 6.947 | 7.048 | 6.74121 | 1680047999999000000 | 25603 |
| X:ADAUSD | 4.43462935167232E+07 | 0.3457 | 0.36816 | 0.3703 | 0.34252 | 1680047999999000000 | 41699 |
| X:LCXUSD | 4.3655476893461E+06 | 0.07642 | 0.0815 | 0.084 | 0.0757 | 1680047999999000000 | 1914 |
| X:ALGOUSD | 2.67514961130791E+07 | 0.1999 | 0.2017 | 0.2044 | 0.197 | 1680047999999000000 | 17754 |
| X:SOLUSD | 1.62702570890817E+06 | 19.92 | 20.45 | 20.88 | 19.6 | 1680047999999000000 | 39358 |
| X:RNDRUSD | 3.16830291143452E+06 | 1.1478 | 1.1769 | 1.1915 | 1.0871 | 1680047999999000000 | 11610 |
| X:BTCGBP | 249.50336357000100 | 22076.67 | 22116.41 | 22279.16 | 21646.53 | 1680047999999000000 | 7767 |
| X:BTCEUR | 2220.673288880030 | 25112.6 | 25167.8 | 25373.49 | 24632.93 | 1680047999999000000 | 49260 |
| X:LTCUSD | 317395.82338879400 | 89.3 | 89.14 | 89.64 | 85.61 | 1680047999999000000 | 54026 |
| X:MOBUSD | 1736.2992326200000 | 1.4787 | 1.4567 | 1.4813 | 1.4028 | 1680047999999000000 | 508 |


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
