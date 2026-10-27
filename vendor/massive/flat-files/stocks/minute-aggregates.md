> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Stocks

### Minute Aggregates

**Endpoint:** `S3 /stocks/minute-aggregates`

**Description:**

Candlesticks with open, high, low, close, and volume at per minute granularity across all U.S. equities, made available as a daily downloadable S3 file.


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
| MSFT | 1975 | 276.75 | 275.52 | 276.75 | 275.25 | 16799904000000000 | 83 |
| MSFT | 2349 | 275.2 | 274.46 | 275.2 | 274.46 | 16799904600000000 | 99 |
| MSFT | 1739 | 274.54 | 274.5 | 274.54 | 274.15 | 16799905200000000 | 55 |
| MSFT | 731 | 275 | 274.53 | 275 | 274.33 | 16799905800000000 | 21 |
| MSFT | 255 | 274.66 | 274.65 | 274.66 | 274.65 | 16799906400000000 | 10 |
| MSFT | 305 | 274.9 | 274.9 | 274.9 | 274.9 | 16799908800000000 | 15 |
| MSFT | 541 | 274.9 | 274.91 | 274.91 | 274.9 | 16799909400000000 | 11 |
| MSFT | 465 | 274.92 | 274.9 | 274.92 | 274.9 | 16799910000000000 | 12 |
| MSFT | 713 | 275 | 275 | 275 | 275 | 16799910600000000 | 7 |
| MSFT | 394 | 275.11 | 275.11 | 275.11 | 275.11 | 16799911200000000 | 8 |


## Plan Access

**Plan Access:** Included in select Stocks plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Included |
| Stocks Developer | Included |
| Stocks Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Included |

## Plan Recency

**Plan Recency:** Updated at 11a ET to include the previous day

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | End-of-day |
| Stocks Developer | End-of-day |
| Stocks Advanced | End-of-day |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | End-of-day |

## Plan History

**Plan History:** Records date back to September 10, 2003

#### Individual Plans

| Plan | History |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | 5 years |
| Stocks Developer | 10 years |
| Stocks Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Stocks Business | All history |
