> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Forex

### Minute Aggregates

**Endpoint:** `S3 /forex/minute-aggregates`

**Description:**

Candlesticks with open, high, low, close, and volume at per minute granularity for over 1,750+ currency pairs, made available as a daily downloadable S3 file.


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
| C:EURUSD | 120 | 1.08063 | 1.08033 | 1.0807 | 1.0803 | 1679961600000000000 | 120 |
| C:EURUSD | 120 | 1.08034 | 1.08055 | 1.0806 | 1.0803 | 1679961660000000000 | 120 |
| C:EURUSD | 83 | 1.0804 | 1.08066 | 1.08068 | 1.0803 | 1679961720000000000 | 83 |
| C:EURUSD | 104 | 1.08067 | 1.08066 | 1.0808 | 1.0805 | 1679961780000000000 | 104 |
| C:EURUSD | 87 | 1.08072 | 1.08075 | 1.0808 | 1.0805 | 1679961840000000000 | 87 |
| C:EURUSD | 87 | 1.08072 | 1.0807 | 1.08074 | 1.0805 | 1679961900000000000 | 87 |
| C:EURUSD | 71 | 1.08072 | 1.08076 | 1.0808 | 1.0805 | 1679961960000000000 | 71 |
| C:EURUSD | 65 | 1.08075 | 1.08064 | 1.0808 | 1.0805 | 1679962020000000000 | 65 |
| C:EURUSD | 126 | 1.08063 | 1.08079 | 1.0808 | 1.0805 | 1679962080000000000 | 126 |
| C:EURUSD | 74 | 1.0808 | 1.08069 | 1.081 | 1.0806 | 1679962140000000000 | 74 |


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

**Plan History:** Records date back to September 25, 2009

#### Individual Plans

| Plan | History |
| --- | --- |
| Currencies Basic | Not included |
| Currencies Starter | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Currencies Business | All history |
