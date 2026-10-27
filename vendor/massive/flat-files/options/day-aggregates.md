> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Options

### Day Aggregates

**Endpoint:** `S3 /options/day-aggregates`

**Description:**

Candlesticks with open, high, low, close, and volume at per day granularity from all U.S. options markets, made available as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `close` | number | The close price for the symbol in the given time period. |
| `high` | number | The highest price for the symbol in the given time period. |
| `low` | number | The lowest price for the symbol in the given time period. |
| `open` | number | The open price for the symbol in the given time period. |
| `ticker` | string | The exchange symbol that this item is traded under. |
| `volume` | number | The trading volume of the symbol in the given time period. |
| `transactions` | integer | The number of transactions in the aggregate window. |
| `window_start` | timestamp - integer | The Unix nanosecond timestamp for the start of the aggregate window. |


## Example Data

| ticker | volume | open | close | high | low | window_start | transactions |
| --- | --- | --- | --- | --- | --- | --- | --- |
| O:SPY230327P00390000 | 200 | 11.82 | 9.83 | 11.82 | 8.09 | 1678680000000000000 | 62 |
| O:IWM230327C00137000 | 1 | 35.77 | 35.77 | 35.77 | 35.77 | 1679889600000000000 | 1 |
| O:NANOS230327C00400000 | 2 | 1.17 | 0.26 | 1.17 | 0.26 | 1679889600000000000 | 2 |


## Plan Access

**Plan Access:** Included in select Options plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Included |
| Options Developer | Included |
| Options Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Options Business | Included |

## Plan Recency

**Plan Recency:** Updated at 11a ET to include the previous day

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Options Basic | Not included |
| Options Starter | End-of-day |
| Options Developer | End-of-day |
| Options Advanced | End-of-day |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Options Business | End-of-day |

## Plan History

**Plan History:** Records date back to June 2, 2014

#### Individual Plans

| Plan | History |
| --- | --- |
| Options Basic | Not included |
| Options Starter | 2 years |
| Options Developer | 4 years |
| Options Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Options Business | All history |
