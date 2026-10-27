> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Options

### Minute Aggregates

**Endpoint:** `S3 /options/minute-aggregates`

**Description:**

Candlesticks with open, high, low, close, and volume at per minute granularity from all U.S. options markets, made available as a daily downloadable S3 file.


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
| O:SPY230327P00390000 | 1 | 11.82 | 11.82 | 11.82 | 11.82 | 1678715580000000000 | 1 |
| O:SPY230327P00390000 | 15 | 11.07 | 11.07 | 11.07 | 11.07 | 1678716000000000000 | 1 |
| O:SPY230327P00390000 | 1 | 11.13 | 11.13 | 11.13 | 11.13 | 1678716780000000000 | 1 |
| O:SPY230327P00390000 | 1 | 11.67 | 11.67 | 11.67 | 11.67 | 1678717020000000000 | 1 |
| O:SPY230327P00390000 | 1 | 8.9 | 8.9 | 8.9 | 8.9 | 1678719780000000000 | 1 |
| O:SPY230327P00390000 | 5 | 9.35 | 9.35 | 9.35 | 9.35 | 1678719960000000000 | 3 |
| O:SPY230327P00390000 | 5 | 8.37 | 8.37 | 8.37 | 8.37 | 1678720740000000000 | 1 |
| O:SPY230327P00390000 | 1 | 8.96 | 8.96 | 8.96 | 8.96 | 1678720980000000000 | 1 |
| O:SPY230327P00390000 | 1 | 8.13 | 8.13 | 8.13 | 8.13 | 1678721340000000000 | 1 |
| O:SPY230327P00390000 | 1 | 8.09 | 8.09 | 8.09 | 8.09 | 1678721460000000000 | 1 |


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
