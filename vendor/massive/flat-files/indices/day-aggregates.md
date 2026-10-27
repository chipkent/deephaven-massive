> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Indices

### Day Aggregates

**Endpoint:** `S3 /indices/day-aggregates`

**Description:**

Candlestick data for a diverse set indices with daily open, high, low, and close values aggregated from all ticks throughout the day.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `close` | number | The close price for the symbol in the given time period. |
| `high` | number | The highest price for the symbol in the given time period. |
| `low` | number | The lowest price for the symbol in the given time period. |
| `open` | number | The open price for the symbol in the given time period. |
| `ticker` | string | The exchange symbol that this item is traded under. |
| `window_start` | timestamp - integer | The Unix nanosecond timestamp for the start of the aggregate window. |


## Example Data

| ticker | open | close | high | low | window_start |
| --- | --- | --- | --- | --- | --- |
| I:AAVE100 | 96.76700000000000 | 99.2175 | 100.40650000000000 | 95.52170000000000 | 1701756000000000000 |
| I:AAVE10RP | 96.76520000000000 | 99.2205 | 100.3647 | 95.5207 | 1701756000000000000 |
| I:AAVE25RP | 96.7651 | 99.2142 | 100.3729 | 95.5057 | 1701756000000000000 |
| I:AAVE400 | 96.7723 | 99.2201 | 100.4225 | 95.522 | 1701756000000000000 |
| I:AAVE50RP | 96.76820000000000 | 99.2121 | 100.393 | 95.5032 | 1701756000000000000 |
| I:ADA100K | 0.39980000000000000 | 0.4239 | 0.4294 | 0.39740000000000000 | 1701756000000000000 |
| I:ADA10KRP | 0.39980000000000000 | 0.4239 | 0.4294 | 0.39740000000000000 | 1701756000000000000 |
| I:ADA25KRP | 0.39980000000000000 | 0.4239 | 0.4294 | 0.39740000000000000 | 1701756000000000000 |
| I:ADA50KRP | 0.39980000000000000 | 0.4239 | 0.4294 | 0.39740000000000000 | 1701756000000000000 |
| I:XNDXTRND | 2755.6953 | 2759.323 | 2763.9738 | 2748.6122 | 1701756000000000000 |


## Plan Access

**Plan Access:** Included in select Indices plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Indices Basic | Not included |
| Indices Starter | Included |
| Indices Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Indices Business | Included |

## Plan Recency

**Plan Recency:** Updated at 11a ET to include the previous day

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Indices Basic | Not included |
| Indices Starter | End-of-day |
| Indices Advanced | End-of-day |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Indices Business | End-of-day |

## Plan History

**Plan History:** Records date back to February 14, 2023

#### Individual Plans

| Plan | History |
| --- | --- |
| Indices Basic | Not included |
| Indices Starter | All history |
| Indices Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Indices Business | All history |
