> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Indices

### Minute Aggregates

**Endpoint:** `S3 /indices/minute-aggregates`

**Description:**

Candlestick data for a diverse set indices with minute-by-minute open, high, low, and close values aggregated from all ticks throughout the day.


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
| I:AAVE100 | 69.86160000000000 | 69.83720000000000 | 69.88340000000000 | 69.83720000000000 | 1679988600000000000 |
| I:AAVE100 | 69.8381 | 69.8507 | 69.8632 | 69.8255 | 1679988660000000000 |
| I:AAVE100 | 69.852 | 69.884 | 69.884 | 69.8507 | 1679988720000000000 |
| I:AAVE100 | 69.884 | 69.84140000000000 | 69.884 | 69.8401 | 1679988780000000000 |
| I:AAVE100 | 69.8421 | 69.8544 | 69.86070000000000 | 69.83460000000000 | 1679988840000000000 |
| I:AAVE100 | 69.8545 | 69.86840000000000 | 69.8786 | 69.84740000000000 | 1679988900000000000 |
| I:AAVE100 | 69.8647 | 69.90050000000000 | 69.90050000000000 | 69.8595 | 1679988960000000000 |
| I:AAVE100 | 69.90050000000000 | 69.8604 | 69.90050000000000 | 69.839 | 1679989020000000000 |
| I:AAVE100 | 69.86 | 69.88510000000000 | 69.88510000000000 | 69.8422 | 1679989080000000000 |
| I:HX50PI | 1536.22197367325 | 1536.22197367325 | 1536.22197367325 | 1536.22197367325 | 1679996400000000000 |


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
