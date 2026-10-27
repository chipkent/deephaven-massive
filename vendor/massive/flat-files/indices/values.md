> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Indices

### Values

**Endpoint:** `S3 /indices/values`

**Description:**

A daily record of tick-by-tick values for a comprehensive set of indices including S&P, Nasdaq, Dow Jones, and more as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | string | The exchange symbol that this item is traded under. |
| `timestamp` | integer | The timestamp in Unix MS. |
| `value` | number | The value of the index. |


## Example Data

| ticker | value | timestamp |
| --- | --- | --- |
| I:AAVE100 | 96.76700000000000 | 1701756000000000000 |
| I:AAVE10RP | 96.76520000000000 | 1701756000000000000 |
| I:AAVE25RP | 96.7651 | 1701756000000000000 |
| I:AAVE400 | 96.7723 | 1701756000000000000 |
| I:AAVE50RP | 96.76820000000000 | 1701756000000000000 |
| I:ADA100K | 0.39980000000000000 | 1701756000000000000 |
| I:ADA10KRP | 0.39980000000000000 | 1701756000000000000 |
| I:ADA25KRP | 0.39980000000000000 | 1701756000000000000 |
| I:ADA50KRP | 0.39980000000000000 | 1701756000000000000 |
| I:XNDXTRND | 2755.6953 | 1701756000000000000 |


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
