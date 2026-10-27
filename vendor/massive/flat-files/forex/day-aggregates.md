> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Forex

### Day Aggregates

**Endpoint:** `S3 /forex/day-aggregates`

**Description:**

Candlesticks with open, high, low, close, and volume at per day granularity for over 1,750+ currency pairs, made available as a daily downloadable S3 file.


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
| C:THBZAR | 65503 | 0.5315684 | 0.5291029 | 0.5344774 | 0.5282698 | 1680047999999000000 | 65503 |
| C:AUDCAD | 151816 | 0.90929 | 0.9116132 | 0.9136431 | 0.9082 | 1680047999999000000 | 151816 |
| C:USDIDR | 4467 | 15106.5 | 15061 | 15114.8 | 15027.9 | 1680047999999000000 | 4467 |
| C:KRWSGD | 172820 | 0.00102673 | 0.00102365 | 0.00102746 | 0.00102024 | 1680047999999000000 | 172820 |
| C:CZKCAD | 151701 | 0.0620206 | 0.06231085 | 0.06260732 | 0.061752 | 1680047999999000000 | 151701 |
| C:TRYSGD | 181691 | 0.06963392 | 0.06938305 | 0.06983544 | 0.06783 | 1680047999999000000 | 181691 |
| C:GBPMXN | 144610 | 22.536167 | 22.46372 | 22.607866 | 22.427 | 1680047999999000000 | 144610 |
| C:CHFUSD | 253862 | 1.0926091 | 1.0862589 | 1.094515 | 1.0839627 | 1680047999999000000 | 253862 |
| C:EURHUF | 64334 | 385.607 | 380.961 | 386.1 | 378.63 | 1680047999999000000 | 64334 |
| C:ZARCYP | 65495 | 0.02956824 | 0.02968438 | 0.02978944 | 0.02953257 | 1680047999999000000 | 65495 |


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
