> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Forex

### Quotes

**Endpoint:** `S3 /forex/quotes`

**Description:**

Global foreign exchange rate quotes from major institutions for over 1,750+ currency pairs, made available as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `ask_exchange` | integer | The ask exchange ID. |
| `ask_price` | number | The ask price. |
| `bid_exchange` | integer | The bid exchange ID. |
| `bid_price` | number | The bid price. |
| `participant_timestamp` | integer | The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the quote was actually generated at the exchange. |
| `ticker` | string | The exchange symbol that this item is traded under. |


## Example Data

| Ticker | ask_exchange | ask_price | bid_exchange | bid_price | participant_timestamp |
| --- | --- | --- | --- | --- | --- |
| X:EUR-USD | 48 | 1.08066 | 48 | 1.08063 | 1679961600000000000 |
| X:EUR-USD | 48 | 1.08066 | 48 | 1.08059 | 1679961600000000000 |
| X:EUR-USD | 48 | 1.08066 | 48 | 1.08063 | 1679961600000000000 |
| X:EUR-USD | 48 | 1.08069 | 48 | 1.08066 | 1679961600000000000 |
| X:EUR-USD | 48 | 1.08067 | 48 | 1.08064 | 1679961600000000000 |
| X:EUR-USD | 48 | 1.08069 | 48 | 1.08066 | 1679961600000000000 |
| X:EUR-USD | 48 | 1.0807 | 48 | 1.08063 | 1679961601000000000 |
| X:EUR-USD | 48 | 1.08069 | 48 | 1.08066 | 1679961601000000000 |
| X:EUR-USD | 48 | 1.0807 | 48 | 1.08067 | 1679961602000000000 |
| X:EUR-USD | 48 | 1.08071 | 48 | 1.08064 | 1679961603000000000 |


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
