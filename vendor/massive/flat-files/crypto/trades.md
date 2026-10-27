> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Crypto

### Trades

**Endpoint:** `S3 /crypto/trades`

**Description:**

Tick-level trades with nanosecond timestamps across an expansive set of crypto pairings, made available as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `conditions` | integer | A list of condition codes. If there are multiple conditions, they will appear in quotations, separated by commas. See Trade Conditions (https://massive.com/knowledge-base/article/stock-trade-conditions) for Massive’s definitions of each condition. |
| `exchange` | integer | The exchange ID. See Exchanges (https://massive.com/docs/rest/crypto/market-operations/exchanges) for Massive's mapping of exchange IDs. |
| `id` | string | The Trade ID which uniquely identifies a trade. These are unique per combination of ticker, exchange, and TRF. For example: A trade for AAPL executed on NYSE and a trade for AAPL executed on NASDAQ could potentially have the same Trade ID. |
| `participant_timestamp` | integer | The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the trade was actually generated at the exchange. |
| `price` | number | The price of the trade. This is the actual dollar value per whole share of this trade. A trade of 100 shares with a price of $2.00 would be worth a total dollar value of $200.00. |
| `size` | number | The size of a trade (also known as volume). |
| `ticker` | string | The exchange symbol that this item is traded under. |


## Example Data

| ticker | conditions | exchange | id | participant_timestamp | price | size |
| --- | --- | --- | --- | --- | --- | --- |
| X:BTCUSD | 1 | 1 | 514943305 | 1679961600212544000 | 27142.85 | 0.00226178 |
| X:BTCUSD | 2 | 1 | 514943306 | 1679961600591766000 | 27140.12 | 0.00073209 |
| X:BTCUSD | 1 | 23 | 57435610 | 1679961600651891712 | 27132.6 | 0.00827539 |
| X:BTCUSD | 1 | 1 | 514943307 | 1679961600736983000 | 27142.3 | 0.00021249 |
| X:BTCUSD | 2 | 1 | 514943313 | 1679961600902795000 | 27139.12 | 0.00127115 |
| X:BTCUSD | 2 | 1 | 514943312 | 1679961600902795000 | 27139.13 | 0.06632499 |
| X:BTCUSD | 2 | 1 | 514943311 | 1679961600902795000 | 27140.11 | 0.00265153 |
| X:BTCUSD | 2 | 1 | 514943310 | 1679961600902795000 | 27140.12 | 0.00171091 |
| X:BTCUSD | 2 | 1 | 514943309 | 1679961600902795000 | 27140.12 | 0.003 |
| X:BTCUSD | 2 | 1 | 514943308 | 1679961600902795000 | 27140.12 | 0.0009116 |


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
