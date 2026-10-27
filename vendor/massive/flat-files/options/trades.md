> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Options

### Trades

**Endpoint:** `S3 /options/trades`

**Description:**

Tick-level trades with nanosecond timestamps from all U.S. options markets including CBOE, NYSE, and NASDAQ, made available as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `conditions` | integer | A list of condition codes. If there are multiple conditions, they will appear in quotations, separated by commas. See Trade Conditions (https://massive.com/knowledge-base/article/stock-trade-conditions) for Massive’s definitions of each condition. |
| `correction` | integer | The trade correction indicator. |
| `exchange` | integer | The exchange ID. See Exchanges (https://massive.com/docs/rest/stocks/market-operations/exchanges) for Massive's mapping of exchange IDs. |
| `price` | number | The price of the trade. This is the actual dollar value per whole share of this trade. A trade of 100 shares with a price of $2.00 would be worth a total dollar value of $200.00. |
| `sip_timestamp` | integer | The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this trade from the exchange which produced it. |
| `size` | number | The size of a trade (also known as volume). |
| `ticker` | string | The exchange symbol that this item is traded under. |


## Example Data

| ticker | conditions | correction | exchange | price | sip_timestamp | size |
| --- | --- | --- | --- | --- | --- | --- |
| O:SPY230327P00390000 | 232 |  | 312 | 11.82 | 1678715620948000000 | 1 |
| O:SPY230327P00390000 | 209 |  | 325 | 11.07 | 1678716040304000000 | 15 |
| O:SPY230327P00390000 | 227 |  | 302 | 11.13 | 1678716830560000000 | 1 |
| O:SPY230327P00390000 | 209 |  | 325 | 11.67 | 1678717075708000000 | 1 |
| O:SPY230327P00390000 | 227 |  | 300 | 8.9 | 1678719793268000000 | 1 |
| O:SPY230327P00390000 | 227 |  | 302 | 9.35 | 1678719975903000000 | 2 |
| O:SPY230327P00390000 | 227 |  | 302 | 9.35 | 1678719975903000000 | 2 |
| O:SPY230327P00390000 | 227 |  | 302 | 9.35 | 1678719975903000000 | 1 |
| O:SPY230327P00390000 | 209 |  | 313 | 8.37 | 1678720757648000000 | 5 |
| O:SPY230327P00390000 | 227 |  | 300 | 8.96 | 1678721008544000000 | 1 |


## Plan Access

**Plan Access:** Included in select Options plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
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
| Options Starter | Not included |
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
| Options Starter | Not included |
| Options Developer | 4 years |
| Options Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Options Business | All history |
