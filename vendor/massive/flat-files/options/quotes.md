> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Options

### Quotes

**Endpoint:** `S3 /options/quotes`

**Description:**

Top of book quotes with nanosecond timestamps from all U.S. options markets including CBOE, NYSE, and NASDAQ, made available as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `ask_exchange` | integer | The ask exchange ID. |
| `ask_price` | number | The ask price. |
| `ask_size` | number | The ask size. This represents the number of contracts sellers are offering at the given ask price. |
| `bid_exchange` | integer | The bid exchange ID. |
| `bid_price` | number | The bid price. |
| `bid_size` | number | The bid size. This represents the number of contracts buyers are bidding for at the given bid price. |
| `sequence_number` | integer | The sequence number represents the sequence in which quote events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). Values reset after each trading session/day. |
| `sip_timestamp` | integer | The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this quote from the exchange which produced it. |
| `ticker` | string | The exchange symbol that this item is traded under. |


## Example Data

| ticker | ask_exchange | ask_price | ask_size | bid_exchange | bid_price | bid_size | sequence_number | sip_timestamp |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| O:SPY241220P00720000 | 309 | 326.77 | 1 | 309 | 321.77 | 1 | 81779 | 1680010200067217664 |
| O:SPY241220P00720000 | 309 | 328.28 | 1 | 303 | 318.28 | 2 | 152697 | 1680010200661050624 |
| O:SPY241220P00720000 | 301 | 328.28 | 201 | 301 | 318.28 | 201 | 219334 | 1680010201264395520 |
| O:SPY241220P00720000 | 301 | 328.28 | 201 | 301 | 320.13 | 200 | 226821 | 1680010201304631040 |
| O:SPY241220P00720000 | 301 | 328.28 | 200 | 301 | 320.13 | 200 | 235757 | 1680010201377559552 |
| O:SPY241220P00720000 | 302 | 328.41 | 1 | 301 | 320.13 | 200 | 271019 | 1680010201635999488 |
| O:SPY241220P00720000 | 300 | 325.88 | 1 | 300 | 322.56 | 1 | 280897 | 1680010201690479360 |
| O:SPY241220P00720000 | 301 | 325.88 | 200 | 301 | 322.56 | 200 | 281400 | 1680010201693857280 |
| O:SPY241220P00720000 | 301 | 326.72 | 1 | 325 | 322.5 | 1 | 519037 | 1680010203073100288 |
| O:SPY241220P00720000 | 301 | 326.72 | 201 | 301 | 321.72 | 201 | 531179 | 1680010203143363840 |


## Plan Access

**Plan Access:** Included in select Options plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | Not included |
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
| Options Developer | Not included |
| Options Advanced | End-of-day |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Options Business | End-of-day |

## Plan History

**Plan History:** Records date back to March 7, 2022

#### Individual Plans

| Plan | History |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | Not included |
| Options Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Options Business | All history |
