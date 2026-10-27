> For the full documentation index, see: https://massive.com/docs/llms.txt

# FLAT-FILES
## Stocks

### Quotes

**Endpoint:** `S3 /stocks/quotes`

**Description:**

Top of book quotes with nanosecond timestamps from all major U.S. exchanges and darkpools, including NYSE, Nasdaq, Cboe, Finra, and more, made available as a daily downloadable S3 file.


## Columns

| Column | Type | Description |
|--------|------|-------------|
| `ask_exchange` | integer | The ask exchange ID. |
| `ask_price` | number | The ask price. |
| `ask_size` | number | The ask size. This represents the number of shares sellers are offering at the given ask price. |
| `bid_exchange` | integer | The bid exchange ID. |
| `bid_price` | number | The bid price. |
| `bid_size` | number | The bid size. This represents the number of shares buyers are bidding for at the given bid price. |
| `conditions` | integer | A list of condition codes. If there are multiple conditions, they will appear in quotations, separated by commas. |
| `indicators` | integer | A list of indicator codes. If there are multiple indicators, they will appear in quotations, separated by commas. |
| `participant_timestamp` | integer | The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the quote was actually generated at the exchange. |
| `sequence_number` | integer | The sequence number represents the sequence in which quote events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). Values reset after each trading session/day. |
| `sip_timestamp` | integer | The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this quote from the exchange which produced it. |
| `tape` | integer | There are 3 tapes which define which exchange the ticker is listed on. These are integers in our objects which represent the letter of the alphabet. Eg: 1 = A, 2 = B, 3 = C.Tape A is NYSE listed securitiesTape B is NYSE ARCA / NYSE AmericanTape C is NASDAQ |
| `ticker` | string | The exchange symbol that this item is traded under. |
| `trf_timestamp` | integer | The nanosecond accuracy TRF (Trade Reporting Facility) Unix Timestamp. This is the timestamp of when the trade reporting facility received this quote. |


## Example Data

| Ticker | ask_exchange | ask_price | ask_size | bid_exchange | bid_price | bid_size | conditions | indicators | participant_timestamp | sequence_number | sip_timestamp | tape | trf_timestamp |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MSFT | 19 | 0 | 0 | 19 | 0 | 0 | 19 |  | 1679990340007888000 | 941 | 1679990340010084598 | 3 | 0 |
| MSFT | 8 | 0 | 0 | 8 | 0 | 0 | 19 |  | 1679990340086205000 | 1714 | 1679990340087539862 | 3 | 0 |
| MSFT | 0 | 0 | 0 | 12 | 275.33 | 2 | 1 |  | 1679990400004452251 | 3178 | 1679990400004468151 | 3 | 0 |
| MSFT | 11 | 277.01 | 3 | 12 | 275.33 | 2 | 1 |  | 1679990400018372096 | 3886 | 1679990400018726518 | 3 | 0 |
| MSFT | 11 | 277.01 | 8 | 12 | 275.33 | 2 | 1 |  | 1679990400018372864 | 3890 | 1679990400018732381 | 3 | 0 |
| MSFT | 11 | 276.4 | 1 | 12 | 275.33 | 2 | 1 |  | 1679990400018940160 | 3939 | 1679990400019291802 | 3 | 0 |
| MSFT | 11 | 276.4 | 1 | 11 | 275.35 | 1 | 1 |  | 1679990400018981120 | 3940 | 1679990400019332609 | 3 | 0 |
| MSFT | 11 | 276.18 | 1 | 11 | 275.35 | 1 | 1 |  | 1679990400018987776 | 3941 | 1679990400019343335 | 3 | 0 |
| MSFT | 11 | 276.2 | 1 | 11 | 275.35 | 1 | 1 |  | 1679990400023727616 | 4348 | 1679990400024080518 | 3 | 0 |
| MSFT | 11 | 276.23 | 2 | 11 | 275.35 | 1 | 1 |  | 1679990400023774208 | 4351 | 1679990400024125082 | 3 | 0 |


## Plan Access

**Plan Access:** Included in select Stocks plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Not included |
| Stocks Developer | Not included |
| Stocks Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Included |

## Plan Recency

**Plan Recency:** Updated at 11a ET to include the previous day

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Not included |
| Stocks Developer | Not included |
| Stocks Advanced | End-of-day |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | End-of-day |

## Plan History

**Plan History:** Records date back to September 10, 2003

#### Individual Plans

| Plan | History |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Not included |
| Stocks Developer | Not included |
| Stocks Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Stocks Business | All history |
