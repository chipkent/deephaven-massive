> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### Last Quote

**Endpoint:** `GET /v2/last/nbbo/{stocksTicker}`

**Description:**

Retrieve the most recent National Best Bid and Offer (NBBO) quote for a specified stock ticker, including the latest bid/ask prices, sizes, exchange details, and timestamp. This endpoint supports monitoring current market conditions and updating platforms or applications with near-term quote information, allowing users to evaluate liquidity, track spreads, and make more informed decisions.

Use Cases: Price display, spread analysis, market monitoring.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `stocksTicker` | string | Yes | Specify a case-sensitive ticker symbol. For example, AAPL represents Apple Inc. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `request_id` | string | A request id assigned by the server. |
| `results` | object | N/A |
| `results.P` | number | The ask price. |
| `results.S` | integer | The ask size. This represents the number of shares sellers are offering at the given ask price. |
| `results.T` | string | The exchange symbol that this item is traded under. |
| `results.X` | integer | The exchange ID. See <a href="https://massive.com/docs/rest/stocks/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `results.c` | array[integer] | A list of condition codes. See [Stocks Condition Codes](https://massive.com/docs/rest/stocks/market-operations/condition-codes) for what each code means. |
| `results.f` | integer | The nanosecond accuracy TRF(Trade Reporting Facility) Unix Timestamp. This is the timestamp of when the trade reporting facility received this message. |
| `results.i` | array[integer] | A list of indicator codes. See the [Conditions and Indicators](https://massive.com/glossary/conditions-indicators) glossary for what each code means. |
| `results.p` | number | The bid price. |
| `results.q` | integer | The sequence number represents the sequence in which message events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). |
| `results.s` | integer | The bid size. This represents the number of shares buyers are bidding for at the given bid price. |
| `results.t` | integer | The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this message from the exchange which produced it. |
| `results.x` | integer | The exchange ID. See <a href="https://massive.com/docs/rest/stocks/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `results.y` | integer | The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the quote was actually generated at the exchange. |
| `results.z` | integer | There are 3 tapes which define which exchange the ticker is listed on. These are integers in our objects which represent the letter of the alphabet. Eg: 1 = A, 2 = B, 3 = C. * Tape A is NYSE listed securities * Tape B is NYSE ARCA / NYSE American * Tape C is NASDAQ |
| `status` | string | The status of this request's response. |

## Sample Response

```json
{
  "request_id": "b84e24636301f19f88e0dfbf9a45ed5c",
  "results": {
    "P": 127.98,
    "S": 7,
    "T": "AAPL",
    "X": 19,
    "p": 127.96,
    "q": 83480742,
    "s": 1,
    "t": 1617827221349730300,
    "x": 11,
    "y": 1617827221349366000,
    "z": 3
  },
  "status": "OK"
}
```


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
| Stocks Business | Not included |
| Stocks Business + Expansion | Included |

## Plan Recency

**Plan Recency:** Real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Not included |
| Stocks Developer | Not included |
| Stocks Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Not included |
| Stocks Business + Expansion | Real-time |

## Plan History

**Plan History:** Not applicable to this endpoint
