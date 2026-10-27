> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### Quotes

**Endpoint:** `GET /v3/quotes/{stockTicker}`

**Description:**

Retrieve National Best Bid and Offer (NBBO) quotes for a specified stock ticker over a defined time range. Each record includes the prevailing best bid/ask prices, sizes, exchanges, and timestamps, reflecting the top-of-book quote environment at each moment. By examining this historical quote data, users can analyze price movements, evaluate liquidity at the NBBO level, and enhance trading strategies or research efforts.

Use Cases: Historical quote analysis, liquidity evaluation, algorithmic backtesting, trading strategy refinement.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `stockTicker` | string | Yes | Specify a case-sensitive ticker symbol. For example, AAPL represents Apple Inc. |
| `timestamp` | string | No | Query by timestamp. Either a date with the format YYYY-MM-DD or a nanosecond timestamp. |
| `timestamp.gte` | string | No | Range by timestamp. |
| `timestamp.gt` | string | No | Range by timestamp. |
| `timestamp.lte` | string | No | Range by timestamp. |
| `timestamp.lt` | string | No | Range by timestamp. |
| `order` | string | No | Order results based on the `sort` field. |
| `limit` | integer | No | Limit the number of results returned, default is 1000 and max is 50000. |
| `sort` | string | No | Sort field used for ordering. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page of data. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | An array of results containing the requested data. |
| `results[].ask_exchange` | integer | The ask exchange ID |
| `results[].ask_price` | number | The ask price. |
| `results[].ask_size` | number | The ask size. This represents the number of shares sellers are offering at the given ask price. |
| `results[].bid_exchange` | integer | The bid exchange ID |
| `results[].bid_price` | number | The bid price. |
| `results[].bid_size` | number | The bid size. This represents the number of shares buyers are bidding for at the given bid price. |
| `results[].conditions` | array[integer] | A list of condition codes. See [Stocks Condition Codes](https://massive.com/docs/rest/stocks/market-operations/condition-codes) for what each code means. |
| `results[].indicators` | array[integer] | A list of indicator codes. See the [Conditions and Indicators](https://massive.com/glossary/conditions-indicators) glossary for what each code means. |
| `results[].participant_timestamp` | integer | The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the quote was actually generated at the exchange. |
| `results[].sequence_number` | integer | The sequence number represents the sequence in which quote events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). Values reset after each trading session/day. |
| `results[].sip_timestamp` | integer | The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this quote from the exchange which produced it. |
| `results[].tape` | integer | There are 3 tapes which define which exchange the ticker is listed on. These are integers in our objects which represent the letter of the alphabet. Eg: 1 = A, 2 = B, 3 = C. * Tape A is NYSE listed securities * Tape B is NYSE ARCA / NYSE American * Tape C is NASDAQ |
| `results[].trf_timestamp` | integer | The nanosecond accuracy TRF (Trade Reporting Facility) Unix Timestamp. This is the timestamp of when the trade reporting facility received this quote. |
| `status` | string | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/v3/quotes/AAPL?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIxLTA0LTI1JmxpbWl0PTEmb3JkZXI9YXNjJnBhZ2VfbWFya2VyPUElN0M5YWRjMjY0ZTgyM2E1ZjBiOGUyNDc5YmZiOGE1YmYwNDVkYzU0YjgwMDcyMWE2YmI1ZjBjMjQwMjU4MjFmNGZiJnNvcnQ9dGlja2Vy",
  "request_id": "a47d1beb8c11b6ae897ab76cdbbf35a3",
  "results": [
    {
      "ask_exchange": 0,
      "ask_price": 0,
      "ask_size": 0,
      "bid_exchange": 11,
      "bid_price": 102.7,
      "bid_size": 60,
      "conditions": [
        1
      ],
      "participant_timestamp": 1517562000065321200,
      "sequence_number": 2060,
      "sip_timestamp": 1517562000065700400,
      "tape": 3
    },
    {
      "ask_exchange": 0,
      "ask_price": 0,
      "ask_size": 0,
      "bid_exchange": 11,
      "bid_price": 170,
      "bid_size": 2,
      "conditions": [
        1
      ],
      "participant_timestamp": 1517562000065408300,
      "sequence_number": 2061,
      "sip_timestamp": 1517562000065791500,
      "tape": 3
    }
  ],
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
| Stocks Business | Included |
| Stocks Business + Expansion | Included |

## Plan Recency

**Plan Recency:** End-of-day or real-time

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
| Stocks Business | End-of-day |
| Stocks Business + Expansion | Real-time |

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
| Stocks Business + Expansion | All history |
