> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Futures

### Quotes

**Endpoint:** `GET /futures/v1/quotes/{ticker}`

**Description:**

Retrieve quote data for a specified futures contract ticker. Each record includes the best bid and offer prices, sizes, and timestamps, reflecting the prevailing quote environment at each moment. This endpoint supports detailed analysis of price dynamics and liquidity conditions to inform trading decisions and market research.

Use Cases: Liquidity analysis, price discovery, trading strategy refinement, market research.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `timestamp` | string | No | The time when the quote was generated at the exchange to nanosecond precision. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.gt` | string | No | Filter greater than the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.gte` | string | No | Filter greater than or equal to the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.lt` | string | No | Filter less than the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.lte` | string | No | Filter less than or equal to the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `session_end_date` | string | No | Also known as the trading date, the date of the end of the trading session, in YYYY-MM-DD format. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'timestamp' if not specified. The sort order defaults to 'desc' if not specified. |
| `ticker` | string | Yes | The futures contract identifier, including the base symbol and contract expiration (e.g., GCJ5 for the April 2025 gold contract). |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].ask_price` | number | The ask price is expressed per unit of the underlying asset, and you apply the contract multiplier to get the full contract value. |
| `results[].ask_size` | integer | The quote size represents the number of futures contracts available at the given ask price. |
| `results[].ask_timestamp` | integer | The time when the ask price was submitted to the exchange. |
| `results[].bid_price` | number | The bid price is expressed per unit of the underlying asset, and you apply the contract multiplier to get the full contract value. |
| `results[].bid_size` | integer | The quote size represents the number of futures contracts available at the given bid price. |
| `results[].bid_timestamp` | integer | The time when the bid price was submitted to the exchange. |
| `results[].channel` | integer | The CME multicast channel this event was sourced from. |
| `results[].report_sequence` | integer | The reporting sequence number. |
| `results[].sequence_number` | integer | The unique sequence number assigned to this quote by the exchange. |
| `results[].session_end_date` | string | Also known as the trading date, the date of the end of the trading session, in YYYY-MM-DD format. |
| `results[].ticker` | string | The futures contract identifier, including the base symbol and contract expiration (e.g., GCJ5 for the April 2025 gold contract). |
| `results[].timestamp` | integer | The time when the quote was generated at the exchange to nanosecond precision. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/futures/v1/quotes/GCJ5?cursor=YXA9MTczNDQ2MDIzMDcwODEyNjI4MSZhcz0mbGltaXQ9MTAwMCZzZXNzaW9uX2VuZF9kYXRlPTIwMjQtMTItMTcmc29ydD10aW1lc3RhbXAuZGVzYw",
  "request_id": "a47d1beb8c11b6ae897ab76cdbbf35a3",
  "results": [
    {
      "ask_size": 0,
      "ask_timestamp": 1734472800076125400,
      "bid_price": 2660,
      "bid_size": 1,
      "bid_timestamp": 1734472800076125400,
      "channel": 360,
      "report_sequence": 2250337,
      "sequence_number": 15357766,
      "session_end_date": "2024-12-17",
      "ticker": "GCJ5",
      "timestamp": 1734472800076125400
    },
    {
      "ask_price": 2686,
      "ask_size": 1,
      "ask_timestamp": 1734472770000588000,
      "bid_price": 2684.7,
      "bid_size": 1,
      "bid_timestamp": 1734472736352455200,
      "channel": 360,
      "report_sequence": 2249866,
      "sequence_number": 15355476,
      "session_end_date": "2024-12-17",
      "ticker": "GCJ5",
      "timestamp": 1734472770000588000
    }
  ],
  "status": "OK"
}
```


## Plan Access

**Plan Access:** Included in select Futures plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Futures Basic | Not included |
| Futures Starter | Not included |
| Futures Developer | Included |
| Futures Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Futures Business CME | Included |
| Futures Business CBOT | Included |
| Futures Business NYMEX | Included |
| Futures Business COMEX | Included |

## Plan Recency

**Plan Recency:** 10-minute delayed or real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Futures Basic | Not included |
| Futures Starter | Not included |
| Futures Developer | 10-minute delayed |
| Futures Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Futures Business CME | Real-time |
| Futures Business CBOT | Real-time |
| Futures Business NYMEX | Real-time |
| Futures Business COMEX | Real-time |

## Plan History

**Plan History:** Records date back to April 3, 2017

#### Individual Plans

| Plan | History |
| --- | --- |
| Futures Basic | Not included |
| Futures Starter | Not included |
| Futures Developer | 5 years |
| Futures Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Futures Business CME | All history |
| Futures Business CBOT | All history |
| Futures Business NYMEX | All history |
| Futures Business COMEX | All history |
