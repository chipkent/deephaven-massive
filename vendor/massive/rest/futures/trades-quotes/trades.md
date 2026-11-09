> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Futures

### Trades

**Endpoint:** `GET /futures/v1/trades/{ticker}`

**Description:**

Retrieve comprehensive, tick-level trade data for a specified futures contract ticker over a defined time range. Each record includes the trade price, size, session end date, and precise timestamps, capturing individual trade events throughout the period. This granular data is essential for constructing aggregated bars and performing detailed analyses of intraday price movements, making it a valuable tool for backtesting, algorithmic strategy development, and market research.

Use Cases: Intraday analysis, algorithmic trading, backtesting, market research.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `timestamp` | string | No | The time when the trade was generated at the exchange to nanosecond precision. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.gt` | string | No | Filter greater than the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.gte` | string | No | Filter greater than or equal to the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.lt` | string | No | Filter less than the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.lte` | string | No | Filter less than or equal to the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `session_end_date` | string | No | Also known as the trading date, the date of the end of the trading session, in YYYY-MM-DD format. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '10' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'timestamp' if not specified. The sort order defaults to 'desc' if not specified. |
| `ticker` | string | Yes | The futures contract identifier, including the base symbol and contract expiration (e.g., GCJ5 for the April 2025 gold contract). |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `channel,price,report_sequence`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].channel` | integer | The CME multicast channel this event was sourced from. |
| `results[].price` | number | The price of the trade. This is the actual dollar value per whole contract of this trade. A trade of 100 contracts with a price of $2.00 would be worth a total dollar value of $200.00. |
| `results[].report_sequence` | integer | The reporting sequence number. |
| `results[].sequence_number` | integer | The unique sequence number assigned to this trade. |
| `results[].session_end_date` | string | Also known as the trading date, the date of the end of the trading session, in YYYY-MM-DD format. |
| `results[].size` | integer | The total number of contracts exchanged between buyers and sellers on a given trade. |
| `results[].ticker` | string | The futures contract identifier, including the base symbol and contract expiration (e.g., GCJ5 for the April 2025 gold contract). |
| `results[].timestamp` | integer | The time when the trade was generated at the exchange to nanosecond precision. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/futures/v1/trades/ESZ4?cursor=YXA9MTczNDQ3MDk3MDAwODU5OTI2MSZhcz0yNzIzNTI4MyZsaW1pdD0xMDAwJnNlc3Npb25fZW5kX2RhdGU9MjAyNC0xMi0xNyZzb3J0PXRpbWVzdGFtcC5kZXNj",
  "request_id": "a47d1beb8c11b6ae897ab76cdbbf35a3",
  "results": [
    {
      "price": 6052,
      "report_sequence": 12033289,
      "sequence_number": 27317882,
      "session_end_date": "2024-12-17",
      "size": 3,
      "ticker": "ESZ4",
      "timestamp": 1734472799000509200
    },
    {
      "price": 6051.75,
      "report_sequence": 12033276,
      "sequence_number": 27317863,
      "session_end_date": "2024-12-17",
      "size": 1,
      "ticker": "ESZ4",
      "timestamp": 1734472798789679900
    },
    {
      "price": 6052,
      "report_sequence": 12033255,
      "sequence_number": 27317826,
      "session_end_date": "2024-12-17",
      "size": 2,
      "ticker": "ESZ4",
      "timestamp": 1734472797000893000
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
