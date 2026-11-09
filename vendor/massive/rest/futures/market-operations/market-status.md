> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Futures

### Market Status

**Endpoint:** `GET /futures/v1/market-status`

**Description:**

Retrieve the current market status for a specific product or products. This endpoint returns real-time indicators, such as open, pause, close, for futures products, along with the corresponding exchange and product codes and an evaluation timestamp. This information enables users to monitor operational conditions and adjust their trading strategies accordingly.

Use Cases: Real-time monitoring, algorithm scheduling, UI updates, operational planning.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `product_code` | string | No | The product code of the futures contracts for which you want statuses. |
| `product_code.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `product_code.gt` | string | No | Filter greater than the value. |
| `product_code.gte` | string | No | Filter greater than or equal to the value. |
| `product_code.lt` | string | No | Filter less than the value. |
| `product_code.lte` | string | No | Filter less than or equal to the value. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '10' if not specified. The maximum allowed limit is '100'. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `market_event,name,product_code`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].market_event` | string | The current status of the market for the product. |
| `results[].name` | string | The name of the futures product. |
| `results[].product_code` | string | The product code of the futures contracts for which you want statuses. |
| `results[].session_end_date` | string | The trading date for the current session. |
| `results[].timestamp` | string | The timestamp for the given market event. |
| `results[].trading_venue` | string | The trading venue (MIC) for the exchange on which the corresponding product trades. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "request_id": "445ebfcfe5bb4b688b7971e1600c952d",
  "results": [
    {
      "market_event": "open",
      "name": "ERCOT North 345 kV Hub Day-Ahead 5 MW Off-Peak Futures",
      "product_code": "ERL",
      "session_end_date": "2025-12-05",
      "timestamp": "2025-12-04T23:00:00+00:00",
      "trading_venue": "XNYM"
    }
  ],
  "status": "OK"
}
```


## Plan Access

**Plan Access:** Included in all Futures plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Futures Basic | Included |
| Futures Starter | Included |
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

**Plan Recency:** Updated in real time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Futures Basic | Updated in real time |
| Futures Starter | Updated in real time |
| Futures Developer | Updated in real time |
| Futures Advanced | Updated in real time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Futures Business CME | Updated in real time |
| Futures Business CBOT | Updated in real time |
| Futures Business NYMEX | Updated in real time |
| Futures Business COMEX | Updated in real time |

## Plan History

**Plan History:** Not applicable to this endpoint
