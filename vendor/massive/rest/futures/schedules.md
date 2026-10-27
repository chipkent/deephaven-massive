> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Futures

### Schedules

**Endpoint:** `GET /futures/v1/schedules`

**Description:**

The Schedules API provides a unified way to retrieve trading schedules for futures markets, returning precise session open and close times, intraday breaks, and any adjustments for holidays or special events. You can filter schedules by session_end_date or retrieve the schedule for a single product using its product code. All times are returned in Coordinated Universal Time (UTC), making it straightforward to align trading, execution, and operational workflows across systems.

This endpoint currently supports 2 years of history.

Use Cases: Schedule planning, market analysis, strategy alignment, risk and operations management.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `product_code` | string | No | The product code of the futures contract. |
| `product_code.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `product_code.gt` | string | No | Filter greater than the value. |
| `product_code.gte` | string | No | Filter greater than or equal to the value. |
| `product_code.lt` | string | No | Filter less than the value. |
| `product_code.lte` | string | No | Filter less than or equal to the value. |
| `session_end_date` | string | No | The session end date for the schedules (also known as the trading date). This field is optional and can be used to filter results by a specific session end date. If left blank, schedules for all dates will be returned. Note that trading sessions end at 5 PM Central Time, so a session ending at 5 PM CT on January 1st would have a session_end_date of 2025-01-01. Value must be formatted 'yyyy-mm-dd'. |
| `session_end_date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `session_end_date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `session_end_date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `session_end_date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `trading_venue` | string | No | The trading venue (MIC) for the exchange on which this schedule's product trades. |
| `trading_venue.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `trading_venue.gt` | string | No | Filter greater than the value. |
| `trading_venue.gte` | string | No | Filter greater than or equal to the value. |
| `trading_venue.lt` | string | No | Filter less than the value. |
| `trading_venue.lte` | string | No | Filter less than or equal to the value. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '10' if not specified. The maximum allowed limit is '1000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'product_code' if not specified. The sort order defaults to 'asc' if not specified. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].event` | string | The type of session on the given trading date. |
| `results[].product_code` | string | The product code of the futures contract. |
| `results[].product_name` | string | The name of the futures product to which this schedule applies. |
| `results[].session_end_date` | string | The session end date for the schedules (also known as the trading date). This field is optional and can be used to filter results by a specific session end date. If left blank, schedules for all dates will be returned. Note that trading sessions end at 5 PM Central Time, so a session ending at 5 PM CT on January 1st would have a session_end_date of 2025-01-01. |
| `results[].timestamp` | string | The timestamp for the given market event. |
| `results[].trading_venue` | string | The trading venue (MIC) for the exchange on which this schedule's product trades. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.staging.massive.com/futures/v2/schedules?cursor=AQANA0VSTAIAAAEFAAEBAwACAQ0DRVJMAQ0ZMjAyNC0wNi0xMFQyMTowMDowMCswMDowMA==",
  "request_id": "a83620d1ec6a4cd5b84ea669e377fd47",
  "results": [
    {
      "event": "pre_open",
      "product_code": "ERL",
      "product_name": "ERCOT North 345 kV Hub Day-Ahead 5 MW Off-Peak Futures",
      "session_end_date": "2024-06-10",
      "timestamp": "2024-06-09T21:00:00+00:00",
      "trading_venue": "XNYM"
    },
    {
      "event": "open",
      "product_code": "ERL",
      "product_name": "ERCOT North 345 kV Hub Day-Ahead 5 MW Off-Peak Futures",
      "session_end_date": "2024-06-10",
      "timestamp": "2024-06-09T22:00:00+00:00",
      "trading_venue": "XNYM"
    },
    {
      "event": "close",
      "product_code": "ERL",
      "product_name": "ERCOT North 345 kV Hub Day-Ahead 5 MW Off-Peak Futures",
      "session_end_date": "2024-06-10",
      "timestamp": "2024-06-10T21:00:00+00:00",
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

**Plan Recency:** Updated daily

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Futures Basic | Updated daily |
| Futures Starter | Updated daily |
| Futures Developer | Updated daily |
| Futures Advanced | Updated daily |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Futures Business CME | Updated daily |
| Futures Business CBOT | Updated daily |
| Futures Business NYMEX | Updated daily |
| Futures Business COMEX | Updated daily |

## Plan History

**Plan History:** Records date back to March 17, 2025

#### Individual Plans

| Plan | History |
| --- | --- |
| Futures Basic | 2 years |
| Futures Starter | 2 years |
| Futures Developer | 2 years |
| Futures Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Futures Business CME | All history |
| Futures Business CBOT | All history |
| Futures Business NYMEX | All history |
| Futures Business COMEX | All history |
