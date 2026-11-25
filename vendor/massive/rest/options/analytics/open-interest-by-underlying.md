> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Options

### Open Interest by Underlying

**Endpoint:** `GET /options/v1/open-interest/underlying`

**Description:**

Retrieve daily open interest for US options rolled up across each underlying's full
chain, as reported by OPRA, with calls and puts split out. Each record totals every
root on the underlying for one trading day. Filter by `underlying_ticker`, and by
`date` for a single day or a date range.

The `date` field is the trading session each figure is for. New data arrives every
morning for the prior session, so the latest `date` available is the previous
trading day.

Use Cases: Underlying-level open interest trends, market-wide positioning,
call/put ratio analysis, options flow modeling.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `underlying_ticker` | string | No | The ticker of the underlying (e.g., SPX). Totals cover every root on the underlying, so weekly roots (SPXW) and roots adjusted after a corporate action (AMCR1) are included. |
| `underlying_ticker.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `underlying_ticker.gt` | string | No | Filter greater than the value. |
| `underlying_ticker.gte` | string | No | Filter greater than or equal to the value. |
| `underlying_ticker.lt` | string | No | Filter less than the value. |
| `underlying_ticker.lte` | string | No | Filter less than or equal to the value. |
| `date` | string | No | The trading date the open interest applies to (formatted as YYYY-MM-DD). Value must be formatted 'yyyy-mm-dd'. |
| `date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'date' if not specified. The sort order defaults to 'desc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `date,open_interest,open_interest_calls`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].date` | string | The trading date the open interest applies to (formatted as YYYY-MM-DD). |
| `results[].open_interest` | integer | Total open interest across every contract on the underlying. |
| `results[].open_interest_calls` | integer | Total open interest across the underlying's call contracts. |
| `results[].open_interest_puts` | integer | Total open interest across the underlying's put contracts. |
| `results[].underlying_ticker` | string | The ticker of the underlying (e.g., SPX). Totals cover every root on the underlying, so weekly roots (SPXW) and roots adjusted after a corporate action (AMCR1) are included. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/options/v1/open-interest/underlying?cursor=YXA9ZGF0ZT0yMDI2LTAxLTIyJnVuZGVybHlpbmdfdGlja2VyPVFRUSZsaW1pdD0yJnNvcnQ9ZGF0ZS5kZXNj",
  "request_id": "0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d",
  "results": [
    {
      "date": "2026-01-23",
      "open_interest": 20987654,
      "open_interest_calls": 8765432,
      "open_interest_puts": 12222222,
      "underlying_ticker": "SPY"
    },
    {
      "date": "2026-01-23",
      "open_interest": 9876543,
      "open_interest_calls": 4321098,
      "open_interest_puts": 5555445,
      "underlying_ticker": "QQQ"
    }
  ],
  "status": "OK"
}
```


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
| Options Business | Not included |
| Historical Options Analytics Business | Included |

## Plan Recency

**Plan Recency:** End-of-day (updated daily)

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
| Options Business | Not included |
| Historical Options Analytics Business | End-of-day |

## Plan History

**Plan History:** Records date back to September 1, 2022

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
| Options Business | Not included |
| Historical Options Analytics Business | All history |
