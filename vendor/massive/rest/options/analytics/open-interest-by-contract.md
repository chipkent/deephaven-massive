> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Options

### Open Interest by Contract

**Endpoint:** `GET /options/v1/open-interest`

**Description:**

Retrieve daily open interest for US options contracts as reported by OPRA, one row
per contract per trading day. Each record includes the contract's underlying ticker,
OCC root, contract type, expiration date, strike price, and its open interest. Filter
by `ticker` or `root`, and by `date` for a single day or a date range.

The `date` field is the trading session each figure is for. New data arrives every
morning for the prior session, so the latest `date` available is the previous
trading day.

Use Cases: Open interest trend analysis, liquidity and positioning research,
options flow modeling, backtesting.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | No | The options ticker symbol (e.g., O:SPY260123C00687000). |
| `ticker.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `ticker.gt` | string | No | Filter greater than the value. |
| `ticker.gte` | string | No | Filter greater than or equal to the value. |
| `ticker.lt` | string | No | Filter less than the value. |
| `ticker.lte` | string | No | Filter less than or equal to the value. |
| `root` | string | No | The OCC root symbol of the contract (e.g., SPXW). Differs from the underlying for weekly roots and for contracts adjusted after a corporate action (e.g., AMCR1). |
| `root.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `root.gt` | string | No | Filter greater than the value. |
| `root.gte` | string | No | Filter greater than or equal to the value. |
| `root.lt` | string | No | Filter less than the value. |
| `root.lte` | string | No | Filter less than or equal to the value. |
| `contract_type` | string | No | The contract type. Possible values: call, put. |
| `contract_type.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `expiration_date` | string | No | The date the contract expires (formatted as YYYY-MM-DD). Value must be formatted 'yyyy-mm-dd'. |
| `expiration_date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `expiration_date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `expiration_date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `expiration_date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `date` | string | No | The trading date the open interest applies to (formatted as YYYY-MM-DD). Value must be formatted 'yyyy-mm-dd'. |
| `date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'date' if not specified. The sort order defaults to 'desc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `contract_type,date,expiration_date`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].contract_type` | string | The contract type. Possible values: call, put. |
| `results[].date` | string | The trading date the open interest applies to (formatted as YYYY-MM-DD). |
| `results[].expiration_date` | string | The date the contract expires (formatted as YYYY-MM-DD). |
| `results[].open_interest` | integer | The number of outstanding contracts that have not been closed or exercised, as of the last open interest update of the day. |
| `results[].root` | string | The OCC root symbol of the contract (e.g., SPXW). Differs from the underlying for weekly roots and for contracts adjusted after a corporate action (e.g., AMCR1). |
| `results[].strike_price` | number | The strike price of the contract. |
| `results[].ticker` | string | The options ticker symbol (e.g., O:SPY260123C00687000). |
| `results[].underlying_ticker` | string | The ticker of the contract's underlying (e.g., SPX). Weekly and adjusted roots are folded in, so SPXW contracts report SPX and AMCR1 contracts report AMCR; see root to tell them apart. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/options/v1/open-interest?cursor=YXA9ZGF0ZT0yMDI2LTAxLTIyJnRpY2tlcj1POlNQWTI2MDEyM0MwMDY4NzAwMCZsaW1pdD0yJnNvcnQ9ZGF0ZS5kZXNj",
  "request_id": "6f1d2c9a8b7e4d3c2b1a0f9e8d7c6b5a",
  "results": [
    {
      "contract_type": "put",
      "date": "2026-01-22",
      "expiration_date": "2026-01-23",
      "open_interest": 9871,
      "root": "SPY",
      "strike_price": 687,
      "ticker": "O:SPY260123P00687000",
      "underlying_ticker": "SPY"
    },
    {
      "contract_type": "call",
      "date": "2026-01-22",
      "expiration_date": "2026-01-23",
      "open_interest": 15234,
      "root": "SPY",
      "strike_price": 687,
      "ticker": "O:SPY260123C00687000",
      "underlying_ticker": "SPY"
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
