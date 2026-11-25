> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Options

### Historical Greeks & IV

**Endpoint:** `GET /options/v1/analytics`

**Description:**

Retrieve daily end-of-day greeks and implied volatility for US options
contracts, one row per listed contract per trading day. Each record includes
delta, gamma, theta, vega, and rho, the solved implied volatility, and the
inputs used to compute them, such as the underlying price, dividend yield, and
risk-free rate, so every value can be traced back to its assumptions. Filter by
`ticker` or `underlying_ticker`, and by `date` for a single day or a date range.

Values are computed from a snapshot of the market taken 15 minutes before the
regular close: 15:45 ET on a standard session, or 12:45 ET on an early-close
day. A contract that cannot be priced on a given day still returns a row, with
null greeks and the inputs that explain why.

Use Cases: Volatility surface construction, risk and exposure analysis, options
strategy backtesting, model validation.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | No | Options contract ticker in Massive format (e.g. O:AMD261121C00220000) |
| `ticker.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `underlying_ticker` | string | No | Underlying symbol |
| `underlying_ticker.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `expiration_date` | string | No | Contract expiration date Value must be formatted 'yyyy-mm-dd'. |
| `contract_type` | string | No | call or put |
| `contract_type.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `date` | string | No | The trading date the values reflect Value must be formatted 'yyyy-mm-dd'. |
| `date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '1000' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'date' if not specified. The sort order defaults to 'desc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `ask,bid,contract_type`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].ask` | number | NBBO ask 15 minutes before the close |
| `results[].bid` | number | NBBO bid 15 minutes before the close |
| `results[].contract_type` | string | call or put |
| `results[].date` | string | The trading date the values reflect |
| `results[].delta` | number | Change in option price per $1 move in the underlying |
| `results[].dividend_yield` | number | The most recent regular dividend by ex-date times its yearly frequency, divided by underlying_price, as a decimal. 0 if none |
| `results[].exercise_style` | string | american or european |
| `results[].expiration_date` | string | Contract expiration date |
| `results[].gamma` | number | Change in delta per $1 move in the underlying |
| `results[].implied_volatility` | number | Annualized implied volatility solved against option_price, as a decimal (0.48 means 48%). Null when the solver cannot produce a value |
| `results[].model` | string | Pricing model: binomial |
| `results[].open_interest` | integer | Open interest as published the morning of date, for the prior trading day's close |
| `results[].option_price` | number | The option price the solver used: the NBBO midpoint 15 minutes before the close |
| `results[].quote_timestamp` | integer | Nanosecond timestamp of the NBBO used |
| `results[].rho` | number | Change in option price per 1 percentage point move in the risk-free rate |
| `results[].risk_free_rate` | number | Annualized Treasury yield at the option's maturity, interpolated on the curve published before date, as a decimal |
| `results[].root` | string | Option root symbol |
| `results[].strike_price` | number | Contract strike price |
| `results[].theta` | number | Change in option price per calendar day |
| `results[].ticker` | string | Options contract ticker in Massive format (e.g. O:AMD261121C00220000) |
| `results[].underlying_price` | number | Underlying price used by the solver: the last SIP trade, or the last published index value, at or before 15 minutes before the close |
| `results[].underlying_ticker` | string | Underlying symbol |
| `results[].underlying_timestamp` | integer | Nanosecond timestamp of the underlying price used |
| `results[].vega` | number | Change in option price per 1 point move in implied volatility |
| `results[].volume` | integer | Contracts traded on date |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "request_id": "c1d4a9e07b2f4f6e9a3b5d8e2f1c0a7b",
  "results": [
    {
      "ask": 9.5,
      "bid": 9.35,
      "contract_type": "call",
      "date": "2026-10-29",
      "delta": 0.5312,
      "dividend_yield": 0,
      "exercise_style": "american",
      "expiration_date": "2026-11-21",
      "gamma": 0.0218,
      "implied_volatility": 0.5137,
      "model": "binomial",
      "open_interest": 14823,
      "option_price": 9.425,
      "quote_timestamp": 1793303099841234000,
      "rho": 0.0689,
      "risk_free_rate": 0.0431,
      "root": "AMD",
      "strike_price": 220,
      "theta": -0.3861,
      "ticker": "O:AMD261121C00220000",
      "underlying_price": 221.35,
      "underlying_ticker": "AMD",
      "underlying_timestamp": 1793303099812004400,
      "vega": 0.2204,
      "volume": 6210
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
