> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Options

### Unusual Options Trades

**Endpoint:** `GET /options/v1/unusual-trades`

**Description:**

Retrieve unusual trades for US options contracts, drawn from real-time data for the
most recent days and from historical trade records before that. A trade qualifies when
its notional premium (price x size x 100) is $100,000 or more, when its size is 1,000
contracts or more, or when it is an intermarket sweep (condition 219). Cancels, late
reports, and compression trades are excluded. Each record includes the options ticker,
underlying ticker, OCC root, price, size, exchange, condition codes, and SIP timestamp,
along with a `flags` list naming every case the trade meets. Filter by `ticker` or
`sip_timestamp`, for a single value or a range.

Use Cases: Options flow and block-trade analysis, sweep detection, unusual activity
screening, positioning research.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | No | The options ticker symbol (e.g., O:SPY260123C00687000). |
| `ticker.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `ticker.gt` | string | No | Filter greater than the value. |
| `ticker.gte` | string | No | Filter greater than or equal to the value. |
| `ticker.lt` | string | No | Filter less than the value. |
| `ticker.lte` | string | No | Filter less than or equal to the value. |
| `sip_timestamp` | string | No | The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this trade from the exchange which produced it. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `sip_timestamp.gt` | string | No | Filter greater than the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `sip_timestamp.gte` | string | No | Filter greater than or equal to the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `sip_timestamp.lt` | string | No | Filter less than the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `sip_timestamp.lte` | string | No | Filter less than or equal to the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'sip_timestamp' if not specified. The sort order defaults to 'desc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `exchange,price,root`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].conditions` | array[integer] | A list of condition codes. See [Options Condition Codes](https://massive.com/docs/rest/options/market-operations/condition-codes) for what each code means. |
| `results[].exchange` | integer | The exchange ID. See <a href="https://massive.com/docs/rest/options/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `results[].flags` | array[string] | The unusual-activity cases the trade meets, as a list. Values: block_premium (notional premium of $100,000 or more), block_size (1,000 contracts or more), iso (intermarket sweep, condition 219). A trade that meets several cases carries every matching flag. |
| `results[].price` | number | The price of the trade. This is the actual dollar value per whole contract of this trade. |
| `results[].root` | string | The OCC root symbol of the contract (e.g., SPXW). Differs from the underlying for weekly roots and for contracts adjusted after a corporate action (e.g., AMCR1). |
| `results[].sequence_number` | integer | The sequence number represents the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential. Values reset after each trading session/day. |
| `results[].sip_timestamp` | integer | The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this trade from the exchange which produced it. |
| `results[].size` | integer | The size of a trade (also known as volume), in contracts. |
| `results[].ticker` | string | The options ticker symbol (e.g., O:SPY260123C00687000). |
| `results[].underlying_ticker` | string | The ticker of the contract's underlying (e.g., SPX). Weekly and adjusted roots are folded in, so SPXW contracts report SPX and AMCR1 contracts report AMCR; see root to tell them apart. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "count": 2,
  "next_url": "https://api.massive.com/options/v1/unusual-trades?cursor=eyJsaW1pd...",
  "request_id": "a3f8b2c1d4e5f6a7",
  "results": [
    {
      "conditions": [
        227
      ],
      "exchange": 316,
      "flags": [
        "block_premium"
      ],
      "price": 15.25,
      "root": "SPY",
      "sequence_number": 112233,
      "sip_timestamp": 1761751432123456800,
      "size": 120,
      "ticker": "O:SPY260123C00687000",
      "underlying_ticker": "SPY"
    },
    {
      "conditions": [
        219
      ],
      "exchange": 304,
      "flags": [
        "block_premium",
        "iso"
      ],
      "price": 42.1,
      "root": "SPXW",
      "sequence_number": 98765,
      "sip_timestamp": 1761751431987654400,
      "size": 25,
      "ticker": "O:SPXW261218C06000000",
      "underlying_ticker": "SPX"
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
| Real-time Unusual Options Trades Business | Included |

## Plan Recency

**Plan Recency:** Real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | Not included |
| Options Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Options Business | Not included |
| Real-time Unusual Options Trades Business | Real-time |

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
| Real-time Unusual Options Trades Business | All history |
