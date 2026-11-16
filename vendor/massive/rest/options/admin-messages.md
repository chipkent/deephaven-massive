> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Options

### Admin & FLEX Messages

**Endpoint:** `GET /options/v1/admin-messages`

**Description:**

Retrieve OPRA administrative messages for US options: the free-text channel of the
OPRA feed, carrying FLEX option prints and quotes, operational notices, and other
messages that don't fit a fixed format. Each record delivers the raw administrative
text along with its exchange, sequence number, and nanosecond SIP timestamp, and is
written once and never modified. Filter by `exchange`, and by a `timestamp` range.

FLEX (FLexible EXchange) options are customized, exchange-traded options contracts that
let counterparties negotiate non-standard terms (such as the strike price, expiration
date, exercise style, and settlement type) beyond what standard listed options offer.
FLEX trades are disseminated by OPRA as administrative messages and surfaced through
this endpoint.

Use Cases: FLEX trade monitoring, market operations tracking, data auditing.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `timestamp.gt` | string | No | Filter greater than the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.gte` | string | No | Filter greater than or equal to the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.lt` | string | No | Filter less than the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `timestamp.lte` | string | No | Filter less than or equal to the value. Value must be an integer timestamp in nanoseconds, formatted 'yyyy-mm-dd', or ISO 8601/RFC 3339 (e.g. '2024-05-28T20:27:41Z'). |
| `exchange` | integer | No | The exchange ID. See Exchanges mapping for details. Value must be an integer. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '1000' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'timestamp' if not specified. The sort order defaults to 'asc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `exchange,id,message`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].exchange` | integer | The exchange ID. See Exchanges mapping for details. |
| `results[].id` | string | The unique identifier for this admin message, composed of the SIP timestamp and the sequence number. |
| `results[].message` | string | The administrative message text as distributed by OPRA. |
| `results[].retransmission` | boolean | True if this record was retransmitted. Omitted when false. |
| `results[].sequence_number` | integer | The sequence number represents the sequence in which admin events happened. These are increasing and unique, but will not always be sequential. |
| `results[].timestamp` | integer | The SIP timestamp in Unix nanoseconds. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/options/v1/admin-messages?cursor=YXA9dGltZXN0YW1wPTE3OTM5MTE0NDI5MzA1NjgwNzkmbGltaXQ9MiZzb3J0PXRpbWVzdGFtcC5hc2M",
  "request_id": "9b1c3f7a2e5d4b8c6f0a1d2e3b4c5d6e",
  "results": [
    {
      "exchange": 302,
      "id": "1793911442930568079-4043826282",
      "message": "FLEX AAPL 20270115 C 00200000 PRINT 150 @ 12.35",
      "sequence_number": 4043826282,
      "timestamp": 1793911442930568200
    },
    {
      "exchange": 300,
      "id": "1793911460115302244-4043826283",
      "message": "OPRA SYSTEM NORMAL",
      "retransmission": true,
      "sequence_number": 4043826283,
      "timestamp": 1793911460115302100
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
| Real-time FLEX Trades Business | Included |

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
| Real-time FLEX Trades Business | Real-time |

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
| Real-time FLEX Trades Business | All history |
