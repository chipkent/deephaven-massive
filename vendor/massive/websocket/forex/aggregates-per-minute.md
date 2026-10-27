> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Forex

### Aggregates (Per Minute)

## Connection

| Feed | URL |
| --- | --- |
| Real-Time | `wss://socket.massive.com/forex` |

Connect to the URL for the feed that your plan includes. See the plan table below.

## Authentication

After you connect, send your API key in an `auth` message:

```json
{"action":"auth","params":"YOUR_API_KEY"}
```

The server replies with `[{"ev":"status","status":"auth_success","message":"authenticated"}]`. Send subscribe messages only after this reply.

## Subscription

The channel prefix for this feed is `CA`. To subscribe, send `CA.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"CA.*"}
```

Separate more than one subscription with commas, for example `CA.<ticker1>,CA.<ticker2>`.
The `CA.*` value subscribes to all tickers.

**Description:**

Stream minute-by-minute aggregated OHLC (Open, High, Low, Close) and volume data for a specified Forex currency pair via WebSocket. These aggregates update continuously in Coordinated Universal Time (UTC) and are derived from the best bid/offer quotes rather than executed trades. If no new quotes occur within a given minute, no bar is emitted, transparently indicating a period without market updates. By providing a continuous feed of updated market snapshots, this endpoint supports intraday analysis, dynamic charting, and the refinement of real-time Forex trading strategies.

Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, market research.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a forex pair in the format {from}-{to} or use * to subscribe to all forex pairs. You can also use a comma separated list to subscribe to multiple forex pairs. You can retrieve active forex tickers from our [Forex Tickers API](https://massive.com/docs/rest/forex/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: CA | The event type. |
| `pair` | string | The currency pair. |
| `o` | number | The open price for this aggregate window. |
| `c` | number | The close price for this aggregate window. |
| `h` | number | The high price for this aggregate window. |
| `l` | number | The low price for this aggregate window. |
| `v` | integer | The volume of trades during this aggregate window. |
| `s` | integer | The start timestamp of this aggregate window in Unix Milliseconds. |
| `e` | integer | The end timestamp of this aggregate window in Unix Milliseconds. |

## Sample Response

```json
{
  "ev": "CA",
  "pair": "USD/EUR",
  "o": 0.8687,
  "c": 0.86889,
  "h": 0.86889,
  "l": 0.8686,
  "v": 20,
  "s": 1539145740000
}
```


## Plan Access

**Plan Access:** Included in select Currencies plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Currencies Basic | Not included |
| Currencies Starter | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Currencies Business | Included |

## Plan Recency

**Plan Recency:** Real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Currencies Basic | Not included |
| Currencies Starter | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Currencies Business | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
