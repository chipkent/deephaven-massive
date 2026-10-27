> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Crypto

### Aggregates (Per Minute)

## Connection

| Feed | URL |
| --- | --- |
| Real-Time | `wss://socket.massive.com/crypto` |

Connect to the URL for the feed that your plan includes. See the plan table below.

## Authentication

After you connect, send your API key in an `auth` message:

```json
{"action":"auth","params":"YOUR_API_KEY"}
```

The server replies with `[{"ev":"status","status":"auth_success","message":"authenticated"}]`. Send subscribe messages only after this reply.

## Subscription

The channel prefix for this feed is `XA`. To subscribe, send `XA.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"XA.*"}
```

Separate more than one subscription with commas, for example `XA.<ticker1>,XA.<ticker2>`.
The `XA.*` value subscribes to all tickers.

**Description:**

Stream minute-by-minute aggregated OHLC (Open, High, Low, Close) and volume data for a specified cryptocurrency pair via WebSocket. These aggregates update continuously in Coordinated Universal Time (UTC). If no trades occur within a given minute, no bar is emitted, transparently indicating a period without trading activity. This endpoint provides a live feed of aggregated bars, enabling users to monitor intraday price movements, refine trading strategies, and power real-time crypto market visualizations.

Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, market research.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a crypto pair in the format {from}-{to} or use * to subscribe to all crypto pairs. You can also use a comma separated list to subscribe to multiple crypto pairs. You can retrieve active crypto tickers from our [Crypto Tickers API](https://massive.com/docs/rest/crypto/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: XA | The event type. |
| `pair` | string | The crypto pair. |
| `o` | number | The open price for this aggregate window. |
| `c` | number | The close price for this aggregate window. |
| `h` | number | The high price for this aggregate window. |
| `l` | number | The low price for this aggregate window. |
| `v` | integer | The volume of trades during this aggregate window. |
| `s` | integer | The start timestamp of this aggregate window in Unix Milliseconds. |
| `e` | integer | The end timestamp of this aggregate window in Unix Milliseconds. |
| `vw` | number | The volume weighted average price. |
| `z` | integer | The average trade size for this aggregate window. |

## Sample Response

```json
{
  "ev": "XA",
  "pair": "BCD-USD",
  "v": 951.6112,
  "vw": 0.7756,
  "z": 73,
  "o": 0.772,
  "c": 0.784,
  "h": 0.784,
  "l": 0.771,
  "s": 1610463240000,
  "e": 1610463300000
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
