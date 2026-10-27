> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Indices

### Aggregates (Per Minute)

## Connection

| Feed | URL |
| --- | --- |
| Delayed | `wss://delayed.massive.com/indices` |
| Real-Time | `wss://socket.massive.com/indices` |

Connect to the URL for the feed that your plan includes. See the plan table below.

## Authentication

After you connect, send your API key in an `auth` message:

```json
{"action":"auth","params":"YOUR_API_KEY"}
```

The server replies with `[{"ev":"status","status":"auth_success","message":"authenticated"}]`. Send subscribe messages only after this reply.

## Subscription

The channel prefix for this feed is `AM`. To subscribe, send `AM.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"AM.*"}
```

Separate more than one subscription with commas, for example `AM.<ticker1>,AM.<ticker2>`.
The `AM.*` value subscribes to all tickers.

**Description:**

Stream minute-by-minute aggregated OHLC (Open, High, Low, Close) for a specified index via WebSocket. These aggregates update continuously in Eastern Time (ET) and capture changes in the index’s values. Unlike stocks or options, index aggregates are derived from index values rather than individual trades. If no new index updates occur within a given minute, no bar is emitted. By providing an ongoing feed of updated market snapshots, this endpoint enables users to track intraday index movements, refine analysis, and power real-time market visualizations.

Use Cases: Real-time monitoring, dynamic charting, intraday trend analysis, market research.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify an index ticker using "I:" prefix or use * to subscribe to all index tickers. You can also use a comma separated list to subscribe to multiple index tickers. You can retrieve available index tickers from our [Index Tickers API](https://massive.com/docs/rest/indices/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: AM | The event type. |
| `sym` | string | The symbol representing the given index. |
| `op` | number | Today's official opening value. |
| `o` | number | The opening index value for this aggregate window. |
| `c` | number | The closing index value for this aggregate window. |
| `h` | number | The highest index value for this aggregate window. |
| `l` | number | The lowest index value for this aggregate window. |
| `s` | integer | The start timestamp of this aggregate window in Unix Milliseconds. |
| `e` | integer | The end timestamp of this aggregate window in Unix Milliseconds. |

## Sample Response

```json
{
  "ev": "AM",
  "sym": "I:SPX",
  "op": 3985.67,
  "o": 3985.67,
  "c": 3985.67,
  "h": 3985.67,
  "l": 3985.67,
  "s": 1678220675805,
  "e": 1678220675805
}
```


## Plan Access

**Plan Access:** Included in select Indices plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Indices Basic | Not included |
| Indices Starter | Included |
| Indices Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Indices Business | Included |

## Plan Recency

**Plan Recency:** 15-minute delayed or real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Indices Basic | Not included |
| Indices Starter | 15-minute delayed |
| Indices Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Indices Business | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
