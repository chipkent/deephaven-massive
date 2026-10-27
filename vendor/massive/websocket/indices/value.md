> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Indices

### Value

## Connection

| Feed | URL |
| --- | --- |
| Business | `wss://business.massive.com/indices` |

Connect to the URL for the feed that your plan includes. See the plan table below.

## Authentication

After you connect, send your API key in an `auth` message:

```json
{"action":"auth","params":"YOUR_API_KEY"}
```

The server replies with `[{"ev":"status","status":"auth_success","message":"authenticated"}]`. Send subscribe messages only after this reply.

## Subscription

The channel prefix for this feed is `V`. To subscribe, send `V.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"V.I:SPX"}
```

Separate more than one subscription with commas, for example `V.<ticker1>,V.<ticker2>`.
The `V.*` value subscribes to all tickers.

**Description:**

Stream real-time index value updates for specified index tickers via WebSocket. Each message provides the index’s current value and timestamp, allowing users to monitor fluctuations in key market benchmarks or sectors throughout the trading session. This continuous feed supports live market analysis, trend identification, and integration of index-based signals into trading strategies and research.

Use Cases: Market analysis, trend detection, portfolio benchmarking, trading strategy refinement.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify an index ticker using "I:" prefix or use * to subscribe to all index tickers. You can also use a comma separated list to subscribe to multiple index tickers. You can retrieve available index tickers from our [Index Tickers API](https://massive.com/docs/rest/indices/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: V | The event type. |
| `val` | number | The value of the index. |
| `T` | string | The assigned ticker of the index. |
| `t` | integer | The Timestamp in Unix MS. |

## Sample Response

```json
{
  "ev": "V",
  "val": 3988.5,
  "T": "I:SPX",
  "t": 1678220098130
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
