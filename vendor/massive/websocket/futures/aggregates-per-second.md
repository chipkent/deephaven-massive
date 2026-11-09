> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Futures

### Aggregates (Per Second)

## Connection

| Feed | URL |
| --- | --- |
| Delayed | `wss://delayed.massive.com/futures` |
| Real-Time | `wss://socket.massive.com/futures` |

Connect to the URL for the feed that your plan includes. See the plan table below.

## Authentication

After you connect, send your API key in an `auth` message:

```json
{"action":"auth","params":"YOUR_API_KEY"}
```

The server replies with `[{"ev":"status","status":"auth_success","message":"authenticated"}]`. Send subscribe messages only after this reply.

## Subscription

The channel prefix for this feed is `A`. To subscribe, send `A.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"A.*"}
```

Separate more than one subscription with commas, for example `A.<value1>,A.<value2>`.
The `A.*` value subscribes to all `ticker` values.

**Description:**

Stream second-by-second aggregated OHLC (Open, High, Low, Close) and volume data for a specified futures contract ticker via WebSocket. Aggregates are continuously updated in Central Time (CT) and are constructed from all trades occurring within each defined aggregation window. If no trades occur during the window, no aggregate bar is emitted, indicating a period of inactivity. The response includes key metrics such as open, high, low, close, trade volume, and the start and end timestamps for each window.

Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, automated trading.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a future ticker or use * to subscribe to all future tickers. To subscribe to more than one contract, send a comma separated list with the channel prefix on each entry, for example `T.ESZ4,T.NQZ4`. You can retrieve available future tickers from our [Futures Contracts API](https://massive.com/docs/rest/futures/contracts/contract-overview).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: A | The event type. |
| `sym` | string | The ticker symbol for the given future. |
| `v` | number | The tick volume. |
| `dv` | number | The total US dollar value of shares traded within the aggregate window. |
| `o` | number | The opening tick price for this aggregate window. |
| `c` | number | The closing tick price for this aggregate window. |
| `h` | number | The highest tick price for this aggregate window. |
| `l` | number | The lowest tick price for this aggregate window. |
| `n` | number | The total number of transactions that occurred within the aggregate window |
| `s` | integer | The start timestamp of this aggregate window in Unix Milliseconds. |
| `e` | integer | The end timestamp of this aggregate window in Unix Milliseconds. |

## Sample Response

```json
{
  "ev": "A",
  "sym": "6CH5",
  "v": 91,
  "dv": 1353.1,
  "o": 6994.5,
  "c": 6995,
  "h": 6995,
  "l": 6994.5,
  "n": 10,
  "s": 1734717060000,
  "e": 1734717120000
}
```


## Plan Access

**Plan Access:** Included in select Futures plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Futures Basic | Not included |
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

**Plan Recency:** 10-minute delayed or real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Futures Basic | Not included |
| Futures Starter | 10-minute delayed |
| Futures Developer | 10-minute delayed |
| Futures Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Futures Business CME | Real-time |
| Futures Business CBOT | Real-time |
| Futures Business NYMEX | Real-time |
| Futures Business COMEX | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
