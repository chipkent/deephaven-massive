> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Futures

### Trades

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

The channel prefix for this feed is `T`. To subscribe, send `T.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"T.*"}
```

Separate more than one subscription with commas, for example `T.<value1>,T.<value2>`.
The `T.*` value subscribes to all `ticker` values.

**Description:**

Stream tick-level trade data for futures tickers via WebSocket. Each message delivers key trade details (price, size, exchange, conditions, and timestamps) as they occur, enabling users to track market activity, power live dashboards, and inform rapid decision-making.

Use Cases: Live monitoring, algorithmic trading, market analysis, data visualization.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a future ticker or use * to subscribe to all future tickers. To subscribe to more than one contract, send a comma separated list with the channel prefix on each entry, for example `T.ESZ4,T.NQZ4`. You can retrieve available future tickers from our [Futures Contracts API](https://massive.com/docs/rest/futures/contracts/contract-overview).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: T | The event type. |
| `sym` | string | The ticker symbol for the given future. |
| `p` | number | The trade price is quoted per unit of the underlying asset, with the total contract value determined by multiplying by the contract’s specific multiplier. |
| `s` | integer | The trade size shows the number of futures contracts actually exchanged. |
| `t` | integer | The timestamp in Unix MS. |
| `q` | integer | The sequence number represents the sequence in which message events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).  |

## Sample Response

```json
{
  "ev": "T",
  "sym": "ESZ4",
  "z": 3,
  "p": 606450,
  "s": 100,
  "t": 1734103628363,
  "q": 32599300
}
```


## Plan Access

**Plan Access:** Included in select Futures plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Futures Basic | Not included |
| Futures Starter | Not included |
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
| Futures Starter | Not included |
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
