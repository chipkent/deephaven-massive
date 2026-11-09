> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Futures

### Quotes

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

The channel prefix for this feed is `Q`. To subscribe, send `Q.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"Q.*"}
```

Separate more than one subscription with commas, for example `Q.<value1>,Q.<value2>`.
The `Q.*` value subscribes to all `ticker` values.

**Description:**

Stream BBO (Best Bid and Offer) quote data for futures tickers via WebSocket. Each message provides the current best bid/ask prices, sizes, and related metadata as they update, allowing users to monitor evolving market conditions, inform trading decisions, and maintain responsive, data-driven applications.

Use Cases: Live monitoring, market analysis, trading decision support, dynamic interface updates.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a future ticker or use * to subscribe to all future tickers. To subscribe to more than one contract, send a comma separated list with the channel prefix on each entry, for example `T.ESZ4,T.NQZ4`. You can retrieve available future tickers from our [Futures Contracts API](https://massive.com/docs/rest/futures/contracts/contract-overview).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: Q | The event type. |
| `sym` | string | The ticker symbol for the given future. |
| `bp` | number | The bid price is expressed per unit of the underlying asset, and you apply the contract multiplier to get the full contract value. |
| `bs` | integer | The quote size represents the number of futures contracts available at the given bid price. |
| `bt` | integer | The timestamp when the bid was submitted to the exchange. |
| `ap` | number | The ask price is expressed per unit of the underlying asset, and you apply the contract multiplier to get the full contract value. |
| `as` | integer | The quote size represents the number of futures contracts available at the given ask price. |
| `at` | integer | The timestamp when the ask was submitted to the exchange. |
| `t` | integer | The timestamp in Unix MS. |

## Sample Response

```json
{
  "ev": "Q",
  "sym": "ESZ4",
  "bp": 114.125,
  "bs": 100,
  "bt": 1734103628360,
  "ap": 114.128,
  "as": 160,
  "at": 1734103628350,
  "t": 1536036818784
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
