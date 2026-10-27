> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Options

### Aggregates (Per Minute)

## Connection

| Feed | URL |
| --- | --- |
| Delayed | `wss://delayed.massive.com/options` |
| Real-Time | `wss://socket.massive.com/options` |

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
{"action":"subscribe","params":"AM.O:SPY251219C00650000"}
```

Separate more than one subscription with commas, for example `AM.<ticker1>,AM.<ticker2>`.
The `AM.*` value subscribes to all tickers.

**Description:**

Stream minute-by-minute aggregated OHLC (Open, High, Low, Close) and volume data for a specified options contract via WebSocket. These aggregates are updated continuously in Eastern Time (ET). Each bar is constructed solely from qualifying trades that meet specific conditions; if no eligible trades occur within a given minute, no bar is emitted. By delivering an ongoing feed of updated market snapshots, this endpoint enables users to closely monitor intraday price movements, enhance trading strategies, and support live data visualizations in the options market.

Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, automated trading.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify an option contract or use * to subscribe to all option contracts. For business plans, the wildcard option is only available on the Full Market and Full Market Delayed expansions. You can also use a comma separated list to subscribe to multiple option contracts. You can retrieve active options contracts from our [Options Contracts API](https://massive.com/docs/rest/options/contracts/all-contracts).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: AM | The event type. |
| `sym` | string | The ticker symbol for the given option contract. |
| `v` | integer | The tick volume. |
| `av` | integer | Today's accumulated volume. |
| `op` | number | Today's official opening price. |
| `vw` | number | The tick's volume weighted average price. |
| `o` | number | The opening tick price for this aggregate window. |
| `c` | number | The closing tick price for this aggregate window. |
| `h` | number | The highest tick price for this aggregate window. |
| `l` | number | The lowest tick price for this aggregate window. |
| `a` | number | Today's volume weighted average price. |
| `z` | integer | The average trade size for this aggregate window. |
| `s` | integer | The start timestamp of this aggregate window in Unix Milliseconds. |
| `e` | integer | The end timestamp of this aggregate window in Unix Milliseconds. |

## Sample Response

```json
{
  "ev": "AM",
  "sym": "O:ONEM220121C00025000",
  "v": 2,
  "av": 8,
  "op": 2.2,
  "vw": 2.05,
  "o": 2.05,
  "c": 2.05,
  "h": 2.05,
  "l": 2.05,
  "a": 2.1312,
  "z": 2,
  "s": 1632419640000,
  "e": 1632419700000
}
```


## Plan Access

**Plan Access:** Included in select Options plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Included |
| Options Developer | Included |
| Options Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Options Business | Included |

## Plan Recency

**Plan Recency:** 15-minute delayed or real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Options Basic | Not included |
| Options Starter | 15-minute delayed |
| Options Developer | 15-minute delayed |
| Options Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Options Business | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
