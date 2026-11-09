> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Stocks

### Aggregates (Per Minute)

## Connection

| Feed | URL |
| --- | --- |
| Delayed | `wss://delayed.massive.com/stocks` |
| Real-Time | `wss://socket.massive.com/stocks` |

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

Separate more than one subscription with commas, for example `AM.<value1>,AM.<value2>`.
The `AM.*` value subscribes to all `ticker` values.

**Description:**

Stream minute-by-minute aggregated OHLC (Open, High, Low, Close) and volume data for specified tickers via WebSocket. These aggregates are updated continuously in Eastern Time (ET) and cover pre-market, regular, and after-hours sessions. Each bar is constructed solely from qualifying trades that meet specific conditions; if no eligible trades occur within a given minute, no bar is emitted. By providing a steady flow of aggregate bars, this endpoint enables users to track intraday price movements, refine trading strategies, and power live data visualizations.

Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, automated trading.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a stock ticker or use * to subscribe to all stock tickers. To subscribe to more than one ticker, send a comma separated list with the channel prefix on each entry, for example `T.AAPL,T.MSFT`. You can retrieve available stock tickers from our [Stock Tickers API](https://massive.com/docs/rest/stocks/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: AM | The event type. |
| `sym` | string | The ticker symbol for the given stock. |
| `v` | integer | The tick volume. |
| `dv` | string | The tick volume including fractional shares, respresented as a string decimal number |
| `av` | integer | Today's accumulated volume. |
| `dav` | string | Today's accumulated volume including fractional shares, respresented as a string decimal number |
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
| `otc` | boolean | Whether or not this aggregate is for an OTC ticker. This field will be left off if false. |

## Sample Response

```json
{
  "ev": "AM",
  "sym": "GTE",
  "v": 4110,
  "av": 9470157,
  "op": 0.4372,
  "vw": 0.4488,
  "o": 0.4488,
  "c": 0.4486,
  "h": 0.4489,
  "l": 0.4486,
  "a": 0.4352,
  "z": 685,
  "s": 1610144640000,
  "e": 1610144700000
}
```


## Plan Access

**Plan Access:** Included in select Stocks plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Included |
| Stocks Developer | Included |
| Stocks Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Included |

## Plan Recency

**Plan Recency:** 15-minute delayed or real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | 15-minute delayed |
| Stocks Developer | 15-minute delayed |
| Stocks Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
