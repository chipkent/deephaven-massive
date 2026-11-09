> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Stocks

### Aggregates (Per Second)

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

The channel prefix for this feed is `A`. To subscribe, send `A.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"A.*"}
```

Separate more than one subscription with commas, for example `A.<value1>,A.<value2>`.
The `A.*` value subscribes to all `ticker` values.

**Description:**

Stream second-by-second aggregated OHLC (Open, High, Low, Close) and volume data for specified tickers via WebSocket. These aggregates are updated continuously in Eastern Time (ET) and cover pre-market, regular, and after-hours sessions. Each bar is constructed solely from qualifying trades that meet specific conditions; if no eligible trades occur within a given minute, no bar is emitted. By providing a steady flow of aggregate bars, this endpoint enables users to track intraday price movements, refine trading strategies, and power live data visualizations.

Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, automated trading.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a stock ticker or use * to subscribe to all stock tickers. To subscribe to more than one ticker, send a comma separated list with the channel prefix on each entry, for example `T.AAPL,T.MSFT`. You can retrieve available stock tickers from our [Stock Tickers API](https://massive.com/docs/rest/stocks/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: A | The event type. |
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
  "ev": "A",
  "sym": "SPCE",
  "v": 200,
  "av": 8642007,
  "op": 25.66,
  "vw": 25.3981,
  "o": 25.39,
  "c": 25.39,
  "h": 25.39,
  "l": 25.39,
  "a": 25.3714,
  "z": 50,
  "s": 1610144868000,
  "e": 1610144869000
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
| Stocks Business | Not included |
| Stocks Business + Expansion | Included |

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
| Stocks Business | Not included |
| Stocks Business + Expansion | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
