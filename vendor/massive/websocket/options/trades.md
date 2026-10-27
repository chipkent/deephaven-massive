> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Options

### Trades

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

The channel prefix for this feed is `T`. To subscribe, send `T.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"T.O:SPY251219C00650000"}
```

Separate more than one subscription with commas, for example `T.<ticker1>,T.<ticker2>`.
The `T.*` value subscribes to all tickers.

**Description:**

Stream tick-level trade data for option contracts via WebSocket. Each message delivers key trade details (price, size, exchange, conditions, and timestamps) as they occur, enabling users to track market activity, power live dashboards, and inform rapid decision-making.

Use Cases: Live monitoring, algorithmic trading, market analysis, data visualization.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify an option contract or use * to subscribe to all option contracts. You can also use a comma separated list to subscribe to multiple option contracts. You can retrieve active options contracts from our [Options Contracts API](https://massive.com/docs/rest/options/contracts/all-contracts).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: T | The event type. |
| `sym` | string | The ticker symbol for the given option contract. |
| `x` | integer | The exchange ID. See <a target="_blank" href="https://massive.com/docs/rest/stocks/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `p` | number | The price. |
| `s` | integer | The trade size. |
| `c` | array[integer] | The trade conditions |
| `t` | integer | The Timestamp in Unix MS. |
| `q` | integer | The sequence number represents the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). |

## Sample Response

```json
{
  "ev": "T",
  "sym": "O:AMC210827C00037000",
  "x": 65,
  "p": 1.54,
  "s": 1,
  "c": [
    233
  ],
  "t": 1629820676333,
  "q": 651921857
}
```


## Plan Access

**Plan Access:** Included in select Options plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | Included |
| Options Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Options Business | Not included |
| Options Business + Expansion | Included |

## Plan Recency

**Plan Recency:** 15-minute delayed or real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | 15-minute delayed |
| Options Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Options Business | Not included |
| Options Business + Expansion | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
