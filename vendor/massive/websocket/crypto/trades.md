> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Crypto

### Trades

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

The channel prefix for this feed is `XT`. To subscribe, send `XT.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"XT.*"}
```

Separate more than one subscription with commas, for example `XT.<ticker1>,XT.<ticker2>`.
The `XT.*` value subscribes to all tickers.

**Description:**

Stream trade data for crypto pairs via WebSocket. Each message delivers key trade details (price, size, exchange, conditions, and timestamps) as they occur, enabling users to track market activity, power live dashboards, and inform rapid decision-making.

Use Cases: Live monitoring, algorithmic trading, market analysis, data visualization.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a crypto pair in the format {from}-{to} or use * to subscribe to all crypto pairs. You can also use a comma separated list to subscribe to multiple crypto pairs. You can retrieve active crypto tickers from our [Crypto Tickers API](https://massive.com/docs/rest/crypto/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: XT | The event type. |
| `pair` | string | The crypto pair. |
| `p` | number | The price. |
| `t` | integer | The Timestamp in Unix MS. |
| `s` | number | The size. |
| `c` | array[integer] | The conditions. 0 (or empty array): empty 1: sellside 2: buyside  |
| `i` | integer | The ID of the trade (optional). |
| `x` | integer | The crypto exchange ID.  See <a target="_blank" href="https://massive.com/docs/rest/crypto/market-operations/exchanges">Exchanges</a> for a list of exchanges and their IDs.  |
| `r` | integer | The timestamp that the tick was received by Massive. |

## Sample Response

```json
{
  "ev": "XT",
  "pair": "BTC-USD",
  "p": 33021.9,
  "t": 1610462007425,
  "s": 0.01616617,
  "c": [
    2
  ],
  "i": 14272084,
  "x": 3,
  "r": 1610462007576
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
