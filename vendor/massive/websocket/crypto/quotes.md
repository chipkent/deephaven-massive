> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Crypto

### Quotes

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

The channel prefix for this feed is `XQ`. To subscribe, send `XQ.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"XQ.*"}
```

Separate more than one subscription with commas, for example `XQ.<value1>,XQ.<value2>`.
The `XQ.*` value subscribes to all `ticker` values.

**Description:**

Stream quote data for specified cryptocurrency pairs via WebSocket. Each message delivers current bid/ask prices, sizes, and relevant metadata from multiple exchanges as they update, allowing users to monitor evolving market conditions, guide trading decisions, and support responsive, data-driven applications.

Use Cases: Live monitoring, market analysis, trading decision support, dynamic interface updates.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a crypto pair in the format {from}-{to} or use * to subscribe to all crypto pairs. To subscribe to more than one pair, send a comma separated list with the channel prefix on each entry, for example `XT.BTC-USD,XT.ETH-USD`. You can retrieve active crypto tickers from our [Crypto Tickers API](https://massive.com/docs/rest/crypto/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: XQ | The event type. |
| `pair` | string | The crypto pair. |
| `bp` | number | The bid price. |
| `bs` | number | The bid size. |
| `ap` | number | The ask price. |
| `as` | number | The ask size. |
| `t` | integer | The Timestamp in Unix MS. |
| `x` | integer | The crypto exchange ID.  See <a target="_blank" href="https://massive.com/docs/rest/crypto/market-operations/exchanges">Exchanges</a> for a list of exchanges and their IDs.  |
| `r` | integer | The timestamp that the tick was received by Massive. |

## Sample Response

```json
{
  "ev": "XQ",
  "pair": "BTC-USD",
  "bp": 33052.79,
  "bs": 0.48,
  "ap": 33073.19,
  "as": 0.601,
  "t": 1610462411115,
  "x": 1,
  "r": 1610462411128
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
