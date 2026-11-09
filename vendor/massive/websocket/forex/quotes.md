> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Forex

### Quotes

## Connection

| Feed | URL |
| --- | --- |
| Real-Time | `wss://socket.massive.com/forex` |

Connect to the URL for the feed that your plan includes. See the plan table below.

## Authentication

After you connect, send your API key in an `auth` message:

```json
{"action":"auth","params":"YOUR_API_KEY"}
```

The server replies with `[{"ev":"status","status":"auth_success","message":"authenticated"}]`. Send subscribe messages only after this reply.

## Subscription

The channel prefix for this feed is `C`. To subscribe, send `C.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"C.*"}
```

Separate more than one subscription with commas, for example `C.<value1>,C.<value2>`.
The `C.*` value subscribes to all `ticker` values.

**Description:**

Stream real-time Best Bid and Offer (BBO) quote data for specified Forex currency pairs via WebSocket. Each message provides the current bid/ask prices, sizes, and associated metadata as they update, enabling users to monitor evolving market conditions, guide trading decisions, and power responsive, data-driven applications.

Use Cases: Live monitoring, market analysis, trading decision support, dynamic interface updates.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a forex pair in the format {from}-{to} or use * to subscribe to all forex pairs. To subscribe to more than one pair, send a comma separated list with the channel prefix on each entry, for example `C.EUR-USD,C.GBP-USD`. You can retrieve active forex tickers from our [Forex Tickers API](https://massive.com/docs/rest/forex/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: C | The event type. |
| `p` | string | The current pair. |
| `x` | integer | The exchange ID. See <a href="https://massive.com/docs/rest/forex/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `a` | number | The ask price. |
| `b` | number | The bid price. |
| `t` | integer | The Timestamp in Unix MS. |

## Sample Response

```json
{
  "ev": "C",
  "p": "USD/CNH",
  "x": "44",
  "a": 6.83366,
  "b": 6.83363,
  "t": 1536036818784
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
