> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Stocks

### Limit Up - Limit Down (LULD)

## Connection

| Feed | URL |
| --- | --- |
| Real-Time | `wss://socket.massive.com/stocks` |

Connect to the URL for the feed that your plan includes. See the plan table below.

## Authentication

After you connect, send your API key in an `auth` message:

```json
{"action":"auth","params":"YOUR_API_KEY"}
```

The server replies with `[{"ev":"status","status":"auth_success","message":"authenticated"}]`. Send subscribe messages only after this reply.

## Subscription

The channel prefix for this feed is `LULD`. To subscribe, send `LULD.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"LULD.*"}
```

Separate more than one subscription with commas, for example `LULD.<ticker1>,LULD.<ticker2>`.
The `LULD.*` value subscribes to all tickers.

**Description:**

Stream real-time Limit Up - Limit Down (LULD) events for specified stock tickers via WebSocket across multiple U.S. exchanges (including NYSE, Nasdaq, Cboe BZX, NYSE Arca, and NYSE American). Events signal when securities approach or breach dynamic price bands, triggering pauses, halts, or resumptions to curb volatility. Halt and resumption messages (indicators 17 and 18) are only available for NASDAQ listed securities. This high-volume feed provides continuous intraday coverage during regular trading hours, with details on price limits, indicators, and timestamps for proactive market response.

Use Cases: Volatility monitoring, risk management, compliance tracking, trading strategy adjustments.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a stock ticker or use * to subscribe to all stock tickers. You can also use a comma separated list to subscribe to multiple stock tickers. You can retrieve available stock tickers from our [Stock Tickers API](https://massive.com/docs/rest/stocks/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: LULD | The event type. |
| `T` | string | The ticker symbol for the given stock. |
| `h` | number | The high price. |
| `l` | number | The low price. |
| `i` | array[integer] | The Indicators. See <a target="_blank" href="https://massive.com/glossary/us/stocks/conditions-indicators" alt="Conditions and Indicators">Conditions and Indicators</a> for a glossary (LULD indicators are located near the bottom).  |
| `z` | integer | The tape. (1 = NYSE, 2 = AMEX, 3 = Nasdaq).  |
| `t` | integer | The Timestamp in Unix MS. |
| `q` | integer | The sequence number represents the sequence in which message events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).  |

## Sample Response

```json
{
  "ev": "LULD",
  "T": "MSFT",
  "h": 492.99,
  "l": 446.04,
  "i": [
    16
  ],
  "z": 3,
  "t": 1764086430905642800,
  "q": 5925769
}
```


## Plan Access

**Plan Access:** Included in select Stocks plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Not included |
| Stocks Developer | Not included |
| Stocks Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Not included |
| Stocks Business + Expansion | Included |

## Plan Recency

**Plan Recency:** Real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Not included |
| Stocks Developer | Not included |
| Stocks Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Not included |
| Stocks Business + Expansion | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
