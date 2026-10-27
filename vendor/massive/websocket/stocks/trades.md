> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Stocks

### Trades

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

The channel prefix for this feed is `T`. To subscribe, send `T.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"T.*"}
```

Separate more than one subscription with commas, for example `T.<ticker1>,T.<ticker2>`.
The `T.*` value subscribes to all tickers.

**Description:**

Stream tick-level trade data for stock tickers via WebSocket. Each message delivers key trade details (price, size, exchange, conditions, and timestamps) as they occur, enabling users to track market activity, power live dashboards, and inform rapid decision-making.

Use Cases: Live monitoring, algorithmic trading, market analysis, data visualization.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a stock ticker or use * to subscribe to all stock tickers. You can also use a comma separated list to subscribe to multiple stock tickers. You can retrieve available stock tickers from our [Stock Tickers API](https://massive.com/docs/rest/stocks/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: T | The event type. |
| `sym` | string | The ticker symbol for the given stock. |
| `x` | integer | The exchange ID. See <a target="_blank" href="https://massive.com/docs/rest/stocks/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `i` | string | The trade ID. |
| `z` | integer | The tape. (1 = NYSE, 2 = AMEX, 3 = Nasdaq).  |
| `p` | number | The price. |
| `s` | integer | The trade size. |
| `ds` | string | The trade size including fractional shares, respresented as a string |
| `c` | array[integer] | The trade conditions. See <a target="_blank" href="https://massive.com/glossary/us/stocks/conditions-indicators"  alt="Conditions and Indicators">Conditions and Indicators</a> for Massive's trade conditions glossary.  |
| `t` | integer | The SIP timestamp in Unix MS. |
| `pt` | integer | Participant timestamp. The time the trade occurred at the exchange or TRF, in milliseconds epoch. Typically ≤ t (SIP timestamp) due to SIP processing latency, though trades reported through a TRF may occasionally carry a pt up to 1ms after t. Omitted on OTC trades reported through the FINRA ORF (exchange 62), which have no participant timestamp.  |
| `q` | integer | The sequence number represents the sequence in which message events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11).  |
| `trfi` | integer | The ID for the Trade Reporting Facility where the trade took place. |
| `trft` | integer | The TRF (Trade Reporting Facility) Timestamp in Unix MS. This is the timestamp of when the trade reporting facility received this trade.  |

## Sample Response

```json
{
  "ev": "T",
  "sym": "MSFT",
  "x": 4,
  "i": "12345",
  "z": 3,
  "p": 114.125,
  "s": 100,
  "c": [
    0,
    12
  ],
  "t": 1536036818784,
  "pt": 1536036818763,
  "q": 3681328
}
```


## Plan Access

**Plan Access:** Included in select Stocks plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Not included |
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
| Stocks Starter | Not included |
| Stocks Developer | 15-minute delayed |
| Stocks Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Not included |
| Stocks Business + Expansion | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
