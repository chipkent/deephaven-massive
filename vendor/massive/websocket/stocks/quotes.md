> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Stocks

### Quotes

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

The channel prefix for this feed is `Q`. To subscribe, send `Q.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"Q.*"}
```

Separate more than one subscription with commas, for example `Q.<value1>,Q.<value2>`.
The `Q.*` value subscribes to all `ticker` values.

**Description:**

Stream NBBO (National Best Bid and Offer) quote data for stock tickers via WebSocket. Each message provides the current best bid/ask prices, sizes, and related metadata as they update, allowing users to monitor evolving market conditions, inform trading decisions, and maintain responsive, data-driven applications.

Use Cases: Live monitoring, market analysis, trading decision support, dynamic interface updates.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a stock ticker or use * to subscribe to all stock tickers. To subscribe to more than one ticker, send a comma separated list with the channel prefix on each entry, for example `T.AAPL,T.MSFT`. You can retrieve available stock tickers from our [Stock Tickers API](https://massive.com/docs/rest/stocks/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: Q | The event type. |
| `sym` | string | The ticker symbol for the given stock. |
| `bx` | integer | The bid exchange ID. May be omitted when there is no bid (see bp). |
| `bp` | number | The bid price. Omitted, together with bs, when there is no bid (a one-sided or empty market). This is most common around the 4:00 AM ET pre-market open, but can occur at any time.  |
| `bs` | integer | The bid size. This represents the number of shares buyers are bidding for at the given bid price. Omitted when there is no bid (see bp).  |
| `ax` | integer | The ask exchange ID. May be omitted when there is no ask (see ap). |
| `ap` | number | The ask price. Omitted, together with as, when there is no ask (a one-sided or empty market). This is most common around the 4:00 AM ET pre-market open, but can occur at any time.  |
| `as` | integer | The ask size. This represents the number of shares sellers are offering at the given ask price. Omitted when there is no ask (see ap).  |
| `c` | integer | The condition. |
| `i` | array[integer] | The indicators. For more information, see our glossary of [Conditions and Indicators](https://massive.com/glossary/us/stocks/conditions-indicators).  |
| `t` | integer | The SIP timestamp in Unix MS. |
| `pt` | integer | Participant timestamp. The time the quote was generated at the exchange, in milliseconds epoch. Typically ≤ t (SIP timestamp) due to SIP processing latency. Omitted on SIP-generated quotes (condition 82), which have no originating exchange participant.  |
| `q` | integer | The sequence number represents the sequence in which quote events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). Values reset after each trading session/day.  |
| `z` | integer | The tape. (1 = NYSE, 2 = AMEX, 3 = Nasdaq). |

## Sample Response

```json
{
  "ev": "Q",
  "sym": "MSFT",
  "bx": 4,
  "bp": 114.125,
  "bs": 100,
  "ax": 7,
  "ap": 114.128,
  "as": 160,
  "c": 0,
  "i": [
    604
  ],
  "t": 1536036818784,
  "pt": 1536036818783,
  "q": 50385480,
  "z": 3
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
