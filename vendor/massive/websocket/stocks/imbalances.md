> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Stocks

### Net Order Imbalance (NOI)

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

The channel prefix for this feed is `NOI`. To subscribe, send `NOI.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"NOI.*"}
```

Separate more than one subscription with commas, for example `NOI.<ticker1>,NOI.<ticker2>`.
The `NOI.*` value subscribes to all tickers.

**Description:**

Stream real-time Net Order Imbalance (NOI) events for specified stock tickers via WebSocket. Focused on NYSE listed securities, this feed delivers updates on buy and sell order imbalances during scheduled exchange auctions. Typically, at market open (9:30 AM ET) and close (4:00 PM ET), along with indicative clearing prices, paired quantities, and imbalance amounts. Intraday events may also occur during ticker specific halts or mini-auctions, offering insights into liquidity pressures and auction dynamics.

Use Cases: Auction price discovery, execution timing, liquidity monitoring, short-term trading signals, market event analysis.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a stock ticker or use * to subscribe to all stock tickers. You can also use a comma separated list to subscribe to multiple stock tickers. You can retrieve available stock tickers from our [Stock Tickers API](https://massive.com/docs/rest/stocks/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: NOI | The event type. |
| `T` | string | The ticker symbol for the given stock. |
| `t` | integer | The Timestamp in Unix Ns. |
| `at` | integer | The time that the auction is planned to take place in the format (hour x 100) + minutes in Eastern Standard Time,  for example 930 would be 9:30 am EST, and 1600 would be 4:00 pm EST.  |
| `a` | string | The auction type. `O` - Early Opening Auction (non-NYSE only) `M` - Core Opening Auction `H` - Reopening Auction (Halt Resume) `C` - Closing Auction `P` - Extreme Closing Imbalance (NYSE only) `R` - Regulatory Closing Imbalance (NYSE only)  |
| `i` | integer | The symbol sequence. |
| `x` | integer | The exchange ID. See <a target="_blank" href="https://massive.com/docs/rest/stocks/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `o` | integer | The imbalance quantity. |
| `p` | integer | The paired quantity. |
| `b` | number | The book clearing price. |

## Sample Response

```json
{
  "ev": "NOI",
  "T": "NTEST.Q",
  "t": 1601318039223013600,
  "at": 930,
  "a": "M",
  "i": 44,
  "x": 10,
  "o": 480,
  "p": 440,
  "b": 25.03
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
| Stocks Advanced | Not included |
| Imbalances Expansion | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Not included |
| Imbalances Expansion Business | Included |

## Plan Recency

**Plan Recency:** Real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Not included |
| Stocks Developer | Not included |
| Stocks Advanced | Not included |
| Imbalances Expansion | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Not included |
| Imbalances Expansion Business | Real-time |

## Plan History

**Plan History:** Not applicable
