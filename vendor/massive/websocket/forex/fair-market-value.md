> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Forex

### Fair Market Value

## Connection

| Feed | URL |
| --- | --- |
| Business | `wss://business.massive.com/forex` |

Connect to the URL for the feed that your plan includes. See the plan table below.

## Authentication

After you connect, send your API key in an `auth` message:

```json
{"action":"auth","params":"YOUR_API_KEY"}
```

The server replies with `[{"ev":"status","status":"auth_success","message":"authenticated"}]`. Send subscribe messages only after this reply.

## Subscription

The channel prefix for this feed is `FMV`. To subscribe, send `FMV.` followed by a value of the `ticker` parameter:

```json
{"action":"subscribe","params":"FMV.*"}
```

Separate more than one subscription with commas, for example `FMV.<ticker1>,FMV.<ticker2>`.
The `FMV.*` value subscribes to all tickers.

**Description:**

Stream real-time Fair Market Value (FMV) data for a specified Forex currency pair via WebSocket. This proprietary metric, available exclusively to Business plan users, provides an algorithmically derived, real-time estimate of the currency pair’s fair market price. By delivering accurate, continuous valuation data, this feed supports more informed trading decisions, enhanced analytics, and improved risk management.

Use Cases: Pricing strategies, algorithmic modeling, risk assessment, investor decision-making.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify a forex pair in the format {from}-{to} or use * to subscribe to all forex pairs. You can also use a comma separated list to subscribe to multiple forex pairs. You can retrieve active forex tickers from our [Forex Tickers API](https://massive.com/docs/rest/forex/tickers/all-tickers).  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: FMV | The event type. |
| `fmv` | number | Fair market value is only available on Business plans. It is our proprietary algorithm to generate a real-time, accurate, fair market value of a tradable security. For more information, <a rel="nofollow" target="_blank" href="https://massive.com/contact">contact us</a>.  |
| `sym` | string | The ticker symbol for the given security. |
| `t` | integer | The nanosecond timestamp. |

## Sample Response

```json
{
  "ev": "FMV",
  "fmv": 1.0631,
  "sym": "C:EURUSD",
  "t": 1678220098130
}
```


## Plan Access

**Plan Access:** Included in select Currencies plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Currencies Basic | Not included |
| Currencies Starter | Not included |

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
| Currencies Starter | Not included |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Currencies Business | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
