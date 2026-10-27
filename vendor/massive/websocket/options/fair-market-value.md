> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Options

### Fair Market Value

## Connection

| Feed | URL |
| --- | --- |
| Business | `wss://business.massive.com/options` |

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
{"action":"subscribe","params":"FMV.O:SPY251219C00650000"}
```

Separate more than one subscription with commas, for example `FMV.<ticker1>,FMV.<ticker2>`.

**Description:**

Stream real-time Fair Market Value (FMV) data for a specified options contract via WebSocket. This proprietary metric, available exclusively to Business plan users, provides an algorithmically derived, real-time estimate of an option’s fair market price. By delivering accurate, continuous valuation data, this feed supports more informed options trading decisions, enhanced analytics, and improved risk management.

Use Cases: Pricing strategies, algorithmic modeling, risk assessment, investor decision-making.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `ticker` | string | Yes | Specify an option contract. You're only allowed to subscribe to 1,000 option contracts per connection. You can also use a comma separated list to subscribe to multiple option contracts. You can retrieve active options contracts from our [Options Contracts API](https://massive.com/docs/rest/options/contracts/all-contracts).  |

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
  "fmv": 7.2,
  "sym": "O:TSLA210903C00700000",
  "t": 1401715883806000000
}
```


## Plan Access

**Plan Access:** Included in select Options plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | Not included |
| Options Advanced | Not included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Options Business | Included |

## Plan Recency

**Plan Recency:** Real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | Not included |
| Options Advanced | Not included |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Options Business | Real-time |

## Plan History

**Plan History:** Not applicable to websockets
