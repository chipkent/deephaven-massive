> For the full documentation index, see: https://massive.com/docs/llms.txt

# WEBSOCKET
## Options

### Admin & FLEX Options

## Connection

| Feed | URL |
| --- | --- |
| Real-Time | `wss://socket.massive.com/options` |

Connect to the URL for the feed that your plan includes. See the plan table below.

## Authentication

After you connect, send your API key in an `auth` message:

```json
{"action":"auth","params":"YOUR_API_KEY"}
```

The server replies with `[{"ev":"status","status":"auth_success","message":"authenticated"}]`. Send subscribe messages only after this reply.

## Subscription

The channel prefix for this feed is `AD`. To subscribe, send `AD.` followed by a value of the `exchange_id` parameter:

```json
{"action":"subscribe","params":"AD.*"}
```

Separate more than one subscription with commas, for example `AD.<value1>,AD.<value2>`.
The `AD.*` value subscribes to all `exchange_id` values.

**Description:**

Stream real-time administrative (admin) messages for options via WebSocket, including OPRA FLEX trade notifications. Each message delivers the raw administrative text along with its exchange, sequence number, and nanosecond SIP timestamp.

FLEX (FLexible EXchange) options are customized, exchange-traded options contracts that let counterparties negotiate non-standard terms — such as the strike price, expiration date, exercise style, and settlement type — beyond what standard listed options offer. FLEX trades are disseminated by OPRA as administrative messages and surfaced through this feed.

Use Cases: FLEX trade monitoring, market operations tracking, data auditing.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `exchange_id` | string | Yes | Specify an exchange ID or use * to subscribe to admin messages from all exchanges. To subscribe to more than one exchange, send a comma separated list with the channel prefix on each entry, for example `AD.302,AD.323`. See <a target="_blank" href="https://massive.com/docs/rest/options/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs.  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `ev` | enum: AD | The event type. |
| `i` | string | The unique identifier for this admin message, composed of the SIP timestamp and the sequence number (e.g., "1793911442930568079-4043826282"). |
| `x` | integer | The exchange ID. See <a target="_blank" href="https://massive.com/docs/rest/options/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `m` | string | The administrative message text as distributed by OPRA. |
| `t` | integer | The SIP timestamp in Unix nanoseconds. |
| `q` | integer | The sequence number represents the sequence in which admin events happened. These are increasing and unique, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). |
| `rt` | boolean | Whether this message is a retransmission (a replay of a previously sent message, e.g., during gap recovery), rather than an original real-time message. Only present, and set to true, when it is a retransmission; it is omitted otherwise. |

## Sample Response

```json
{
  "ev": "AD",
  "i": "1793911442930568079-4043826282",
  "t": 1793911442930568200,
  "q": 4043826282,
  "x": 302,
  "m": "TEXT CFLEX SPY LST PM EUR 4 LEGS 1 @ 733.36"
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
| Options Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Options Business | Not included |
| Real-time FLEX Trades Business | Included |

## Plan Recency

**Plan Recency:** Real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | Not included |
| Options Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Options Business | Not included |
| Real-time FLEX Trades Business | Real-time |

## Plan History

**Plan History:** Not applicable
