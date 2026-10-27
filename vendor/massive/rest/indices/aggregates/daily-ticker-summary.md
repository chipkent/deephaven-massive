> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Indices

### Daily Ticker Summary (OHLC)

**Endpoint:** `GET /v1/open-close/{indicesTicker}/{date}`

**Description:**

Retrieve the opening and closing prices for a specific index on a given date, along with any pre-market and after-hours trade prices. This endpoint provides essential daily pricing details, enabling users to evaluate performance, conduct historical analysis, and gain insights into trading activity outside regular market sessions.

Use Cases: Daily performance analysis, historical data collection, after-hours insights, portfolio tracking.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `indicesTicker` | string | Yes | The ticker symbol of Index. |
| `date` | string | Yes | The date of the requested open/close in the format YYYY-MM-DD. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `afterHours` | number | The close value of the ticker symbol in after hours trading. |
| `close` | number | The close value for the symbol in the given time period. |
| `from` | string | The requested date. |
| `high` | number | The highest value for the symbol in the given time period. |
| `low` | number | The lowest value for the symbol in the given time period. |
| `open` | number | The open value for the symbol in the given time period. |
| `preMarket` | number | The open value of the ticker symbol in pre-market trading. |
| `status` | string | The status of this request's response. |
| `symbol` | string | The exchange symbol that this item is traded under. |

## Sample Response

```json
{
  "afterHours": 11830.43006295237,
  "close": 11830.28178808306,
  "from": "2023-03-10T00:00:00.000Z",
  "high": 12069.62262033557,
  "low": 11789.85923449393,
  "open": 12001.69552583921,
  "preMarket": 12001.69552583921,
  "status": "OK",
  "symbol": "I:NDX"
}
```


## Plan Access

**Plan Access:** Included in all Indices plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Indices Basic | Included |
| Indices Starter | Included |
| Indices Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Indices Business | Included |

## Plan Recency

**Plan Recency:** End-of-day, 15-minute delayed, or real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Indices Basic | End-of-day |
| Indices Starter | 15-minute delayed |
| Indices Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Indices Business | Real-time |

## Plan History

**Plan History:** Records date back to February 14, 2023

#### Individual Plans

| Plan | History |
| --- | --- |
| Indices Basic | All history |
| Indices Starter | All history |
| Indices Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Indices Business | All history |
