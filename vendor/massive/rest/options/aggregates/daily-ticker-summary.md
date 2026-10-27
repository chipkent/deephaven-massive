> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Options

### Daily Ticker Summary (OHLC)

**Endpoint:** `GET /v1/open-close/{optionsTicker}/{date}`

**Description:**

Retrieve the opening and closing prices for a specific options contract on a given date, along with any pre-market and after-hours trade prices. This endpoint provides essential daily pricing details, enabling users to evaluate performance, conduct historical analysis, and gain insights into trading activity outside regular market sessions.

Use Cases: Daily performance analysis, historical data collection, after-hours insights, portfolio tracking.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `optionsTicker` | string | Yes | The ticker symbol of the options contract. |
| `date` | string | Yes | The date of the requested open/close in the format YYYY-MM-DD. |
| `adjusted` | boolean | No | Whether or not the results are adjusted for splits.  By default, results are adjusted. Set this to false to get results that are NOT adjusted for splits.  |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `afterHours` | number | The close price of the ticker symbol in after hours trading. |
| `close` | number | The close price for the symbol in the given time period. |
| `from` | string | The requested date. |
| `high` | number | The highest price for the symbol in the given time period. |
| `low` | number | The lowest price for the symbol in the given time period. |
| `open` | number | The open price for the symbol in the given time period. |
| `otc` | boolean | Whether or not this aggregate is for an OTC ticker. This field will be left off if false. |
| `preMarket` | number | The open price of the ticker symbol in pre-market trading. |
| `status` | string | The status of this request's response. |
| `symbol` | string | The exchange symbol that this item is traded under. |
| `volume` | number | The trading volume of the symbol in the given time period. |

## Sample Response

```json
{
  "afterHours": 26.35,
  "close": 26.35,
  "from": "2023-01-09",
  "high": 26.35,
  "low": 25,
  "open": 25,
  "preMarket": 25,
  "status": "OK",
  "symbol": "O:TSLA210903C00700000",
  "volume": 2
}
```


## Plan Access

**Plan Access:** Included in select Options plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Options Basic | Included |
| Options Starter | Included |
| Options Developer | Included |
| Options Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Options Business | Included |
| Options Business + Expansion | Included |

## Plan Recency

**Plan Recency:** End-of-day, 15-minute delayed, or real-time

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Options Basic | End-of-day |
| Options Starter | 15-minute delayed |
| Options Developer | 15-minute delayed |
| Options Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Options Business | Real-time |
| Options Business + Expansion | Real-time |

## Plan History

**Plan History:** Records date back to June 2, 2014

#### Individual Plans

| Plan | History |
| --- | --- |
| Options Basic | 2 years |
| Options Starter | 2 years |
| Options Developer | 4 years |
| Options Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Options Business | All history |
| Options Business + Expansion | All history |
