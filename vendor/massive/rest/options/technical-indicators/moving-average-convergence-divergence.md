> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Options

### Moving Average Convergence/Divergence (MACD)

**Endpoint:** `GET /v1/indicators/macd/{optionsTicker}`

**Description:**

Retrieve the Moving Average Convergence/Divergence (MACD) for a specified ticker over a defined time range. MACD is a momentum indicator derived from two moving averages, helping to identify trend strength, direction, and potential trading signals.

Use Cases: Momentum analysis, signal generation (crossover events), spotting overbought/oversold conditions, and confirming trend directions.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `optionsTicker` | string | Yes | The ticker symbol for which to get MACD data. |
| `timestamp` | string | No | Query by timestamp. Either a date with the format YYYY-MM-DD or a millisecond timestamp. |
| `timespan` | string | No | The size of the aggregate time window. |
| `adjusted` | boolean | No | Whether or not the aggregates used to calculate the MACD are adjusted for splits. By default, aggregates are adjusted. Set this to false to get results that are NOT adjusted for splits. |
| `short_window` | integer | No | The short window size used to calculate MACD data. |
| `long_window` | integer | No | The long window size used to calculate MACD data. |
| `signal_window` | integer | No | The window size used to calculate the MACD signal line. |
| `series_type` | string | No | The price in the aggregate which will be used to calculate the MACD. i.e. 'close' will result in using close prices to  calculate the MACD. |
| `expand_underlying` | boolean | No | Whether or not to include the aggregates used to calculate this indicator in the response. |
| `order` | string | No | The order in which to return the results, ordered by timestamp. |
| `limit` | integer | No | Limit the number of results returned, default is 10 and max is 5000 |
| `timestamp.gte` | string | No | Range by timestamp. |
| `timestamp.gt` | string | No | Range by timestamp. |
| `timestamp.lte` | string | No | Range by timestamp. |
| `timestamp.lt` | string | No | Range by timestamp. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page of data. |
| `request_id` | string | A request id assigned by the server. |
| `results` | object | The results of the MACD indicator calculation. |
| `results.underlying` | object | The underlying aggregates used. |
| `results.values` | array[object] | Each entry in the values array represents MACD indicator data for a specific timestamp and includes: |
| `status` | string | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/v1/indicators/macd/O:SPY241220P00720000?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIxLTA0LTI1JmxpbWl0PTEmb3JkZXI9YXNjJnBhZ2VfbWFya2VyPUElN0M5YWRjMjY0ZTgyM2E1ZjBiOGUyNDc5YmZiOGE1YmYwNDVkYzU0YjgwMDcyMWE2YmI1ZjBjMjQwMjU4MjFmNGZiJnNvcnQ9dGlja2Vy",
  "request_id": "a47d1beb8c11b6ae897ab76cdbbf35a3",
  "results": {
    "underlying": {
      "url": "https://api.massive.com/v2/aggs/ticker/O:SPY241220P00720000/range/1/day/2003-01-01/2022-07-25"
    },
    "values": [
      {
        "histogram": 38.3801666667,
        "signal": 106.9811666667,
        "timestamp": 1517562000016,
        "value": 145.3613333333
      },
      {
        "histogram": 41.098859136,
        "signal": 102.7386283473,
        "timestamp": 1517562001016,
        "value": 143.8374874833
      }
    ]
  },
  "status": "OK"
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
