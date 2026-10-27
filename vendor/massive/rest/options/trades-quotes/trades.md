> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Options

### Trades

**Endpoint:** `GET /v3/trades/{optionsTicker}`

**Description:**

Retrieve comprehensive, tick-level trade data for a specified options ticker within a defined time range. Each record includes price, size, exchange, trade conditions, and precise timestamp information. This granular data is foundational for constructing aggregated bars and performing in-depth analyses, as it captures every eligible trade that contributes to calculations of open, high, low, and close (OHLC) values. By leveraging these trades, users can refine their understanding of intraday price movements, test and optimize algorithmic strategies, and ensure compliance by maintaining an auditable record of market activity.

Use Cases: Intraday analysis, algorithmic trading, market microstructure research, data integrity and compliance.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `optionsTicker` | string | Yes | The options ticker symbol to get trades for. |
| `timestamp` | string | No | Query by trade timestamp. Either a date with the format YYYY-MM-DD or a nanosecond timestamp. |
| `timestamp.gte` | string | No | Range by timestamp. |
| `timestamp.gt` | string | No | Range by timestamp. |
| `timestamp.lte` | string | No | Range by timestamp. |
| `timestamp.lt` | string | No | Range by timestamp. |
| `order` | string | No | Order results based on the `sort` field. |
| `limit` | integer | No | Limit the number of results returned, default is 1000 and max is 50000. |
| `sort` | string | No | Sort field used for ordering. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page of data. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | An array of results containing the requested data. |
| `results[].conditions` | array[integer] | A list of condition codes. See [Options Condition Codes](https://massive.com/docs/rest/options/market-operations/condition-codes) for what each code means. |
| `results[].correction` | integer | The trade correction indicator. |
| `results[].exchange` | integer | The exchange ID. See <a href="https://massive.com/docs/rest/options/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `results[].id` | string | The trade ID. Options trades do not have a trade ID, so this field is returned as an empty string. |
| `results[].participant_timestamp` | integer | The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the trade was actually generated at the exchange. |
| `results[].price` | number | The price of the trade. This is the price per share of the underlying, so the total dollar value of a trade is the price multiplied by the size and by the contract multiplier, which is typically 100 shares per contract. |
| `results[].sequence_number` | integer | The sequence number represents the sequence in which message events happened. These are increasing and unique per ticker symbol, but will not always be consecutive (e.g. 1, 2, 6, 9, 10, 11). |
| `results[].sip_timestamp` | integer | The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this trade from the exchange which produced it. |
| `results[].size` | number | The size of a trade (also known as volume). |
| `status` | string | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/v3/trades/O:AZO140621P00530000?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIxLTA0LTI1JmxpbWl0PTEmb3JkZXI9YXNjJnBhZ2VfbWFya2VyPUElN0M5YWRjMjY0ZTgyM2E1ZjBiOGUyNDc5YmZiOGE1YmYwNDVkYzU0YjgwMDcyMWE2YmI1ZjBjMjQwMjU4MjFmNGZiJnNvcnQ9dGlja2Vy",
  "request_id": "a47d1beb8c11b6ae897ab76cdbbf35a3",
  "results": [
    {
      "exchange": 46,
      "participant_timestamp": 1401715883806000000,
      "price": 6.91,
      "sequence_number": 1,
      "sip_timestamp": 1401715883806000000,
      "size": 1
    },
    {
      "conditions": [
        209
      ],
      "exchange": 67,
      "participant_timestamp": 1401716547786000000,
      "price": 7.2,
      "sequence_number": 2,
      "sip_timestamp": 1401716547786000000,
      "size": 1
    }
  ],
  "status": "OK"
}
```


## Plan Access

**Plan Access:** Included in select Options plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
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
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | 15-minute delayed |
| Options Advanced | Real-time |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Options Business | End-of-day |
| Options Business + Expansion | Real-time |

## Plan History

**Plan History:** Records date back to June 2, 2014

#### Individual Plans

| Plan | History |
| --- | --- |
| Options Basic | Not included |
| Options Starter | Not included |
| Options Developer | 4 years |
| Options Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Options Business | All history |
| Options Business + Expansion | All history |
