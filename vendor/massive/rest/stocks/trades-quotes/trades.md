> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### Trades

**Endpoint:** `GET /v3/trades/{stockTicker}`

**Description:**

Retrieve comprehensive, tick-level trade data for a specified stock ticker within a defined time range. Each record includes price, size, exchange, trade conditions, and precise timestamp information. This granular data is foundational for constructing aggregated bars and performing in-depth analyses, as it captures every eligible trade that contributes to calculations of open, high, low, and close (OHLC) values. By leveraging these trades, users can refine their understanding of intraday price movements, test and optimize algorithmic strategies, and ensure compliance by maintaining an auditable record of market activity.

Use Cases: Intraday analysis, algorithmic trading, market microstructure research, data integrity and compliance.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `stockTicker` | string | Yes | Specify a case-sensitive ticker symbol. For example, AAPL represents Apple Inc. |
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
| `results[].conditions` | array[integer] | A list of condition codes. See [Stocks Condition Codes](https://massive.com/docs/rest/stocks/market-operations/condition-codes) for what each code means. |
| `results[].correction` | integer | The trade correction indicator. |
| `results[].decimal_size` | string | The size of the trade including the fractional component. This is represented as a decimal string. |
| `results[].exchange` | integer | The exchange ID. See <a href="https://massive.com/docs/rest/stocks/market-operations/exchanges" alt="Exchanges">Exchanges</a> for Massive's mapping of exchange IDs. |
| `results[].id` | string | The Trade ID which uniquely identifies a trade. These are unique per combination of ticker, exchange, and TRF. For example: A trade for AAPL executed on NYSE and a trade for AAPL executed on NASDAQ could potentially have the same Trade ID. |
| `results[].participant_timestamp` | integer | The nanosecond accuracy Participant/Exchange Unix Timestamp. This is the timestamp of when the trade was actually generated at the exchange. Omitted on OTC trades reported through the FINRA ORF (exchange 62), which carry no participant timestamp. |
| `results[].price` | number | The price of the trade. This is the actual dollar value per whole share of this trade. A trade of 100 shares with a price of $2.00 would be worth a total dollar value of $200.00. |
| `results[].sequence_number` | integer | The sequence number represents the sequence in which trade events happened. These are increasing and unique per ticker symbol, but will not always be sequential (e.g., 1, 2, 6, 9, 10, 11). Values reset after each trading session/day. |
| `results[].sip_timestamp` | integer | The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when the SIP received this trade from the exchange which produced it. |
| `results[].size` | number | The size of a trade (also known as volume). |
| `results[].tape` | integer | There are 3 tapes which define which exchange the ticker is listed on. These are integers in our objects which represent the letter of the alphabet. Eg: 1 = A, 2 = B, 3 = C. * Tape A is NYSE listed securities * Tape B is NYSE ARCA / NYSE American * Tape C is NASDAQ |
| `results[].trf_id` | integer | The ID for the Trade Reporting Facility where the trade took place. |
| `results[].trf_timestamp` | integer | The nanosecond accuracy TRF (Trade Reporting Facility) Unix Timestamp. This is the timestamp of when the trade reporting facility received this trade. |
| `status` | string | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/v3/trades/AAPL?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIxLTA0LTI1JmxpbWl0PTEmb3JkZXI9YXNjJnBhZ2VfbWFya2VyPUElN0M5YWRjMjY0ZTgyM2E1ZjBiOGUyNDc5YmZiOGE1YmYwNDVkYzU0YjgwMDcyMWE2YmI1ZjBjMjQwMjU4MjFmNGZiJnNvcnQ9dGlja2Vy",
  "request_id": "a47d1beb8c11b6ae897ab76cdbbf35a3",
  "results": [
    {
      "conditions": [
        12,
        41
      ],
      "decimal_size": "100.0",
      "exchange": 11,
      "id": "1",
      "participant_timestamp": 1517562000015577000,
      "price": 171.55,
      "sequence_number": 1063,
      "sip_timestamp": 1517562000016036600,
      "size": 100,
      "tape": 3
    },
    {
      "conditions": [
        12,
        41
      ],
      "decimal_size": "100.0",
      "exchange": 11,
      "id": "2",
      "participant_timestamp": 1517562000015577600,
      "price": 171.55,
      "sequence_number": 1064,
      "sip_timestamp": 1517562000016038100,
      "size": 100,
      "tape": 3
    }
  ],
  "status": "OK"
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
| Stocks Business | Included |
| Stocks Business + Expansion | Included |

## Plan Recency

**Plan Recency:** End-of-day, 15-minute delayed, or real-time

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
| Stocks Business | End-of-day |
| Stocks Business + Expansion | Real-time |

## Plan History

**Plan History:** Records date back to September 10, 2003

#### Individual Plans

| Plan | History |
| --- | --- |
| Stocks Basic | Not included |
| Stocks Starter | Not included |
| Stocks Developer | 10 years |
| Stocks Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Stocks Business | All history |
| Stocks Business + Expansion | All history |
