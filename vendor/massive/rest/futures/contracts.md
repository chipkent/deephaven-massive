> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Futures

### Contracts

**Endpoint:** `GET /futures/v1/contracts`

**Description:**

The Contracts API provides a single source for discovering all listed futures contracts and retrieving complete contract specifications. You can query the full contract index with filters for product code, trade dates, active status, and date, returning key attributes such as ticker, first and last trade dates, days to maturity, exchange code, and order quantity limits in paginated form. The same API also returns the full specification for a single contract, including settlement dates, tick sizes, and other trading and risk related fields. Point-in-time lookups allow you to reconstruct the exact contract definition that applied on any given day.

Use Cases: Historical research, trading system integration, portfolio workflows, risk management.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `date` | string | No | A date string in the format YYYY-MM-DD. This parameter will return point-in-time information about contracts for the specified day. Value must be formatted 'yyyy-mm-dd'. |
| `date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `product_code` | string | No | The identifier for the contract's product. |
| `product_code.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `product_code.gt` | string | No | Filter greater than the value. |
| `product_code.gte` | string | No | Filter greater than or equal to the value. |
| `product_code.lt` | string | No | Filter less than the value. |
| `product_code.lte` | string | No | Filter less than or equal to the value. |
| `provider_id` | string | No | A unique identifier for the contract assigned by the data provider. Note that this value is null for historical data (pre-2025-03-12). |
| `provider_product_id` | string | No | A unique identifier for the product to which a contract, or contracts, belongs. Note that this value is null for historical data (pre-2025-03-12). |
| `ticker` | string | No | The ticker for the contract. |
| `ticker.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `ticker.gt` | string | No | Filter greater than the value. |
| `ticker.gte` | string | No | Filter greater than or equal to the value. |
| `ticker.lt` | string | No | Filter less than the value. |
| `ticker.lte` | string | No | Filter less than or equal to the value. |
| `active` | boolean | No | Whether or not a given contract was tradeable at the given point in time. Active is true when (first_trade_date <= date >= last_trade_date) and false otherwise. |
| `type` | string | No | The type of contract, one of 'single' or 'combo'. Leaving this filter blank will query for contracts where type is 'single', 'combo' or empty. This field only exists on contracts as of 2025-03-12 and later. It will be null when date < 2025-03-12. |
| `type.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `first_trade_date` | string | No | The first day on which the contract was tradeable. Value must be formatted 'yyyy-mm-dd'. |
| `first_trade_date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `first_trade_date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `first_trade_date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `first_trade_date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `last_trade_date` | string | No | The last day on which the contract was tradeable. Value must be formatted 'yyyy-mm-dd'. |
| `last_trade_date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `last_trade_date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `last_trade_date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `last_trade_date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '1000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'product_code' if not specified. The sort order defaults to 'asc' if not specified. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].active` | boolean | Whether or not a given contract was tradeable at the given point in time. Active is true when (first_trade_date <= date >= last_trade_date) and false otherwise. |
| `results[].date` | string | A date string in the format YYYY-MM-DD. This parameter will return point-in-time information about contracts for the specified day. |
| `results[].days_to_maturity` | integer | The number of calendar days between the 'date' and the contract's final settlement date. |
| `results[].first_trade_date` | string | The first day on which the contract was tradeable. |
| `results[].group_code` | string | An identifier used to identify logical groups of products. The group_code is only populated for contracts listed for trading on CME Globex. |
| `results[].last_trade_date` | string | The last day on which the contract was tradeable. |
| `results[].max_order_quantity` | integer | The maximum order quantity. |
| `results[].min_order_quantity` | integer | The minimum order quantity. |
| `results[].name` | string | The name of this contract. |
| `results[].product_code` | string | The identifier for the contract's product. |
| `results[].provider_id` | string | A unique identifier for the contract assigned by the data provider. Note that this value is null for historical data (pre-2025-03-12). |
| `results[].provider_product_id` | string | A unique identifier for the product to which a contract, or contracts, belongs. Note that this value is null for historical data (pre-2025-03-12). |
| `results[].settlement_date` | string | The date on which this contract settles. |
| `results[].settlement_tick_size` | number | The tick size for settlement. |
| `results[].spread_tick_size` | number | The tick size for spreads. |
| `results[].ticker` | string | The ticker for the contract. |
| `results[].trade_tick_size` | number | The tick size for trades. |
| `results[].trading_venue` | string | The trading venue (MIC) for the exchange on which this contract trades. |
| `results[].type` | string | The type of contract, one of 'single' or 'combo'. Leaving this filter blank will query for contracts where type is 'single', 'combo' or empty. This field only exists on contracts as of 2025-03-12 and later. It will be null when date < 2025-03-12. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/futures/v1/contracts?cursor=YWN0aXZlPXRydWUmZGF0ZT0yMDIxLTA0LTI1JmxpbWl0PTEmb3JkZXI9YXNjJnBhZ2VfbWFya2VyPUElN0M5YWRjMjY0ZTgyM2E1ZjBiOGUyNDc5YmZiOGE1YmYwNDVkYzU0YjgwMDcyMWE2YmI1ZjBjMjQwMjU4MjFmNGZiJnNvcnQ9dGlja2Vy",
  "request_id": "000a000a0a0a000a0a0aa00a0a0000a0",
  "results": [
    {
      "active": true,
      "date": "2026-10-07",
      "days_to_maturity": 72,
      "first_trade_date": "2021-09-17",
      "group_code": "ES",
      "last_trade_date": "2026-12-18",
      "max_order_quantity": 3000,
      "min_order_quantity": 1,
      "name": "ESZ6 Future",
      "product_code": "ES",
      "provider_id": "H3ZLPMESGCUM",
      "provider_product_id": "VGQ4V6NVXLBW",
      "settlement_date": "2026-12-18",
      "settlement_tick_size": 0.0025,
      "spread_tick_size": 0.01,
      "ticker": "ESZ6",
      "trade_tick_size": 0.25,
      "trading_venue": "XCME",
      "type": "single"
    }
  ],
  "status": "OK"
}
```


## Plan Access

**Plan Access:** Included in all Futures plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Futures Basic | Included |
| Futures Starter | Included |
| Futures Developer | Included |
| Futures Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Futures Business CME | Included |
| Futures Business CBOT | Included |
| Futures Business NYMEX | Included |
| Futures Business COMEX | Included |

## Plan Recency

**Plan Recency:** Updated daily

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Futures Basic | Updated daily |
| Futures Starter | Updated daily |
| Futures Developer | Updated daily |
| Futures Advanced | Updated daily |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Futures Business CME | Updated daily |
| Futures Business CBOT | Updated daily |
| Futures Business NYMEX | Updated daily |
| Futures Business COMEX | Updated daily |

## Plan History

**Plan History:** Records date back to April 3, 2017

#### Individual Plans

| Plan | History |
| --- | --- |
| Futures Basic | 2 years |
| Futures Starter | 2 years |
| Futures Developer | 5 years |
| Futures Advanced | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Futures Business CME | All history |
| Futures Business CBOT | All history |
| Futures Business NYMEX | All history |
| Futures Business COMEX | All history |
