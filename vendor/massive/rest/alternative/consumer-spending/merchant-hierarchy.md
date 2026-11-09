> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Alternative

### Merchant Hierarchy

**Endpoint:** `GET /consumer-spending/eu/v1/merchant-hierarchy`

**Description:**

Reference data mapping merchants to parent companies, tickers, sectors, and industries across Fable's European consumer transaction panel. Each row represents a single merchant with its corporate hierarchy and classification metadata. The latest weekly snapshot contains ~3,500 merchants covering ~250 US public companies.

Use `lookup_name` to join with the `name` field from the [Merchant Aggregates](/docs/rest/alternative/consumer-spending/merchant-aggregates) endpoint. Filter by `active_from` and `active_to` to perform point-in-time joins against a specific `transaction_date` (e.g., `active_from.lte=2025-06-15&active_to.gte=2025-06-15`). An `active_from` of `0001-01-01` indicates no known start date, and an `active_to` of `9999-12-31` indicates the mapping is currently active.

Use Cases: Merchant-to-ticker mapping, corporate hierarchy lookups, sector and industry classification, point-in-time historical hierarchy analysis.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `lookup_name` | string | No | Lowercase merchant tag used for joining with the merchant-aggregates endpoint name field (e.g., 'asda', 'amazon'). |
| `lookup_name.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `lookup_name.gt` | string | No | Filter greater than the value. |
| `lookup_name.gte` | string | No | Filter greater than or equal to the value. |
| `lookup_name.lt` | string | No | Filter less than the value. |
| `lookup_name.lte` | string | No | Filter less than or equal to the value. |
| `ticker` | string | No | Stock ticker associated with the merchant (~250 US public companies mapped). Uses Bloomberg standard. |
| `ticker.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `ticker.gt` | string | No | Filter greater than the value. |
| `ticker.gte` | string | No | Filter greater than or equal to the value. |
| `ticker.lt` | string | No | Filter less than the value. |
| `ticker.lte` | string | No | Filter less than or equal to the value. |
| `listing_status` | string | No | Whether the merchant is a publicly listed company or private. Possible values: public, private. |
| `listing_status.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `active_from` | string | No | Date this merchant hierarchy mapping became applicable. A value of 0001-01-01 indicates no known start date. Use with active_to to match against transaction_date from the merchant-aggregates endpoint to perform point-in-time queries. Value must be formatted 'yyyy-mm-dd'. |
| `active_from.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `active_from.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `active_from.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `active_from.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `active_to` | string | No | Date after which this merchant hierarchy mapping is no longer applicable due to a corporate action. A value of 9999-12-31 indicates the mapping is currently active. Value must be formatted 'yyyy-mm-dd'. |
| `active_to.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `active_to.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `active_to.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `active_to.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'lookup_name' if not specified. The sort order defaults to 'asc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `active_from,active_to,category`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].active_from` | string | Date this merchant hierarchy mapping became applicable. A value of 0001-01-01 indicates no known start date. Use with active_to to match against transaction_date from the merchant-aggregates endpoint to perform point-in-time queries. |
| `results[].active_to` | string | Date after which this merchant hierarchy mapping is no longer applicable due to a corporate action. A value of 9999-12-31 indicates the mapping is currently active. |
| `results[].category` | string | Merchant category assigned by the data provider (Title Case, e.g., 'Groceries', 'General Merchandise', 'Travel'). Values are managed by the data provider and may expand over time. |
| `results[].grandparent_name` | string | Merchant's grandparent business name (Title Case). |
| `results[].grandparent_ticker` | string | Stock ticker of the merchant's grandparent company. Uses Bloomberg standard. |
| `results[].great_grandparent_name` | string | Merchant's great-grandparent business name (Title Case, top-level corporate entity). |
| `results[].great_grandparent_ticker` | string | Stock ticker of the merchant's great-grandparent company (top-level corporate entity). Uses Bloomberg standard. |
| `results[].industry` | string | Industry classification based on GICS/BICS/ICB standards. |
| `results[].industry_group` | string | Industry group classification based on GICS/BICS/ICB standards. |
| `results[].listing_status` | string | Whether the merchant is a publicly listed company or private. Possible values: public, private. |
| `results[].lookup_name` | string | Lowercase merchant tag used for joining with the merchant-aggregates endpoint name field (e.g., 'asda', 'amazon'). |
| `results[].normalized_name` | string | Cleaned, standardized merchant display name (Title Case, e.g., 'Asda', 'Amazon'). |
| `results[].parent_name` | string | Merchant's parent business name (Title Case). Useful for aggregating transactions across subsidiary brands. |
| `results[].parent_ticker` | string | Stock ticker of the merchant's parent company. Uses Bloomberg standard. |
| `results[].sector` | string | Sector classification based on GICS/BICS/ICB standards. |
| `results[].sub_industry` | string | Sub-industry classification based on GICS/BICS/ICB standards. |
| `results[].ticker` | string | Stock ticker associated with the merchant (~250 US public companies mapped). Uses Bloomberg standard. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "count": 1,
  "request_id": "a1b2c3d4e5f6",
  "results": [
    {
      "active_from": "0001-01-01",
      "active_to": "9999-12-31",
      "category": "General Merchandise",
      "grandparent_name": "Amazon Inc",
      "grandparent_ticker": "AMZN US",
      "great_grandparent_name": "Amazon Inc",
      "great_grandparent_ticker": "AMZN US",
      "industry": "E-Commerce Discretionary",
      "industry_group": "Retail - Discretionary",
      "listing_status": "public",
      "lookup_name": "amazon",
      "normalized_name": "Amazon Inc",
      "parent_name": "Amazon Inc",
      "parent_ticker": "AMZN US",
      "sector": "Consumer Discretionary",
      "sub_industry": "E-Commerce Discretionary",
      "ticker": "AMZN US"
    }
  ],
  "status": "OK"
}
```


## Plan Access

**Plan Access:** Included in select Consumer Spending expansion plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| European Consumer Spending by Merchant | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| European Consumer Spending by Merchant Business | Included |

## Plan Recency

**Plan Recency:** Included in select Consumer Spending expansion plans (updated weekly)

#### Individual Plans

| Plan | Recency |
| --- | --- |
| European Consumer Spending by Merchant | 30-day delayed |

#### Business Plans

| Plan | Recency |
| --- | --- |
| European Consumer Spending by Merchant Business | 7-day delayed |

## Plan History

**Plan History:** Records date back to November 1, 2021

#### Individual Plans

| Plan | History |
| --- | --- |
| European Consumer Spending by Merchant | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| European Consumer Spending by Merchant Business | All history |
