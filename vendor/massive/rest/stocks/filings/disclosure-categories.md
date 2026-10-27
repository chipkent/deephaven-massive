> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### Disclosure Categories

**Endpoint:** `GET /stocks/taxonomies/vX/disclosures`

**Description:**

Access the full standardized hierarchical taxonomy used to classify disclosures in SEC Form 8-K filings. It includes primary, secondary, and tertiary categories with detailed descriptions, providing the controlled vocabulary that powers the 8-K disclosures endpoint.

Use Cases: Event-classification models, category-level analytics and dashboards, cross-company comparisons, building category-aware filters, and thematic monitoring of corporate disclosures.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `taxonomy` | string | No | Taxonomy version that defines this classification (e.g., '1.0'). |
| `taxonomy.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `taxonomy.gt` | string | No | Filter greater than the value. |
| `taxonomy.gte` | string | No | Filter greater than or equal to the value. |
| `taxonomy.lt` | string | No | Filter less than the value. |
| `taxonomy.lte` | string | No | Filter less than or equal to the value. |
| `primary_category` | string | No | Top-level disclosure category. |
| `primary_category.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `primary_category.gt` | string | No | Filter greater than the value. |
| `primary_category.gte` | string | No | Filter greater than or equal to the value. |
| `primary_category.lt` | string | No | Filter less than the value. |
| `primary_category.lte` | string | No | Filter less than or equal to the value. |
| `secondary_category` | string | No | Mid-level disclosure category. |
| `secondary_category.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `secondary_category.gt` | string | No | Filter greater than the value. |
| `secondary_category.gte` | string | No | Filter greater than or equal to the value. |
| `secondary_category.lt` | string | No | Filter less than the value. |
| `secondary_category.lte` | string | No | Filter less than or equal to the value. |
| `tertiary_category` | string | No | Most specific disclosure category. |
| `tertiary_category.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `tertiary_category.gt` | string | No | Filter greater than the value. |
| `tertiary_category.gte` | string | No | Filter greater than or equal to the value. |
| `tertiary_category.lt` | string | No | Filter less than the value. |
| `tertiary_category.lte` | string | No | Filter less than or equal to the value. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '200' if not specified. The maximum allowed limit is '1000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'taxonomy' if not specified. The sort order defaults to 'desc' if not specified. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].description` | string | Detailed explanation of what the disclosure category covers, including typical examples. |
| `results[].primary_category` | string | Top-level disclosure category. |
| `results[].secondary_category` | string | Mid-level disclosure category. |
| `results[].taxonomy` | string | Taxonomy version that defines this classification (e.g., '1.0'). |
| `results[].tertiary_category` | string | Most specific disclosure category. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "request_id": "a1b2c3d4e5f6a7b8c9d0e1f2",
  "results": [
    {
      "description": "New CEO appointment with background, employment terms, and compensation.",
      "primary_category": "leadership_and_governance",
      "secondary_category": "executive_leadership",
      "taxonomy": "1.0",
      "tertiary_category": "ceo_appointment"
    },
    {
      "description": "Quarterly financial results including revenue, net income, EPS, and key operating metrics with management commentary.",
      "primary_category": "financial_results",
      "secondary_category": "earnings_and_performance",
      "taxonomy": "1.0",
      "tertiary_category": "quarterly_earnings"
    }
  ],
  "status": "OK"
}
```


## Plan Access

**Plan Access:** Included in all Stocks plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Stocks Basic | Included |
| Stocks Starter | Included |
| Stocks Developer | Included |
| Stocks Advanced | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Included |

## Plan Recency

**Plan Recency:** Updated as needed

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Updated as needed |
| Stocks Starter | Updated as needed |
| Stocks Developer | Updated as needed |
| Stocks Advanced | Updated as needed |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Updated as needed |

## Plan History

**Plan History:** Not applicable to this endpoint
