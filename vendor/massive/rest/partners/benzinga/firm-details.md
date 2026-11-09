> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Partners

### Firm Details

**Endpoint:** `GET /benzinga/v1/firms`

**Description:**

Retrieve structured data on analyst firms, including firm names and identifiers. Each record can be linked to associated analysts and ratings to provide context around the sources of equity research. This dataset helps map coverage across institutions and supports analysis of firm-level activity in the market.

Use Cases: Research coverage mapping, institutional analysis, source attribution.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `benzinga_id` | string | No | The identifier used by Benzinga for this record. |
| `benzinga_id.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `benzinga_id.gt` | string | No | Filter greater than the value. |
| `benzinga_id.gte` | string | No | Filter greater than or equal to the value. |
| `benzinga_id.lt` | string | No | Filter less than the value. |
| `benzinga_id.lte` | string | No | Filter less than or equal to the value. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'name' if not specified. The sort order defaults to 'asc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `benzinga_id,currency,last_updated`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].benzinga_id` | string | The identifier used by Benzinga for this record. |
| `results[].currency` | string | Primary currency used by the financial firm, with some entries having null values. |
| `results[].last_updated` | string | Timestamp indicating when the firm's information was last modified or verified in the database. |
| `results[].name` | string | The name of a research firm or investment bank which issues ratings. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "count": 1,
  "request_id": 1,
  "results": [
    {
      "benzinga_id": "5e147c6b7da4190001b287b4",
      "currency": "USD",
      "last_updated": "2020-01-07T12:41:25Z",
      "name": "Piper Sandler"
    }
  ],
  "status": "OK"
}
```


## Plan Access

**Plan Access:** Included in select Benzinga expansion plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| Benzinga Analyst Ratings | Included |
| Benzinga Analyst Insights | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Benzinga Analyst Ratings Business | Included |
| Benzinga Analyst Insights Business | Included |

## Plan Recency

**Plan Recency:** Updated every 2 hours

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Benzinga Analyst Ratings | Updated every 2 hours |
| Benzinga Analyst Insights | Updated every 2 hours |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Benzinga Analyst Ratings Business | Updated every 2 hours |
| Benzinga Analyst Insights Business | Updated every 2 hours |

## Plan History

**Plan History:** Not applicable to this endpoint
