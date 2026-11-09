> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### Material Event Text (8-K)

**Endpoint:** `GET /stocks/filings/8-K/vX/text`

**Description:**

Retrieve parsed plain-text content from the core Items sections of Form 8-K current reports. These disclose major material events such as acquisitions, leadership changes, material contracts, and other significant developments in AI-ready format.

Use Cases: Real-time event detection and alerts, event-driven trading strategies, automated news digestion, M&A tracking, and corporate action timelines.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `cik` | string | No | SEC Central Index Key (10 digits, zero-padded). |
| `cik.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `cik.gt` | string | No | Filter greater than the value. |
| `cik.gte` | string | No | Filter greater than or equal to the value. |
| `cik.lt` | string | No | Filter less than the value. |
| `cik.lte` | string | No | Filter less than or equal to the value. |
| `ticker` | string | No | Stock ticker symbol for the company. |
| `ticker.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `ticker.gt` | string | No | Filter greater than the value. |
| `ticker.gte` | string | No | Filter greater than or equal to the value. |
| `ticker.lt` | string | No | Filter less than the value. |
| `ticker.lte` | string | No | Filter less than or equal to the value. |
| `form_type` | string | No | SEC form type (e.g., '8-K', '8-K/A' for amendments). |
| `form_type.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `form_type.gt` | string | No | Filter greater than the value. |
| `form_type.gte` | string | No | Filter greater than or equal to the value. |
| `form_type.lt` | string | No | Filter less than the value. |
| `form_type.lte` | string | No | Filter less than or equal to the value. |
| `filing_date` | string | No | Date when the filing was submitted to the SEC (formatted as YYYY-MM-DD). Value must be formatted 'yyyy-mm-dd'. |
| `filing_date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `filing_date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `filing_date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `filing_date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '10' if not specified. The maximum allowed limit is '100'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'filing_date' if not specified. The sort order defaults to 'desc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `accession_number,cik,filing_date`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].accession_number` | string | SEC accession number uniquely identifying the filing (e.g., '0000004962-25-000002'). |
| `results[].cik` | string | SEC Central Index Key (10 digits, zero-padded). |
| `results[].filing_date` | string | Date when the filing was submitted to the SEC (formatted as YYYY-MM-DD). |
| `results[].filing_url` | string | SEC URL source for the full filing. |
| `results[].form_type` | string | SEC form type (e.g., '8-K', '8-K/A' for amendments). |
| `results[].items_text` | string | Parsed text content from the 8-K filing, including item numbers and descriptions. |
| `results[].ticker` | string | Stock ticker symbol for the company. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "count": 2,
  "next_url": "https://api.massive.com/stocks/filings/8-K/vX/text?cursor=eyJsaW1pd...",
  "request_id": "a3f8b2c1d4e5f6g7",
  "results": [
    {
      "accession_number": "0000004962-25-000002",
      "cik": "0000004962",
      "filing_date": "2025-01-15",
      "filing_url": "https://www.sec.gov/Archives/edgar/data/4962/0000004962-25-000002.txt",
      "form_type": "8-K",
      "items_text": "Item 7.01\tRegulation FD Disclosure\n\nAmerican Express Company is hereby furnishing below delinquency and write-off statistics...",
      "ticker": "AXP"
    },
    {
      "accession_number": "0000320193-25-000010",
      "cik": "0000320193",
      "filing_date": "2025-01-14",
      "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/0000320193-25-000010.txt",
      "form_type": "8-K",
      "items_text": "Item 2.02\tResults of Operations and Financial Condition\n\nOn January 14, 2025, Apple Inc. announced financial results...",
      "ticker": "AAPL"
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

**Plan Recency:** Updated daily

#### Individual Plans

| Plan | Recency |
| --- | --- |
| Stocks Basic | Updated daily |
| Stocks Starter | Updated daily |
| Stocks Developer | Updated daily |
| Stocks Advanced | Updated daily |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Updated daily |

## Plan History

**Plan History:** Not applicable to this endpoint
