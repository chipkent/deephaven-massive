> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### SEC EDGAR Index

**Endpoint:** `GET /stocks/filings/vX/index`

**Description:**

Retrieve the master index of all SEC EDGAR filings, including form types, filing dates, CIKs, tickers, issuer names, accession numbers, and direct links to original documents on SEC.gov. This serves as the central discovery layer for the entire EDGAR database, allowing powerful filtering and sorting without manual website searches.

Use Cases: Automated filing discovery and monitoring, regulatory compliance alerts, due diligence pipelines, competitive intelligence, and historical disclosure research.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `cik` | string | No | SEC Central Index Key (CIK) identifying the filing entity. |
| `cik.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `cik.gt` | string | No | Filter greater than the value. |
| `cik.gte` | string | No | Filter greater than or equal to the value. |
| `cik.lt` | string | No | Filter less than the value. |
| `cik.lte` | string | No | Filter less than or equal to the value. |
| `ticker` | string | No | Stock ticker symbol for the filing entity, if available. |
| `ticker.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `ticker.gt` | string | No | Filter greater than the value. |
| `ticker.gte` | string | No | Filter greater than or equal to the value. |
| `ticker.lt` | string | No | Filter less than the value. |
| `ticker.lte` | string | No | Filter less than or equal to the value. |
| `form_type` | string | No | SEC form type (e.g., '10-K', '10-Q', '8-K', 'S-1', '4', etc.). |
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
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '1000' if not specified. The maximum allowed limit is '10000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'filing_date' if not specified. The sort order defaults to 'desc' if not specified. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].accession_number` | string | SEC accession number uniquely identifying the filing (e.g., '0000320193-24-000123'). |
| `results[].cik` | string | SEC Central Index Key (CIK) identifying the filing entity. |
| `results[].filing_date` | string | Date when the filing was submitted to the SEC (formatted as YYYY-MM-DD). |
| `results[].filing_url` | string | Direct URL to the filing on the SEC EDGAR website. |
| `results[].form_type` | string | SEC form type (e.g., '10-K', '10-Q', '8-K', 'S-1', '4', etc.). |
| `results[].issuer_name` | string | Name of the company or entity that submitted the filing. |
| `results[].ticker` | string | Stock ticker symbol for the filing entity, if available. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "next_url": "https://api.massive.com/stocks/filings/vX/index?cursor=eyJsaW1pd...",
  "request_id": "1daccfd9794e482e96d104dee6ed432b",
  "results": [
    {
      "accession_number": "0000320193-25-000079",
      "cik": "0000320193",
      "filing_date": "2025-10-31",
      "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/0000320193-25-000079.txt",
      "form_type": "10-K",
      "issuer_name": "Apple Inc.",
      "ticker": "AAPL"
    },
    {
      "accession_number": "0000950170-25-010491",
      "cik": "0000789019",
      "filing_date": "2025-01-29",
      "filing_url": "https://www.sec.gov/Archives/edgar/data/789019/0000950170-25-010491.txt",
      "form_type": "10-Q",
      "issuer_name": "MICROSOFT CORP",
      "ticker": "MSFT"
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
