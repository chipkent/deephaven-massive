> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### Initial Insider Ownership (Form 3)

**Endpoint:** `GET /stocks/filings/vX/form-3`

**Description:**

Retrieve SEC Form 3 filings that report the initial beneficial ownership of securities by corporate insiders (directors, officers, and 10%+ shareholders). These filings establish the baseline ownership position when an insider first becomes subject to Section 16 reporting requirements.

Use Cases: Tracking new insider positions, monitoring executive and director ownership, due diligence on large shareholders, building ownership databases, and regulatory compliance checks.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `issuer_cik` | string | No | SEC Central Index Key of the issuer company (10 digits, zero-padded). |
| `issuer_cik.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `owner_cik` | string | No | SEC Central Index Key of the reporting owner (10 digits, zero-padded). |
| `owner_cik.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `tickers` | string | No | Filter for arrays that contain the value. |
| `tickers.all_of` | string | No | Filter for arrays that contain all of the values. Multiple values can be specified by using a comma separated list. |
| `tickers.any_of` | string | No | Filter for arrays that contain any of the values. Multiple values can be specified by using a comma separated list. |
| `form_type` | string | No | SEC form type ('3' for initial filing, '3/A' for amendments). |
| `filing_date` | string | No | Date when the filing was submitted to the SEC (formatted as YYYY-MM-DD). Value must be formatted 'yyyy-mm-dd'. |
| `filing_date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `filing_date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `filing_date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `filing_date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '10000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'filing_date' if not specified. The sort order defaults to 'desc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `accession_number,aff_10b5_one,date_of_original_submission`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].accession_number` | string | Unique SEC accession number for the filing (e.g., '0001209191-25-012345'). |
| `results[].aff_10b5_one` | boolean | Whether the transaction was made pursuant to a Rule 10b5-1 trading plan. |
| `results[].date_of_original_submission` | string | Date of the original filing submission for amendment filings (3/A). Null for initial filings (formatted as YYYY-MM-DD). |
| `results[].direct_or_indirect` | string | Whether ownership is direct ('D') or indirect ('I'). |
| `results[].exercise_date` | string | Date exercisable for derivative securities (formatted as YYYY-MM-DD). |
| `results[].exercise_price` | number | Exercise or conversion price of derivative securities in USD. |
| `results[].filing_date` | string | Date when the filing was submitted to the SEC (formatted as YYYY-MM-DD). |
| `results[].filing_url` | string | Direct URL to the filing on the SEC EDGAR website. |
| `results[].footnotes` | array[object] | List of footnotes from the filing that are relevant to this row, each with an id and description. |
| `results[].form_type` | string | SEC form type ('3' for initial filing, '3/A' for amendments). |
| `results[].is_director` | boolean | Whether the reporting owner is a director of the issuer. |
| `results[].is_officer` | boolean | Whether the reporting owner is an officer of the issuer. |
| `results[].is_other` | boolean | Whether the reporting owner has another relationship with the issuer. |
| `results[].is_ten_percent_owner` | boolean | Whether the reporting owner holds 10% or more of a class of equity securities. |
| `results[].issuer_cik` | string | SEC Central Index Key of the issuer company (10 digits, zero-padded). |
| `results[].issuer_name` | string | Name of the issuer company as reported in the filing. |
| `results[].nature_of_ownership` | string | Nature of indirect ownership (e.g., 'By Trust', 'By Spouse'). |
| `results[].not_subject_to_section_16` | boolean | Whether the reporting owner is not subject to Section 16 of the Securities Exchange Act. |
| `results[].officer_title` | string | Title of the officer, if the reporting owner is an officer. |
| `results[].owner_cik` | string | SEC Central Index Key of the reporting owner (10 digits, zero-padded). |
| `results[].owner_name` | string | Name of the reporting owner (individual or entity). |
| `results[].period_of_report` | string | Date of the event triggering the filing (formatted as YYYY-MM-DD). |
| `results[].remarks` | string | Additional remarks included in the filing. |
| `results[].security_title` | string | Title or description of the security (e.g., 'Common Stock', 'Stock Option'). |
| `results[].security_type` | string | Type of security ('non-derivative' or 'derivative'). |
| `results[].shares_owned` | number | Number of shares beneficially owned. |
| `results[].tickers` | array[string] | A list of ticker symbols for the issuer company. Multiple symbols may indicate different share classes. |
| `results[].underlying_security_shares` | number | Number of underlying shares for derivative holdings. |
| `results[].underlying_security_title` | string | Title of the underlying security for derivative holdings. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "count": 1,
  "next_url": "https://api.massive.com/stocks/filings/vX/form-3?cursor=eyJsaW1pd...",
  "request_id": "047c7035a86042b1925118e5f68d81b0",
  "results": [
    {
      "accession_number": "0001628280-26-022046",
      "aff_10b5_one": null,
      "date_of_original_submission": null,
      "direct_or_indirect": "D",
      "exercise_date": null,
      "exercise_price": null,
      "filing_date": "2026-03-30",
      "filing_url": "https://www.sec.gov/Archives/edgar/data/1903508/0001628280-26-022046.txt",
      "footnotes": [
        {
          "description": "The options granted on May 17, 2022 vested 100% on the third anniversary of the grant date.",
          "id": "F1"
        },
        {
          "description": "The exercise price of the options granted on May 17, 2022 was GPB 8.85, or approximately $11.80 based on a GBP to USD exchange rate as of March 27, 2026 of 1.3336.",
          "id": "F2"
        }
      ],
      "form_type": "3",
      "is_director": false,
      "is_officer": true,
      "is_other": false,
      "is_ten_percent_owner": false,
      "issuer_cik": "0001903508",
      "issuer_name": "Public Policy Holding Company, Inc.",
      "nature_of_ownership": null,
      "not_subject_to_section_16": null,
      "officer_title": "Chief Administrative Officer",
      "owner_cik": "0002125791",
      "owner_name": "Mazzanti Matthew Ross",
      "period_of_report": "2026-03-20",
      "remarks": null,
      "security_title": "Options",
      "security_type": "derivative",
      "shares_owned": null,
      "tickers": [
        "PPHC"
      ],
      "underlying_security_shares": 9000,
      "underlying_security_title": "Common Stock, $0.001 par value"
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
