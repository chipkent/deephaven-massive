> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### Insider Holdings

**Endpoint:** `GET /stocks/insiders/vX/holdings`

**Description:**

Retrieve the roster of a company's insiders and the shares each one currently holds — one row per reporting owner per issuer. Each record identifies a director, officer, or 10%+ shareholder with their roles, officer title, first and latest filing dates, filing count, and direct and indirect share holdings plus ownership percentage, synthesized from all of that owner's SEC Form 3, 4, and 5 filings. Unlike the Form 3 and Form 4 endpoints, which return individual filings as submitted, this endpoint returns one consolidated, current ownership position per insider — use each owner's owner_cik to follow their complete filing history across companies through the Form 3, 4, and 5 endpoints.

Use Cases: Identifying who a company's insiders are, measuring insider ownership stakes and concentration, looking up a director or officer's CIK, and tracking an insider's holdings across multiple companies.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `tickers` | string | No | Filter for arrays that contain the value. |
| `tickers.all_of` | string | No | Filter for arrays that contain all of the values. Multiple values can be specified by using a comma separated list. |
| `tickers.any_of` | string | No | Filter for arrays that contain any of the values. Multiple values can be specified by using a comma separated list. |
| `issuer_cik` | string | No | SEC Central Index Key of the issuer company (10 digits, zero-padded). |
| `issuer_cik.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `owner_cik` | string | No | SEC Central Index Key of the reporting owner (10 digits, zero-padded). Stable across issuers and years; use it to follow a person through the Form 3, 4, and 5 endpoints. |
| `owner_cik.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `last_filing_date` | string | No | Date of the owner's most recent Form 3, 4, or 5 filing for this issuer (formatted as YYYY-MM-DD). Value must be formatted 'yyyy-mm-dd'. |
| `last_filing_date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `last_filing_date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `last_filing_date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `last_filing_date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '10000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'last_filing_date' if not specified. The sort order defaults to 'desc' if not specified. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].filing_count` | integer | Number of Form 3, 4, and 5 filings by the owner for this issuer. |
| `results[].first_filing_date` | string | Date of the owner's first Form 3, 4, or 5 filing for this issuer (formatted as YYYY-MM-DD). Filings begin in 2003. |
| `results[].issuer_cik` | string | SEC Central Index Key of the issuer company (10 digits, zero-padded). |
| `results[].issuer_name` | string | Name of the issuer company as reported in the most recent filing. |
| `results[].last_filing_date` | string | Date of the owner's most recent Form 3, 4, or 5 filing for this issuer (formatted as YYYY-MM-DD). |
| `results[].officer_title` | string | Title of the officer in the most recent filing, if the reporting owner is an officer. |
| `results[].oldest_position_date` | string | Earliest as_of_date among the summed positions (formatted as YYYY-MM-DD). A date far behind last_filing_date means at least one position has not been restated recently and may be stale. |
| `results[].owner_cik` | string | SEC Central Index Key of the reporting owner (10 digits, zero-padded). Stable across issuers and years; use it to follow a person through the Form 3, 4, and 5 endpoints. |
| `results[].owner_name` | string | Name of the reporting owner (individual or entity) as reported in the most recent filing. |
| `results[].ownership_pct` | number | shares_owned as a percentage of the issuer's shares outstanding (largest share class) as of last_filing_date. Absent when shares outstanding is not available for the issuer. |
| `results[].position_count` | integer | Number of positions summed into shares_owned: one for direct holdings plus one per indirect vehicle. |
| `results[].roles` | array[string] | Relationships of the reporting owner to the issuer in the most recent filing. Possible values: director, officer, ten_percent_owner, other. |
| `results[].shares_owned` | number | Shares of the issuer's non-derivative securities the owner holds: the sum of the latest reported balance of each position (direct, and each indirect vehicle) across their Form 3, 4, and 5 filings. As filed, not adjusted for splits. Unexercised options and other derivative securities are not included. Absent when the owner has never reported a share balance. |
| `results[].shares_owned_direct` | number | Shares held directly, in the owner's own name. |
| `results[].shares_owned_indirect` | number | Shares held indirectly through trusts, LLCs, family members, and other vehicles. |
| `results[].tickers` | array[string] | Ticker symbols of the issuer as of the owner's most recent filing. Multiple symbols may indicate different share classes. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "count": 1,
  "next_url": "https://api.massive.com/stocks/insiders/vX/holdings?cursor=eyJsaW1pd...",
  "request_id": "047c7035a86042b1925118e5f68d81b0",
  "results": [
    {
      "filing_count": 94,
      "first_filing_date": "2013-10-09",
      "issuer_cik": "0001045810",
      "issuer_name": "NVIDIA CORP",
      "last_filing_date": "2026-09-18",
      "officer_title": "EVP & Chief Financial Officer",
      "oldest_position_date": "2021-09-15",
      "owner_cik": "0001588670",
      "owner_name": "Kress Colette",
      "ownership_pct": 0.0198,
      "position_count": 11,
      "roles": [
        "officer"
      ],
      "shares_owned": 4776533,
      "shares_owned_direct": 765103,
      "shares_owned_indirect": 4011430,
      "tickers": [
        "NVDA"
      ]
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
