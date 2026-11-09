> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Economy

### Funding Conditions

**Endpoint:** `GET /fed/v1/funding-conditions`

**Description:**

Daily U.S. overnight funding snapshot — one row per day showing how cheap or tight it is for banks and dealers to borrow cash overnight. It pulls together the Fed's policy rates (effective fed funds rate, IORB, and the target range), SOFR—the recommended alternative to USD LIBOR—and OBFR, a measure of wholesale, unsecured overnight bank funding costs. It also includes trading volume and rate-distribution detail for SOFR and OBFR, the size of the NY Fed's overnight repo and reverse repo operations, and short-term borrowing costs for top-rated commercial paper (90-day, financial and nonfinancial).

Use Cases: Monitoring short-term funding stresses, tracking the transmission of monetary policy across secured and unsecured markets, analyzing repo and reverse-repo flows, and supporting macroeconomic and rates research.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `date` | string | No | Calendar date of the observation (YYYY-MM-DD). Value must be formatted 'yyyy-mm-dd'. |
| `date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '50000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'date' if not specified. The sort order defaults to 'asc' if not specified. |
| `fields` | string | No | A comma-separated list of fields to return for each result (e.g. `date,effective_fed_funds_rate,effective_fed_funds_volume`). If omitted, all fields are returned. When set, each result contains only the fields you list; every other field is dropped, even fields a full response always includes. Nested fields can be specified using dot notation (for example, `parent.child`). |

## Response Formats

This endpoint returns JSON by default, and Apache Arrow when the request sends `Accept: application/vnd.apache.arrow.file`. The Arrow body has no `next_url`; to page through results, read the next page's URL from the `Link` response header (`rel="next"`).

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].date` | string | Calendar date of the observation (YYYY-MM-DD). |
| `results[].effective_fed_funds_rate` | number | Federal Funds Effective Rate as a percentage (DFF series from FRED); published daily. |
| `results[].effective_fed_funds_volume` | number | Total transaction volume underlying the effective federal funds rate calculation, in billions of U.S. dollars; reported on business days. |
| `results[].fed_funds_target_lower` | number | Lower bound of the federal funds target range as a percentage (DFEDTARL series from FRED); posted every calendar day. |
| `results[].fed_funds_target_upper` | number | Upper bound of the federal funds target range as a percentage (DFEDTARU series from FRED); posted every calendar day. |
| `results[].fed_overnight_repo_treasury_amount` | number | NY Fed overnight repo operation amount against Treasury collateral, in billions of U.S. dollars (RPONTSYD series from FRED); reported on business days. |
| `results[].fed_overnight_reverse_repo_treasury_amount` | number | NY Fed overnight reverse repo operation amount against Treasury collateral, in billions of U.S. dollars (RRPONTSYD series from FRED); reported on business days. |
| `results[].financial_commercial_paper_90d_rate` | number | 90-day AA financial commercial paper interest rate as a percentage, quoted on a discount basis (DCPF3M series from FRED); reported on business days, with occasional source-level gaps. |
| `results[].interest_on_reserve_balances` | number | Interest rate paid on reserve balances as a percentage (IORB series from FRED); posted every calendar day. |
| `results[].nonfinancial_commercial_paper_90d_rate` | number | 90-day AA nonfinancial commercial paper interest rate as a percentage, quoted on a discount basis (DCPN3M series from FRED); reported on business days, with occasional source-level gaps. |
| `results[].obfr_25th_percentile` | number | 25th percentile transaction rate of the OBFR distribution as a percentage; posted on business days. |
| `results[].obfr_75th_percentile` | number | 75th percentile transaction rate of the OBFR distribution as a percentage; posted on business days. |
| `results[].obfr_volume` | number | Total transaction volume underlying the OBFR calculation, in billions of U.S. dollars; reported on business days. |
| `results[].overnight_bank_funding_rate` | number | Overnight Bank Funding Rate (OBFR) as a percentage; published by the Federal Reserve Bank of New York and reported on business days. |
| `results[].secured_overnight_financing_rate` | number | Secured Overnight Financing Rate (SOFR) as a percentage; published by the Federal Reserve Bank of New York and reported on business days. |
| `results[].sofr_25th_percentile` | number | 25th percentile transaction rate of the SOFR distribution as a percentage; posted on business days. |
| `results[].sofr_75th_percentile` | number | 75th percentile transaction rate of the SOFR distribution as a percentage; posted on business days. |
| `results[].sofr_volume` | number | Total transaction volume underlying the SOFR calculation, in billions of U.S. dollars; reported on business days. |
| `results[].tgcr_25th_percentile` | number | 25th percentile transaction rate of the TGCR distribution as a percentage; posted on business days. |
| `results[].tgcr_75th_percentile` | number | 75th percentile transaction rate of the TGCR distribution as a percentage; posted on business days. |
| `results[].tri_party_general_collateral_rate` | number | Tri-Party General Collateral Rate (TGCR) as a percentage; published by the Federal Reserve Bank of New York as a repo reference rate and reported on business days. |
| `results[].tri_party_general_collateral_volume` | number | Total transaction volume underlying the TGCR calculation, in billions of U.S. dollars; reported on business days. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "count": 1,
  "request_id": 1,
  "results": [
    {
      "date": "2025-06-02",
      "effective_fed_funds_rate": 4.33,
      "effective_fed_funds_volume": 98,
      "fed_funds_target_lower": 4.25,
      "fed_funds_target_upper": 4.5,
      "fed_overnight_repo_treasury_amount": 0,
      "fed_overnight_reverse_repo_treasury_amount": 214.5,
      "financial_commercial_paper_90d_rate": 4.35,
      "interest_on_reserve_balances": 4.4,
      "nonfinancial_commercial_paper_90d_rate": 4.34,
      "obfr_25th_percentile": 4.3,
      "obfr_75th_percentile": 4.33,
      "obfr_volume": 305,
      "overnight_bank_funding_rate": 4.32,
      "secured_overnight_financing_rate": 4.31,
      "sofr_25th_percentile": 4.28,
      "sofr_75th_percentile": 4.32,
      "sofr_volume": 2214,
      "tgcr_25th_percentile": 4.27,
      "tgcr_75th_percentile": 4.31,
      "tri_party_general_collateral_rate": 4.3,
      "tri_party_general_collateral_volume": 1876
    }
  ],
  "status": "OK"
}
```


## Plan Access

**Plan Access:** Included in all plans

#### Individual Plans

| Plan | Access |
| --- | --- |
| All Stocks plans | Included |
| All Options plans | Included |
| All Indices plans | Included |
| All Currencies plans | Included |

#### Business Plans

| Plan | Access |
| --- | --- |
| Stocks Business | Included |
| Options Business | Included |
| Indices Business | Included |
| Currencies Business | Included |

## Plan Recency

**Plan Recency:** Updated daily

#### Individual Plans

| Plan | Recency |
| --- | --- |
| All Stocks plans | Updated daily |
| All Options plans | Updated daily |
| All Indices plans | Updated daily |
| All Currencies plans | Updated daily |

#### Business Plans

| Plan | Recency |
| --- | --- |
| Stocks Business | Updated daily |
| Options Business | Updated daily |
| Indices Business | Updated daily |
| Currencies Business | Updated daily |

## Plan History

**Plan History:** Records date back to July 1, 1954

#### Individual Plans

| Plan | History |
| --- | --- |
| All Stocks plans | All history |
| All Options plans | All history |
| All Indices plans | All history |
| All Currencies plans | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| Stocks Business | All history |
| Options Business | All history |
| Indices Business | All history |
| Currencies Business | All history |
