> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Stocks

### Insider Trading Activity

**Endpoint:** `GET /stocks/insiders/vX/activity`

**Description:**

Retrieve aggregated insider trading activity for a company — how much its corporate insiders bought and sold each day. Each row sums all open-market purchases and sales by a single issuer's directors, officers, and 10%+ shareholders for one transaction date, drawn from SEC Form 4 and Form 5 filings, and reports purchase and sale counts, share totals, dollar values, and net buying or selling. Unlike the Form 3 and Form 4 endpoints, which return individual insider filings as submitted, this endpoint delivers the pre-aggregated daily totals across every insider at a company — so you can gauge net insider sentiment or build a rolling window over a date range without parsing raw filings.

Use Cases: Measuring net insider buying and selling, tracking insider sentiment trends, screening for unusual insider activity, management confidence analysis, and ranking companies by aggregate insider flow.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `tickers` | string | No | Filter for arrays that contain the value. |
| `tickers.all_of` | string | No | Filter for arrays that contain all of the values. Multiple values can be specified by using a comma separated list. |
| `tickers.any_of` | string | No | Filter for arrays that contain any of the values. Multiple values can be specified by using a comma separated list. |
| `issuer_cik` | string | No | SEC Central Index Key of the issuer company (10 digits, zero-padded). |
| `issuer_cik.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `transaction_date` | string | No | Date of the transactions (formatted as YYYY-MM-DD). Value must be formatted 'yyyy-mm-dd'. |
| `transaction_date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `transaction_date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `transaction_date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `transaction_date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '10000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'transaction_date' if not specified. The sort order defaults to 'desc' if not specified. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].issuer_cik` | string | SEC Central Index Key of the issuer company (10 digits, zero-padded). |
| `results[].issuer_name` | string | Name of the issuer company as reported in the filings. |
| `results[].last_filing_date` | string | Filing date of the latest filing that reported one of these transactions (formatted as YYYY-MM-DD). Transactions are usually filed within two business days. |
| `results[].net_shares` | number | Shares purchased minus shares sold. |
| `results[].net_value` | number | Value purchased minus value sold in USD. |
| `results[].planned_purchase_value` | number | Value of shares purchased under a Rule 10b5-1 trading plan in USD. Zero on filings before the plan checkbox was introduced in 2023. |
| `results[].planned_sale_value` | number | Value of shares sold under a Rule 10b5-1 trading plan in USD. Zero on filings before the plan checkbox was introduced in 2023. |
| `results[].purchase_count` | integer | Number of open-market purchase transactions. |
| `results[].purchase_shares` | number | Shares purchased. |
| `results[].purchase_value` | number | Value of shares purchased in USD. |
| `results[].purchaser_count` | integer | Number of distinct insiders who purchased. |
| `results[].sale_count` | integer | Number of open-market sale transactions. |
| `results[].sale_shares` | number | Shares sold. |
| `results[].sale_value` | number | Value of shares sold in USD. |
| `results[].seller_count` | integer | Number of distinct insiders who sold. |
| `results[].tickers` | array[string] | Ticker symbols of the issuer as of the transaction date. Multiple symbols may indicate different share classes; the totals cover the issuer, not each ticker. |
| `results[].transaction_date` | string | Date of the transactions (formatted as YYYY-MM-DD). |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "count": 1,
  "next_url": "https://api.massive.com/stocks/insiders/vX/activity?cursor=eyJsaW1pd...",
  "request_id": "047c7035a86042b1925118e5f68d81b0",
  "results": [
    {
      "issuer_cik": "0001045810",
      "issuer_name": "NVIDIA CORP",
      "last_filing_date": "2026-09-18",
      "net_shares": -34918,
      "net_value": -7648632.45,
      "planned_purchase_value": 0,
      "planned_sale_value": 7648632.45,
      "purchase_count": 0,
      "purchase_shares": 0,
      "purchase_value": 0,
      "purchaser_count": 0,
      "sale_count": 3,
      "sale_shares": 34918,
      "sale_value": 7648632.45,
      "seller_count": 1,
      "tickers": [
        "NVDA"
      ],
      "transaction_date": "2026-09-17"
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
