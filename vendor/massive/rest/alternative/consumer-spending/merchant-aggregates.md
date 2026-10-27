> For the full documentation index, see: https://massive.com/docs/llms.txt

# REST
## Alternative

### Merchant Aggregates

**Endpoint:** `GET /consumer-spending/eu/v1/merchant-aggregates`

**Description:**

Aggregated European consumer spending data powered by [Fable Data](https://www.fabledata.com/). Track daily credit card, debit card, and open banking transaction totals for ~250 US public companies across 6 European countries (UK, Germany, France, Italy, Spain, Austria). Data available from 2016; Spain from 2019.

Raw spend totals must be normalized to accurately measure spending trends. See [Normalizing Merchant Aggregates](/docs/rest/alternative/overview#normalizing-merchant-aggregates) for methodology.

Use Cases: Consumer spending trend analysis, merchant revenue estimation, cross-border spending comparisons, sector and industry benchmarking.

## Query Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `transaction_date` | string | No | The calendar date when the consumer transactions occurred. Value must be formatted 'yyyy-mm-dd'. |
| `transaction_date.gt` | string | No | Filter greater than the value. Value must be formatted 'yyyy-mm-dd'. |
| `transaction_date.gte` | string | No | Filter greater than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `transaction_date.lt` | string | No | Filter less than the value. Value must be formatted 'yyyy-mm-dd'. |
| `transaction_date.lte` | string | No | Filter less than or equal to the value. Value must be formatted 'yyyy-mm-dd'. |
| `name` | string | No | The merchant or entity name for this aggregate (lowercase). When type is 'merchant', this value can be used as the lookup_name in the merchant-hierarchy endpoint to retrieve full corporate hierarchy details. |
| `name.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `name.gt` | string | No | Filter greater than the value. |
| `name.gte` | string | No | Filter greater than or equal to the value. |
| `name.lt` | string | No | Filter less than the value. |
| `name.lte` | string | No | Filter less than or equal to the value. |
| `user_country` | string | No | Country of consumer's residence (ISO codes). Available countries: UK, DE, FR, ES, IT, AT. Values outside these are mapped to 'unknown'. |
| `user_country.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `channel` | string | No | Transaction channel. Possible values: online, offline, bnpl (buy now pay later platforms such as Scala, Klarna, Zilch). |
| `channel.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `consumer_type` | string | No | The panel the account is sourced from. Possible values: consumer_credit, consumer_debit, open_banking. |
| `consumer_type.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `parent_name` | string | No | Merchant's parent business name (Title Case). Useful for aggregating transactions across subsidiary brands. Also available in the merchant-hierarchy endpoint for full corporate structure. |
| `parent_name.any_of` | string | No | Filter equal to any of the values. Multiple values can be specified by using a comma separated list. |
| `parent_name.gt` | string | No | Filter greater than the value. |
| `parent_name.gte` | string | No | Filter greater than or equal to the value. |
| `parent_name.lt` | string | No | Filter less than the value. |
| `parent_name.lte` | string | No | Filter less than or equal to the value. |
| `limit` | integer | No | Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '5000'. |
| `sort` | string | No | A comma separated list of sort columns. For each column, append '.asc' or '.desc' to specify the sort direction. The sort column defaults to 'transaction_date' if not specified. The sort order defaults to 'desc' if not specified. |

## Response Attributes

| Field | Type | Description |
| --- | --- | --- |
| `next_url` | string | If present, this value can be used to fetch the next page. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | The results for this request. |
| `results[].channel` | string | Transaction channel. Possible values: online, offline, bnpl (buy now pay later platforms such as Scala, Klarna, Zilch). |
| `results[].consumer_type` | string | The panel the account is sourced from. Possible values: consumer_credit, consumer_debit, open_banking. |
| `results[].eight_day_rolling_category_accounts` | integer | The count of unique consumer accounts active for a given country, consumer type, and that transacted in this merchant category (MCC Group) over the prior 8 days. Used for short-term normalization calculations (e.g., spend per 1000 panelists = (total_spend / eight_day_rolling_category_accounts) * 1000). |
| `results[].eight_day_rolling_total_accounts` | integer | The count of unique consumer accounts active for a given country and consumer type that had any transaction activity (across all categories) over the prior 8 days. Provides a baseline for total panel size. |
| `results[].mcc_group` | string | Merchant category code group associated with the merchant or payment processor. |
| `results[].merchant_industry` | string | Industry name based on GICS/BICS/ICB data classification for the merchant. |
| `results[].merchant_ticker` | string | Stock ticker associated with the merchant (~250 US public companies mapped). Uses Bloomberg tickers and they are point in time. |
| `results[].name` | string | The merchant or entity name for this aggregate (lowercase). When type is 'merchant', this value can be used as the lookup_name in the merchant-hierarchy endpoint to retrieve full corporate hierarchy details. |
| `results[].parent_name` | string | Merchant's parent business name (Title Case). Useful for aggregating transactions across subsidiary brands. Also available in the merchant-hierarchy endpoint for full corporate structure. |
| `results[].published_date` | string | The date when this data version was published. For original data, this is approximately 7 days after transaction_date. |
| `results[].spend_in_distinct_account_key_count` | integer | The count of distinct account keys (unique consumer accounts) with inbound transactions. |
| `results[].spend_in_spend` | number | The total inbound transaction amount (refunds, returns, credits) in the specified currency for this aggregation. Values are positive, representing money flowing back into consumer accounts. |
| `results[].spend_in_transaction_count` | integer | The count of inbound transactions (refunds, returns). |
| `results[].spend_out_distinct_account_key_count` | integer | The count of distinct account keys (unique consumer accounts) with outbound transactions. |
| `results[].spend_out_spend` | number | The total outbound transaction amount (money spent by consumers) in the specified currency for this aggregation. Values are negative, representing money flowing out of consumer accounts. |
| `results[].spend_out_transaction_count` | integer | The count of outbound transactions (purchases, payments). |
| `results[].total_accounts` | integer | The total count of distinct consumer accounts with any transaction activity for this aggregation. |
| `results[].total_spend` | number | Sum of spend_out_spend and spend_in_spend. Typically negative (net outflow). A positive value indicates refunds exceeded new spending for this aggregation. |
| `results[].total_transactions` | integer | The total count of all transactions (outbound + inbound). |
| `results[].transaction_currency` | string | ISO currency code for the transaction (base card/account currency). Always EUR or GBP in this dataset. All spend amounts are denominated in this currency. |
| `results[].transaction_date` | string | The calendar date when the consumer transactions occurred. |
| `results[].twenty_eight_day_rolling_category_accounts` | integer | The count of unique consumer accounts active for a given country, consumer type, and that transacted in this merchant category (MCC Group) over the prior 28 days. Used for longer-term normalization calculations. |
| `results[].twenty_eight_day_rolling_total_accounts` | integer | The count of unique consumer accounts active for a given country and consumer type that had any transaction activity (across all categories) over the prior 28 days. Provides a longer-term baseline for total panel size. |
| `results[].type` | string | The type of aggregation. Can be 'merchant' or 'payment_processor'. |
| `results[].user_country` | string | Country of consumer's residence (ISO codes). Available countries: UK, DE, FR, ES, IT, AT. Values outside these are mapped to 'unknown'. |
| `status` | enum: OK | The status of this request's response. |

## Sample Response

```json
{
  "request_id": "c38af84c88ba484cb8430ba1511b1f24",
  "results": [
    {
      "channel": "offline",
      "consumer_type": "open_banking",
      "eight_day_rolling_category_accounts": 40346,
      "eight_day_rolling_total_accounts": 463648,
      "mcc_group": "memberships & organizations",
      "merchant_industry": "Leisure Clubs & Facilities",
      "merchant_ticker": "0206472D US",
      "name": "zumba fitness",
      "parent_name": "Zumba Fitness Llc",
      "published_date": "2026-01-25",
      "spend_in_distinct_account_key_count": 0,
      "spend_in_spend": 0,
      "spend_in_transaction_count": 0,
      "spend_out_distinct_account_key_count": 1,
      "spend_out_spend": -3,
      "spend_out_transaction_count": 1,
      "total_accounts": 1,
      "total_spend": -3,
      "total_transactions": 1,
      "transaction_currency": "GBP",
      "transaction_date": "2026-01-18",
      "twenty_eight_day_rolling_category_accounts": 91642,
      "twenty_eight_day_rolling_total_accounts": 524917,
      "type": "merchant",
      "user_country": "UK"
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

**Plan Recency:** Included in select Consumer Spending expansion plans (updated daily)

#### Individual Plans

| Plan | Recency |
| --- | --- |
| European Consumer Spending by Merchant | 30-day delayed |

#### Business Plans

| Plan | Recency |
| --- | --- |
| European Consumer Spending by Merchant Business | 7-day delayed |

## Plan History

**Plan History:** Records date back to January 1, 2016

#### Individual Plans

| Plan | History |
| --- | --- |
| European Consumer Spending by Merchant | All history |

#### Business Plans

| Plan | History |
| --- | --- |
| European Consumer Spending by Merchant Business | All history |
