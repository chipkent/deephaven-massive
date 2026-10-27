> For the full documentation index, see: https://massive.com/docs/llms.txt

# Alternative Data Overview

Our Alternative Data APIs provide access to non-traditional datasets that complement core market data. These datasets offer unique signals for investment research, competitive analysis, and macro trends that aren't captured by price, volume, or fundamental data alone.

## Available Endpoints

Browse the endpoints available for Alternative data. Each row links to its dedicated documentation, with a short description and the API path.

| Endpoints | Description | Path |
| --- | --- | --- |
| **Consumer Spending (EU)** | | |
| [Merchant Aggregates](https://massive.com/docs/rest/alternative/consumer-spending/merchant-aggregates.md) | Aggregated European consumer spending data powered by [Fable Data](https://www.fabledata.com/). Track daily credit card, debit card, and open banking transaction totals for ~250 US public companies across 6 European countries (UK, Germany, France, Italy, Spain, Austria). Data available from 2016; Spain from 2019. Raw spend totals must be normalized to accurately measure spending trends. See [Normalizing Merchant Aggregates](https://massive.com/docs/rest/alternative/overview.md#normalizing-merchant-aggregates) for methodology. Use Cases: Consumer spending trend analysis, merchant revenue estimation, cross-border spending comparisons, sector and industry benchmarking. | `GET /consumer-spending/eu/v1/merchant-aggregates` |
| [Merchant Hierarchy](https://massive.com/docs/rest/alternative/consumer-spending/merchant-hierarchy.md) | Reference data mapping merchants to parent companies, tickers, sectors, and industries across Fable's European consumer transaction panel. Each row represents a single merchant with its corporate hierarchy and classification metadata. The latest weekly snapshot contains ~3,500 merchants covering ~250 US public companies. Use `lookup_name` to join with the `name` field from the [Merchant Aggregates](https://massive.com/docs/rest/alternative/consumer-spending/merchant-aggregates.md) endpoint. Filter by `active_from` and `active_to` to perform point-in-time joins against a specific `transaction_date` (e.g., `active_from.lte=2025-06-15&active_to.gte=2025-06-15`). An `active_from` of `0001-01-01` indicates no known start date, and an `active_to` of `9999-12-31` indicates the mapping is currently active. Use Cases: Merchant-to-ticker mapping, corporate hierarchy lookups, sector and industry classification, point-in-time historical hierarchy analysis. | `GET /consumer-spending/eu/v1/merchant-hierarchy` |

## Consumer Spending (EU)

European consumer transaction data powered by [Fable Data](https://www.fabledata.com/). Daily credit card, debit card, and open banking transactions across 6 European countries with Bloomberg ticker and industry mapping for ~250 US public companies. Open banking data achieves >85% fill rate at the 7-day mark.

[View Consumer Spending endpoints](https://massive.com/docs/rest/alternative/consumer-spending/merchant-aggregates.md)

## Normalizing Merchant Aggregates

Growth in the Consumer Spending dataset reflects new account acquisition from data partners, not population or spending growth in any given country. Comparing raw spend totals across time periods will be misleading because the panel of cardholders changes over time. To accurately measure spending trends, you must normalize by the number of active accounts in the period you're analyzing.

Rolling account counts are provided at 8-day and 28-day windows. Choose the window that best aligns with your analysis period. Use `spend_out_spend` for most analyses; use `total_spend` for industries with high refund rates (e.g., fast fashion).

**Country-level normalization** adjusts for overall panel growth in a country. Divide spend by total active accounts to get spend per panelist, then compare across periods.

Example — YoY growth for a UK merchant:

`(Q4 2024 UK spend / Q4 2024 active UK accounts) ÷ (Q4 2023 UK spend / Q4 2023 active UK accounts) − 1`

Use `eight_day_rolling_total_accounts` or `twenty_eight_day_rolling_total_accounts` as the account count.

**Category-level normalization** provides more precise results when market participation within a category is relatively stable over time (e.g., groceries on a monthly basis). Instead of total accounts, divide by accounts active in the relevant spending category.

Example — YoY growth normalized by category:

`(Q4 2024 UK spend / Q4 2024 active UK Category Y accounts) ÷ (Q4 2023 UK spend / Q4 2023 active UK Category Y accounts) − 1`

Use `eight_day_rolling_category_accounts` or `twenty_eight_day_rolling_category_accounts` as the account count.

**Cross-segment comparisons:** For cross-country or cross-consumer-type comparisons, calculate spend per panelist within each segment first, then combine using weighted averages based on relative panel sizes.
### Sign Convention

Spend values in consumer spending use bank statement convention from the consumer's perspective. Outbound transactions (`spend_out_spend`) are **negative** (money leaving the consumer's account), and inbound transactions (`spend_in_spend`, e.g. refunds) are **positive** (money returning to the consumer's account). `total_spend` is the sum of both and is typically negative. When calculating normalized metrics like spend per panelist, use the absolute value if you want a positive magnitude.

## Next Steps

Explore our REST API endpoints to access alternative datasets. Our detailed documentation will guide you through integrating this data into your applications for investment research, competitive analysis, and macro trend identification.
