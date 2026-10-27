> For the full documentation index, see: https://massive.com/docs/llms.txt

# Economy Overview

Our Economy API provides access to key macroeconomic indicators sourced from the Federal Reserve and other official government sources. This data is essential for understanding the broader economic environment that influences financial markets, from interest rate movements to inflation dynamics.

## Available Endpoints

Browse the endpoints available for Economy. Each row links to its dedicated documentation, with a short description and the API path.

| Endpoints | Description | Path |
| --- | --- | --- |
| [Treasury Yields](https://massive.com/docs/rest/economy/treasury-yields.md) | Retrieve historical U.S. Treasury yield data for standard timeframes ranging from 1-month to 30-years, with daily historical records back to 1962. This endpoint lets you query by date or date range to see how interest rates have changed over time. Each data point reflects the market yield for Treasury securities of a specific maturity, helping users understand short- and long-term rate movements. Use Cases: Charting rate trends, comparing short vs. long-term yields, economic research. | `GET /fed/v1/treasury-yields` |
| [Inflation](https://massive.com/docs/rest/economy/inflation.md) | Retrieve key indicators of realized inflation, reflecting actual changes in consumer prices and spending behavior in the U.S. economy. This endpoint provides both headline and core inflation measures from the CPI and PCE indexes, offering a well-rounded view of historical price trends. Use Cases: Tracking inflation trends, evaluating purchasing power, supporting monetary policy and economic research. | `GET /fed/v1/inflation` |
| [Inflation Expectations](https://massive.com/docs/rest/economy/inflation-expectations.md) | Retrieve a broad view of how inflation is expected to evolve over time in the U.S. economy. This endpoint combines signals from financial markets and economic models to capture both near-term and long-term inflation outlooks. Each data point helps contextualize how inflation risk is perceived by investors and forecasters across different time horizons. Use Cases: Analyzing inflation sentiment, comparing near-term vs. long-term expectations, supporting macroeconomic research and fixed-income strategy. | `GET /fed/v1/inflation-expectations` |
| [Labor Market](https://massive.com/docs/rest/economy/labor-market.md) | Retrieve key labor market indicators from the Federal Reserve, including unemployment rate, labor force participation, average hourly earnings, and job openings data. Use Cases: Analyzing employment trends, monitoring workforce dynamics, supporting macroeconomic research and labor market analysis. | `GET /fed/v1/labor-market` |
| [Funding Conditions](https://massive.com/docs/rest/economy/funding-conditions.md) | Daily U.S. overnight funding snapshot — one row per day showing how cheap or tight it is for banks and dealers to borrow cash overnight. It pulls together the Fed's policy rates (effective fed funds rate, IORB, and the target range), SOFR—the recommended alternative to USD LIBOR—and OBFR, a measure of wholesale, unsecured overnight bank funding costs. It also includes trading volume and rate-distribution detail for SOFR and OBFR, the size of the NY Fed's overnight repo and reverse repo operations, and short-term borrowing costs for top-rated commercial paper (90-day, financial and nonfinancial). Use Cases: Monitoring short-term funding stresses, tracking the transmission of monetary policy across secured and unsecured markets, analyzing repo and reverse-repo flows, and supporting macroeconomic and rates research. | `GET /fed/v1/funding-conditions` |

## Data Sources

Our economy data is sourced directly from official government and Federal Reserve publications, ensuring accuracy and reliability. Data is updated as new releases become available from these authoritative sources, with historical records spanning decades for long-term analysis.

## Use Cases

- **Fixed Income Analysis**: Analyze interest rate data to inform bond trading strategies and portfolio duration decisions.
- **Economic Research**: Study historical relationships between macroeconomic indicators and market behavior.
- **Risk Management**: Monitor economic indicators to assess real return projections and macro risk exposures.
- **Monetary Policy Analysis**: Track how Federal Reserve policy decisions impact markets and economic conditions.
- **Valuation Models**: Incorporate macroeconomic assumptions into discounted cash flow and asset pricing models.

## Next Steps

Explore our Economy API endpoints to integrate macroeconomic data into your applications and analyses. Each endpoint provides detailed documentation on available parameters, response formats, and example code in multiple programming languages.
