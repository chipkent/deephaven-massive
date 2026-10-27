> For the full documentation index, see: https://massive.com/docs/llms.txt

# Stocks Overview

Massive offers comprehensive historical U.S. stock market data through convenient, compressed CSV Flat Files. These datasets include daily and historical trade, quote, and aggregated market data sourced directly from all major U.S. exchanges, FINRA trading facilities, and dark pools. By leveraging our Flat Files, you can efficiently download large historical datasets for robust market analysis, research, backtesting strategies, and more, without the need for extensive API requests.

All stock Flat Files contain unadjusted data. Prices and volumes are not adjusted for stock splits, dividends, or other corporate actions. If you require adjusted data for backtesting or analysis, you can retrieve it via the [REST API](https://massive.com/docs/rest/quickstart.md) by setting the `adjusted` flag to `true`, or apply adjustments manually using data from the [Splits endpoint](https://massive.com/docs/rest/stocks/corporate-actions/splits.md).

## Available Datasets

Browse the datasets available for Stocks Flat Files. Each row links to its dedicated documentation, with a short description and the dataset path.

| Datasets | Description | Path |
| --- | --- | --- |
| [Day Aggregates](https://massive.com/docs/flat-files/stocks/day-aggregates.md) | Candlesticks with open, high, low, close, and volume at per-day granularity across all U.S. equities, made available as a daily downloadable S3 file. | `S3 us_stocks_sip/day_aggs_v1` |
| [Minute Aggregates](https://massive.com/docs/flat-files/stocks/minute-aggregates.md) | Candlesticks with open, high, low, close, and volume at per minute granularity across all U.S. equities, made available as a daily downloadable S3 file. | `S3 us_stocks_sip/minute_aggs_v1` |
| [Trades](https://massive.com/docs/flat-files/stocks/trades.md) | Tick level nanosecond timestamped trade data from all major U.S. exchanges and darkpools including NYSE, Nasdaq, Cboe, Finra, and more, made available as a daily downloadable S3 file. | `S3 us_stocks_sip/trades_v1` |
| [Quotes](https://massive.com/docs/flat-files/stocks/quotes.md) | Top of book quotes with nanosecond timestamps from all major U.S. exchanges and darkpools, including NYSE, Nasdaq, Cboe, Finra, and more, made available as a daily downloadable S3 file. | `S3 us_stocks_sip/quotes_v1` |

## Data Availability and Frequency
Stocks Flat Files are generated daily after the close of the full market session, capturing all trading activity from:

- Pre-Market Trading: 4:00 AM to 9:30 AM Eastern Time (ET)
- Regular Market Hours: 9:30 AM to 4:00 PM ET
- After-Hours Trading: 4:00 PM to 8:00 PM ET

The finalized daily datasets become available for download by approximately 11:00 AM ET the following day. All timestamps in the datasets are provided as Unix timestamps (seconds since epoch, UTC). When converting these timestamps into human-readable form (e.g., market open at 9:30 AM), remember they represent UTC time, not Eastern Time (ET). To correctly align data with market hours or dates, you'll need to explicitly convert timestamps from UTC to ET during your analysis.

## Reliable and Comprehensive Historical Data
Our Flat Files include comprehensive market coverage, ensuring you receive an accurate historical record from the entire U.S. stock market ecosystem. Data sources include:

- All 19 major U.S. stock exchanges, including NYSE, Nasdaq, Cboe, MIAX, MEMX, IEX, LTSE, and others.
- FINRA trading facilities such as NYSE TRF, Nasdaq TRF Carteret, and Nasdaq TRF Chicago, which capture off-exchange trade reporting.
- Dark pools for comprehensive trade visibility.

Massive consolidates and processes data sourced directly from regulated Securities Information Processors (SIPs), ensuring the highest standards of data integrity and compliance with U.S. market regulations. SIPs aggregate and standardize data across multiple exchanges, providing official trade records and the National Best Bid and Offer (NBBO).

## How Flat Files Streamline Your Workflow
Utilizing Flat Files simplifies accessing and handling extensive historical datasets. Instead of managing numerous individual API requests, you can download structured historical datasets quickly and conveniently. This simplifies data management, accelerates your analysis, and reduces integration complexity, particularly beneficial for:

- Historical market analysis and financial research
- Strategy backtesting and quantitative modeling
- Data-driven application development
- Compliance and audit reporting

## Data Structure and Formats
Stocks Flat Files are provided in CSV format with headers across all files, ensuring straightforward integration. The datasets cover key market metrics including:

- Trades: Individual trade executions with price, volume, and timestamps
- Quotes: Bid and ask prices, sizes, and exchange
- Aggregates: Minute and daily Aggregates, with open, high, low, close, and volume data

These structured datasets enable rapid ingestion and analysis, supporting both manual exploration and automated workflows.

## Regulatory Compliance
Massive ensures compliance with U.S. stock market data regulations. Historical Flat Files are available to both individual retail users and institutional clients under licensing terms tailored to specific needs. We closely adhere to exchange licensing and regulatory requirements, providing users clarity and confidence in utilizing our data.

## Next Steps
Explore our Stocks Flat Files through the integrated File Browser or configure your preferred S3-compatible client for automated downloads. Our comprehensive documentation provides detailed guidance to help you integrate extensive historical market data seamlessly into your analysis and workflows.
