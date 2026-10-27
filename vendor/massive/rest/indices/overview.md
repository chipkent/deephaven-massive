> For the full documentation index, see: https://massive.com/docs/llms.txt

# Indices Overview

At Massive, we provide a comprehensive suite of market data for over 10,000+ indices from multiple index families, including S&P, Nasdaq, Dow Jones, and more. Our standardized and intuitive APIs offer real-time prices, historical data, and reference information. Accessible via REST APIs, WebSocket streams, and flat files, this extensive dataset supports the development of sophisticated applications for both retail and professional users. By delivering detailed index data and analytics, our platform equips you with the tools necessary for in-depth market analysis and informed decision-making.

## Available Endpoints

Browse the endpoints available for Indices. Each row links to its dedicated documentation, with a short description and the API path.

| Endpoints | Description | Path |
| --- | --- | --- |
| **Tickers** | | |
| [All Tickers](https://massive.com/docs/rest/indices/tickers/all-tickers.md) | Retrieve a comprehensive list of ticker symbols supported by Massive across various asset classes (e.g., stocks, indices, forex, crypto). Each ticker entry provides essential details such as symbol, name, market, currency, and active status. Use Cases: Asset discovery, data integration, filtering/selection, and application development. | `GET /v3/reference/tickers` |
| [Ticker Overview](https://massive.com/docs/rest/indices/tickers/ticker-overview.md) | Retrieve comprehensive details for a single ticker supported by Massive that is active as-of a given date. This endpoint offers a deep look into a company’s fundamental attributes, including its primary exchange, standardized identifiers (CIK, composite FIGI, share class FIGI), market capitalization, industry classification, and key dates. Users also gain access to branding assets (e.g., logos, icons), enabling them to enrich applications and analyses with visually consistent, contextually relevant information. For information on delisted tickers, use the [All Tickers](https://massive.com/docs/rest/indices/tickers/all-tickers.md) endpoint with `active=false`. Use Cases: Company research, data integration, application enhancement, due diligence & compliance. | `GET /v3/reference/tickers/{ticker}` |
| **Aggregate Bars (OHLC)** | | |
| [Custom Bars](https://massive.com/docs/rest/indices/aggregates/custom-bars.md) | Retrieve aggregated historical OHLC (Open, High, Low, Close) and value data for a specified index over a custom date range and time interval in Eastern Time (ET). Unlike stocks or options, these aggregates are derived from index values rather than individual trades, reflecting the performance of a market segment, sector, or benchmark. If no index updates occur within a given timeframe, no aggregate bar is produced, resulting in an empty interval that indicates a period without new index data. Users can customize their view by adjusting the multiplier and timespan parameters (e.g., a 5-minute interval). This approach supports various analytical and visualization needs related to broad market or sector performance. Use Cases: Data visualization, market trend analysis, benchmark comparisons, research and modeling. | `GET /v2/aggs/ticker/{indicesTicker}/range/{multiplier}/{timespan}/{from}/{to}` |
| [Previous Day Bar](https://massive.com/docs/rest/indices/aggregates/previous-day-bar.md) | Retrieve the previous trading day's open, high, low, and close (OHLC) data for a specified index ticker. This endpoint provides key pricing metrics, including volume, to help users assess recent performance and inform trading strategies. Use Cases: Baseline comparison, technical analysis, market research, and daily reporting. | `GET /v2/aggs/ticker/{indicesTicker}/prev` |
| [Daily Ticker Summary](https://massive.com/docs/rest/indices/aggregates/daily-ticker-summary.md) | Retrieve the opening and closing prices for a specific index on a given date, along with any pre-market and after-hours trade prices. This endpoint provides essential daily pricing details, enabling users to evaluate performance, conduct historical analysis, and gain insights into trading activity outside regular market sessions. Use Cases: Daily performance analysis, historical data collection, after-hours insights, portfolio tracking. | `GET /v1/open-close/{indicesTicker}/{date}` |
| **Snapshots** | | |
| [Indices Snapshot](https://massive.com/docs/rest/indices/snapshots/indices-snapshot.md) | Retrieve snapshot data for one or more indices, including their current value, recent performance metrics, and trading session details. By consolidating key information for each specified index, this endpoint helps users assess market conditions, track broad economic sentiment, and integrate index-level data into trading or analysis workflows. Use Cases: Market condition assessment, economic sentiment tracking, portfolio context, and integrated index data analysis. | `GET /v3/snapshot/indices` |
| [Unified Snapshot](https://massive.com/docs/rest/indices/snapshots/unified-snapshot.md) | Retrieve unified snapshots of market data for multiple asset classes including stocks, options, forex, and cryptocurrencies in a single request. This endpoint consolidates key metrics such as last trade, last quote, open, high, low, close, and volume for a comprehensive view of current market conditions. By aggregating data from various sources into one response, users can efficiently monitor, compare, and act on information spanning multiple markets and asset types. Use Cases: Cross-market analysis, diversified portfolio monitoring, global market insights, multi-asset trading strategies. | `GET /v3/snapshot` |
| **Technical Indicators** | | |
| [SMA](https://massive.com/docs/rest/indices/technical-indicators/simple-moving-average.md) | Retrieve the Simple Moving Average (SMA) for a specified ticker over a defined time range. The SMA calculates the average price across a set number of periods, smoothing price fluctuations to reveal underlying trends and potential signals. Use Cases: Trend analysis, trading signal generation (e.g., SMA crossovers), identifying support/resistance, and refining entry/exit timing. | `GET /v1/indicators/sma/{indicesTicker}` |
| [EMA](https://massive.com/docs/rest/indices/technical-indicators/exponential-moving-average.md) | Retrieve the Exponential Moving Average (EMA) for a specified ticker over a defined time range. The EMA places greater weight on recent prices, enabling quicker trend detection and more responsive signals. Use Cases: Trend identification, EMA crossover signals, dynamic support/resistance levels, and adjusting strategies based on recent market volatility. | `GET /v1/indicators/ema/{indicesTicker}` |
| [MACD](https://massive.com/docs/rest/indices/technical-indicators/moving-average-convergence-divergence.md) | Retrieve the Moving Average Convergence/Divergence (MACD) for a specified ticker over a defined time range. MACD is a momentum indicator derived from two moving averages, helping to identify trend strength, direction, and potential trading signals. Use Cases: Momentum analysis, signal generation (crossover events), spotting overbought/oversold conditions, and confirming trend directions. | `GET /v1/indicators/macd/{indicesTicker}` |
| [RSI](https://massive.com/docs/rest/indices/technical-indicators/relative-strength-index.md) | Retrieve the Relative Strength Index (RSI) for a specified ticker over a defined time range. The RSI measures the speed and magnitude of price changes, oscillating between 0 and 100 to help identify overbought or oversold conditions. Use Cases: Overbought/oversold detection, divergence analysis, trend confirmation, and refining market entry/exit strategies. | `GET /v1/indicators/rsi/{indicesTicker}` |
| **Market Operations** | | |
| [Market Holidays](https://massive.com/docs/rest/indices/market-operations/market-holidays.md) | Retrieve upcoming market holidays and their corresponding open/close times. This endpoint is forward-looking only, listing future holidays that affect market hours. Use this data to plan ahead for trading activities and system operations. Use Cases: Trading schedule adjustments, integrated holiday calendars, operational planning (e.g., system maintenance), and notifying users about upcoming market closures. | `GET /v1/marketstatus/upcoming` |
| [Market Status](https://massive.com/docs/rest/indices/market-operations/market-status.md) | Retrieve the current trading status for various exchanges and overall financial markets. This endpoint provides real-time indicators of whether markets are open, closed, or operating in pre-market/after-hours sessions, along with timing details for the current or upcoming trading periods. Use Cases: Real-time monitoring, algorithm scheduling, UI updates, and operational planning. | `GET /v1/marketstatus/now` |

## Market Hours and Timezone

Indices generally update in alignment with the trading hours of their underlying assets. While the exact update times depend on the index methodology and the exchange that calculates it, most U.S. indices reflect regular market hours:

* Regular Market Hours: Monday through Friday, 9:30 AM to 4:00 PM Eastern Time (ET)

Some indices may also update during pre-market and after-hours sessions, depending on the policies of the exchanges and the nature of the underlying securities. All timestamps in the datasets are provided as Unix timestamps (seconds since epoch, UTC). When converting these timestamps into human-readable form (e.g., market open at 9:30 AM), remember they represent UTC time, not Eastern Time (ET). To correctly align data with market hours or dates, you'll need to explicitly convert timestamps from UTC to ET during your analysis.

## Infrastructure and Reliability

Our platform sources index data directly from major exchanges, including those operated by Cboe, Nasdaq, and CME. By connecting directly to these exchanges, we minimize latency and enhance data integrity, ensuring that you receive timely and accurate index values as they are calculated and broadcasted.

Indices may not update as frequently as individual securities, since they are calculated based on the performance of multiple underlying assets. Our infrastructure is designed to accommodate these varying update frequencies, efficiently capturing and distributing index data, whether it updates multiple times per second or once per day. This robust setup ensures that you always have access to reliable, up-to-date market information.

## Data Flow: From Exchanges to You

Index data begins at the exchanges, where the index values are calculated and disseminated based on established methodologies. Companies like S&P or Dow Jones determine the methodology and constituent lists, but the actual index values are often calculated and broadcasted by the exchanges.

We maintain direct connections with these exchanges to capture index values at their source. Our systems then standardize, store, and distribute this data through our APIs, ensuring consistent formats and easy integration into your applications.

**Indices Groups Covered Include (but are not limited to):**
* Standard & Poor's (S&P)
* Societe Generale
* Morgan Stanley Capital International (MSCI)
* FTSE Russell
* Morningstar
* Cboe Streaming Market Indices Cryptocurrency (CCCY)
* Cboe Global Indices (CGI)
* Nasdaq
* Dow Jones

By aggregating data from these diverse sources, we deliver a unified view of the global market landscape, enabling comprehensive market analysis and strategic decision-making.

## Regulatory Compliance

We adhere to all regulatory requirements for distributing index market data. This includes complying with licensing agreements and data usage policies established by the exchanges and index providers. Our data services are designed to meet the needs of both non-professional and professional users, ensuring that you have access to accurate and timely information while maintaining compliance with relevant regulations.

For business clients, we offer tailored plans that accommodate specific licensing requirements, providing the flexibility needed to serve a wide range of professional trading environments.

## Next Steps

Explore our REST APIs and WebSocket streams to unlock the full potential of our indices market data. Our detailed documentation guides you through integrating real-time and historical information into your applications, enabling tasks such as:

* Retrieving real-time index values
* Accessing historical aggregate data
* Monitoring changes in index levels over time
* Integrating index data into trading algorithms and analytical tools

By leveraging our comprehensive indices dataset, you can develop sophisticated trading strategies, perform market analysis, and build applications that require timely and accurate index market data.
