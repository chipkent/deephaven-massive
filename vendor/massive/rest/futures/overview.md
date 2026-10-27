> For the full documentation index, see: https://massive.com/docs/llms.txt

# Futures Overview

At Massive, we provide a comprehensive suite of U.S. futures market data through standardized and intuitive APIs, offering deep insights into the dynamic futures landscape. Our offerings encompass real-time and historical data, contract details, product information, and trading schedules sourced from major U.S. futures exchanges including CME, CBOT, COMEX, and NYMEX. Accessible via REST APIs, and WebSocket streams, this extensive dataset supports the development of sophisticated applications for both retail and professional trading environments. Leveraging detailed trade, quote, and contract data alongside rich reference information, our platform delivers a robust analytical toolset crucial for informed trading decisions and comprehensive market analysis.

## Available Endpoints

Browse the endpoints available for Futures. Each row links to its dedicated documentation, with a short description and the API path.

| Endpoints | Description | Path |
| --- | --- | --- |
| [Contracts](https://massive.com/docs/rest/futures/contracts.md) | The Contracts API provides a single source for discovering all listed futures contracts and retrieving complete contract specifications. You can query the full contract index with filters for product code, trade dates, active status, and date, returning key attributes such as ticker, first and last trade dates, days to maturity, exchange code, and order quantity limits in paginated form. The same API also returns the full specification for a single contract, including settlement dates, tick sizes, and other trading and risk related fields. Point-in-time lookups allow you to reconstruct the exact contract definition that applied on any given day. Use Cases: Historical research, trading system integration, portfolio workflows, risk management. | `GET /futures/v1/contracts` |
| [Products](https://massive.com/docs/rest/futures/products.md) | The Products API is a unified source for discovering all supported futures products and retrieving full product specifications. It returns the complete product universe with product codes, names, exchange identifiers, sector and asset class classifications, product type, settlement method, and pricing and quotation details. You can filter by name, exchange, sector, asset class, product type, or date to capture the product set or product definition that existed at a specific point in time. It also retrieves the full specification for a single product, supporting accurate system configuration, analytics, trading workflows, and historical reconciliation. Use Cases: Product specification, historical product checks, risk management, trading system integration. | `GET /futures/v1/products` |
| [Schedules](https://massive.com/docs/rest/futures/schedules.md) | The Schedules API provides a unified way to retrieve trading schedules for futures markets, returning precise session open and close times, intraday breaks, and any adjustments for holidays or special events. You can filter schedules by session_end_date or retrieve the schedule for a single product using its product code. All times are returned in Coordinated Universal Time (UTC), making it straightforward to align trading, execution, and operational workflows across systems. This endpoint currently supports 2 years of history. Use Cases: Schedule planning, market analysis, strategy alignment, risk and operations management. | `GET /futures/v1/schedules` |
| [Aggregate Bars (OHLC)](https://massive.com/docs/rest/futures/aggregates.md) | Retrieve historical OHLC (open, high, low, close) and volume bars for a futures contract over a date range. Set the bar size with the resolution parameter, from seconds up to a full session, week, month, quarter, or year. Bars are built from the trades in each interval, so an interval with no trades has no bar. A futures session opens the evening before the date it settles on, so a session bar's window_start is the day before its session_end_date. To load the session that settles on a given date, query window_start for the day before. Use Cases: Market monitoring, technical analysis, backtesting, trading strategy development. | `GET /futures/v1/aggs/{ticker}` |
| **Snapshots** | | |
| [Futures Contracts Snapshot](https://massive.com/docs/rest/futures/snapshots/contracts-snapshot.md) | Retrieve real-time snapshots for a set of futures contracts, including key market data such as the latest trade, quote, session metrics (open, high, low, close, volume), and settlement prices. This endpoint returns the most up-to-date view of contract activity, filtered by ticker or product code, and supports custom sorting and pagination for efficient data access. Use Cases: Real-time trading systems, intraday market analysis, performance monitoring, and portfolio valuation. | `GET /futures/v1/snapshot` |
| **Trades & Quotes** | | |
| [Trades](https://massive.com/docs/rest/futures/trades-quotes/trades.md) | Retrieve comprehensive, tick-level trade data for a specified futures contract ticker over a defined time range. Each record includes the trade price, size, session end date, and precise timestamps, capturing individual trade events throughout the period. This granular data is essential for constructing aggregated bars and performing detailed analyses of intraday price movements, making it a valuable tool for backtesting, algorithmic strategy development, and market research. Use Cases: Intraday analysis, algorithmic trading, backtesting, market research. | `GET /futures/v1/trades/{ticker}` |
| [Quotes](https://massive.com/docs/rest/futures/trades-quotes/quotes.md) | Retrieve quote data for a specified futures contract ticker. Each record includes the best bid and offer prices, sizes, and timestamps, reflecting the prevailing quote environment at each moment. This endpoint supports detailed analysis of price dynamics and liquidity conditions to inform trading decisions and market research. Use Cases: Liquidity analysis, price discovery, trading strategy refinement, market research. | `GET /futures/v1/quotes/{ticker}` |
| **Market Operations** | | |
| [Market Status](https://massive.com/docs/rest/futures/market-operations/market-status.md) | Retrieve the current market status for a specific product or products. This endpoint returns real-time indicators, such as open, pause, close, for futures products, along with the corresponding exchange and product codes and an evaluation timestamp. This information enables users to monitor operational conditions and adjust their trading strategies accordingly. Use Cases: Real-time monitoring, algorithm scheduling, UI updates, operational planning. | `GET /futures/v1/market-status` |
| [Exchanges](https://massive.com/docs/rest/futures/market-operations/exchanges.md) | Retrieve a list of supported futures exchanges, including their unique exchange codes, names, and other important details. Use Cases: Exchange reference, market analysis, and compliance checks. | `GET /futures/v1/exchanges` |

## Market Hours and Timezone

Timestamps in our futures API responses are returned in Coordinated Universal Time (UTC). Trade, quote, and aggregate (`window_start`) timestamps are Unix timestamps expressed in nanoseconds since the epoch, while the [schedules](https://massive.com/docs/rest/futures/schedules.md) and [market status](https://massive.com/docs/rest/futures/market-operations/market-status.md) endpoints return ISO 8601 timestamps in UTC (for example, `2025-03-16T22:00:00+00:00`). The `session_end_date` field on trade, quote, and aggregate records reflects the exchange trading session date, following the convention that a session ending at 5:00 p.m. CT carries that day's date.

The trading hours below are shown in Central Time (CT), the timezone the major U.S. futures exchanges use to publish their schedules. For specific trading hours and daily maintenance periods for each product, please refer to our [schedules endpoint](https://massive.com/docs/rest/futures/schedules.md) for detailed information.

### CME and CBOT Products
- **Trading Hours:**
  - **Start:** Sunday 5:00 p.m. CT
  - **End:** Friday 5:45 p.m. CT
- **Daily Maintenance Period:** Varies per product (refer to the [schedules endpoint](https://massive.com/docs/rest/futures/schedules.md) for details)

### COMEX and NYMEX Products
- **Trading Hours:**
  - **Start:** Sunday 5:00 p.m. CT
  - **End:** Friday 4:00 p.m. CT
- **Daily Maintenance Period:** Varies per product (refer to the [schedules endpoint](https://massive.com/docs/rest/futures/schedules.md) for details)

### Cryptocurrency Futures (CME Group)

Starting at 4:00 p.m. CT on Friday, May 29, 2026, CME Group cryptocurrency futures will trade continuously (nearly 24 hours a day, 7 days a week). These products will include a short weekly maintenance window (typically at least two hours over the weekend). Weekend and holiday trading sessions from Friday evening through Sunday evening will carry the trade date of the following business day. See also [CME Group's official 24/7 Cryptocurrency Trading page](https://www.cmegroup.com/markets/cryptocurrencies/24-7-crypto-trading.html).

For the most up to date product specific schedules, maintenance periods, and any other details, always refer to our [schedules endpoint](https://massive.com/docs/rest/futures/schedules.md).

## Infrastructure and Reliability

Our platform's robustness begins with our primary data facility at the Equinix Data Center in New Jersey, strategically co-located with the exchanges. This setup allows us to receive data through direct physical connections, significantly reducing latency and enhancing the reliability of the data. Initially established in New Jersey, our infrastructure has expanded to include full redundancy at the ORD11 data center in Chicago. This expansion ensures uninterrupted service and data availability, even under adverse conditions, across multiple geographic locations.

We maintain direct connections to major futures exchanges, such as CME, CBOT, COMEX, and NYMEX, to ensure minimal latency and reliable data transmission. Data is sourced directly from proprietary exchange feeds and processed through high-performance systems for near real-time distribution. This robust infrastructure supports a wide range of financial applications, from algorithmic trading to in-depth market research.

## Data Flow: From Exchanges to You

Futures data originates directly from the major exchanges where contracts and products are defined and traded. We establish and maintain direct relationships with these exchanges while adhering to licensing requirements to capture every trade, quote, and market event as it occurs.

![The flow of U.S. futures market data from major exchanges (e.g., CME, CBOT, COMEX, NYMEX) through direct connections to Massive's co-located data facilities.](https://massive.com/docs/img/graphic-futures-dataflow-light.svg)

The raw data is then normalized and enriched through our advanced processing systems, ensuring a complete and accurate view of the U.S. futures market. This structured data flow is delivered to you via our REST APIs, and WebSocket streams, empowering you to make informed decisions with confidence.

Our comprehensive dataset includes data from key U.S. futures exchanges:
- CME
- CBOT
- COMEX
- NYMEX

## Next Steps

Explore our REST and WebSocket API endpoints to fully harness the capabilities of our futures market data. Our detailed documentation will guide you through integrating both real-time and historical data into your applications, equipping you with the insights needed to drive sophisticated trading strategies and market analysis.
