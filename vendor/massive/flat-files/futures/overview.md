> For the full documentation index, see: https://massive.com/docs/llms.txt

# Futures Overview

Massive offers extensive historical U.S. futures market data through convenient CSV Flat Files. Our structured datasets encompass detailed historical trade data, quotes, aggregated market data, contract details, and product information sourced directly from major U.S. futures exchanges, including CME, CBOT, COMEX, and NYMEX. Leveraging Flat Files simplifies access to substantial historical datasets, supporting sophisticated analysis, research, backtesting strategies, and regulatory compliance.

## Available Datasets

Browse the datasets available for Futures Flat Files. Each row links to its dedicated documentation, with a short description and the dataset path.

| Datasets | Description | Path |
| --- | --- | --- |
| **Session Aggregates** | | |
| [CBOT](https://massive.com/docs/flat-files/futures/session-aggregates/cbot.md) | Candlestick data for CBOT futures with open, high, low, close, and volume aggregated from all trades during each trading session. | `S3 us_futures_cbot/session_aggs_v1` |
| [CME](https://massive.com/docs/flat-files/futures/session-aggregates/cme.md) | Candlestick data for CME futures with open, high, low, close, and volume aggregated from all trades during each trading session. | `S3 us_futures_cme/session_aggs_v1` |
| [NYMEX](https://massive.com/docs/flat-files/futures/session-aggregates/nymex.md) | Candlestick data for NYMEX futures with open, high, low, close, and volume aggregated from all trades during each trading session. | `S3 us_futures_nymex/session_aggs_v1` |
| [COMEX](https://massive.com/docs/flat-files/futures/session-aggregates/comex.md) | Candlestick data for COMEX futures with open, high, low, close, and volume aggregated from all trades during each trading session. | `S3 us_futures_comex/session_aggs_v1` |
| **Minute Aggregates** | | |
| [CBOT](https://massive.com/docs/flat-files/futures/minute-aggregates/cbot.md) | Candlestick data for CBOT futures with minute-by-minute open, high, low, close, and volume aggregated from all trades. | `S3 us_futures_cbot/minute_aggs_v1` |
| [CME](https://massive.com/docs/flat-files/futures/minute-aggregates/cme.md) | Candlestick data for CME futures with minute-by-minute open, high, low, close, and volume aggregated from all trades. | `S3 us_futures_cme/minute_aggs_v1` |
| [NYMEX](https://massive.com/docs/flat-files/futures/minute-aggregates/nymex.md) | Candlestick data for NYMEX futures with minute-by-minute open, high, low, close, and volume aggregated from all trades. | `S3 us_futures_nymex/minute_aggs_v1` |
| [COMEX](https://massive.com/docs/flat-files/futures/minute-aggregates/comex.md) | Candlestick data for COMEX futures with minute-by-minute open, high, low, close, and volume aggregated from all trades. | `S3 us_futures_comex/minute_aggs_v1` |
| **Trades** | | |
| [CBOT](https://massive.com/docs/flat-files/futures/trades/cbot.md) | Tick-level trades with nanosecond timestamps for CBOT futures, delivered as a daily downloadable S3 file. | `S3 us_futures_cbot/trades_v1` |
| [CME](https://massive.com/docs/flat-files/futures/trades/cme.md) | Tick-level trades with nanosecond timestamps for CME futures, delivered as a daily downloadable S3 file. | `S3 us_futures_cme/trades_v1` |
| [NYMEX](https://massive.com/docs/flat-files/futures/trades/nymex.md) | Tick-level trades with nanosecond timestamps for NYMEX futures, delivered as a daily downloadable S3 file. | `S3 us_futures_nymex/trades_v1` |
| [COMEX](https://massive.com/docs/flat-files/futures/trades/comex.md) | Tick-level trades with nanosecond timestamps for COMEX futures, delivered as a daily downloadable S3 file. | `S3 us_futures_comex/trades_v1` |
| **Quotes** | | |
| [CBOT](https://massive.com/docs/flat-files/futures/quotes/cbot.md) | Top-of-book quotes with nanosecond timestamps for CBOT futures, delivered as a daily downloadable S3 file. | `S3 us_futures_cbot/quotes_v1` |
| [CME](https://massive.com/docs/flat-files/futures/quotes/cme.md) | Top-of-book quotes with nanosecond timestamps for CME futures, delivered as a daily downloadable S3 file. | `S3 us_futures_cme/quotes_v1` |
| [NYMEX](https://massive.com/docs/flat-files/futures/quotes/nymex.md) | Top-of-book quotes with nanosecond timestamps for NYMEX futures, delivered as a daily downloadable S3 file. | `S3 us_futures_nymex/quotes_v1` |
| [COMEX](https://massive.com/docs/flat-files/futures/quotes/comex.md) | Top-of-book quotes with nanosecond timestamps for COMEX futures, delivered as a daily downloadable S3 file. | `S3 us_futures_comex/quotes_v1` |

## Data Availability and Frequency
Futures Flat Files are updated daily, capturing comprehensive trading activity from major U.S. futures exchanges. Data is standardized in Central Time (CT), aligning seamlessly with major exchange operations, trading schedules, and market events.

Specific product trading hours and daily maintenance periods vary by exchange:

- **CME and CBOT Products:**
  - Sunday 5:00 p.m. CT to Friday 5:45 p.m. CT (with daily maintenance periods)

- **COMEX and NYMEX Products:**
  - Sunday 5:00 p.m. CT to Friday 4:00 p.m. CT (with daily maintenance periods)

For precise trading schedules, refer to the provided documentation or exchange-specific information. The finalized daily datasets become available for download by approximately 11:00 AM ET the following day.

## Reliable and Comprehensive Historical Data
Our Futures Flat Files datasets are sourced directly from primary exchange feeds, ensuring data accuracy and integrity. Exchanges include:

- CME (Chicago Mercantile Exchange)
- CBOT (Chicago Board of Trade)
- COMEX (Commodity Exchange)
- NYMEX (New York Mercantile Exchange)

By leveraging direct exchange connectivity, we deliver robust and comprehensive historical market records suitable for detailed analytics, strategy development, and market research.

## Simplifying Historical Data Access
Flat Files significantly streamline accessing and managing historical futures data, eliminating the need for numerous individual API requests. This enables efficient:

- Historical performance analysis
- Backtesting trading strategies
- Risk and portfolio management
- Regulatory reporting and compliance
- Economic and financial research

## Data Structure and Formats
Futures Flat Files are provided in CSV format, featuring standardized headers for easy data integration and analysis. Each dataset includes:

- Trades: Transaction-level data including price, volume, timestamps, and trade conditions
- Quotes: Bid and ask pricing with size and depth
- Aggregates: Minute, hourly, and daily open, high, low, close, and volume (OHLCV) data

This structured approach supports rapid ingestion, analytics, and integration into both manual and automated analytical workflows.

## Infrastructure and Reliability
Massive maintains robust, high-performance infrastructure located at strategically co-located data centers, such as Equinix in New Jersey and ORD11 in Chicago. These secure and direct connections to major exchanges ensure minimal latency, comprehensive data coverage, and reliable, consistent data delivery, supporting high-quality analytics and decision-making processes.

## Regulatory Compliance
Massive strictly adheres to regulatory and exchange licensing requirements, providing fully compliant historical futures market data. This ensures data integrity and usability for both professional and retail users across diverse analytical and trading environments.

## Next Steps
Start exploring Futures Flat Files using our integrated File Browser or configure your preferred S3-compatible client for automated and efficient downloads. Utilize our comprehensive documentation to seamlessly integrate extensive historical futures data into your market analysis tools, trading systems, and research processes.
