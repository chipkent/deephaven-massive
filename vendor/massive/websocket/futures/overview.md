> For the full documentation index, see: https://massive.com/docs/llms.txt

# Futures Overview

At Massive, we provide real-time streaming access to U.S. futures market data through WebSockets, delivering a continuous flow of updates directly to your applications. Our WebSocket feeds include aggregates (per minute and per second), trades, and quotes, enabling traders, analysts, and developers to receive live, push-based data with minimal latency. By leveraging these streaming endpoints, you can power dynamic dashboards, feed algorithmic trading strategies, and monitor market conditions as they evolve without the overhead of repeated polling.

## Available Feeds

| Feeds | Description | Path |
| --- | --- | --- |
| [Aggregates (Per Minute)](https://massive.com/docs/websocket/futures/aggregates-per-minute.md) | Stream minute-by-minute aggregated OHLC (Open, High, Low, Close) and volume data for a specified futures contract ticker via WebSocket. Aggregates are continuously updated in Central Time (CT) and are constructed from all trades occurring within each defined aggregation window. If no trades occur during the window, no aggregate bar is emitted, indicating a period of inactivity. The response includes key metrics such as open, high, low, close, trade volume, and the start and end timestamps for each window. Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, automated trading. | `WS /futures/AM` |
| [Aggregates (Per Second)](https://massive.com/docs/websocket/futures/aggregates-per-second.md) | Stream second-by-second aggregated OHLC (Open, High, Low, Close) and volume data for a specified futures contract ticker via WebSocket. Aggregates are continuously updated in Central Time (CT) and are constructed from all trades occurring within each defined aggregation window. If no trades occur during the window, no aggregate bar is emitted, indicating a period of inactivity. The response includes key metrics such as open, high, low, close, trade volume, and the start and end timestamps for each window. Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, automated trading. | `WS /futures/A` |
| [Trades](https://massive.com/docs/websocket/futures/trades.md) | Stream tick-level trade data for futures tickers via WebSocket. Each message delivers key trade details (price, size, exchange, conditions, and timestamps) as they occur, enabling users to track market activity, power live dashboards, and inform rapid decision-making. Use Cases: Live monitoring, algorithmic trading, market analysis, data visualization. | `WS /futures/T` |
| [Quotes](https://massive.com/docs/websocket/futures/quotes.md) | Stream BBO (Best Bid and Offer) quote data for futures tickers via WebSocket. Each message provides the current best bid/ask prices, sizes, and related metadata as they update, allowing users to monitor evolving market conditions, inform trading decisions, and maintain responsive, data-driven applications. Use Cases: Live monitoring, market analysis, trading decision support, dynamic interface updates. | `WS /futures/Q` |

## Exchanges

Futures websocket feeds include data from the following exchanges.

- CME
- CBOT
- COMEX
- NYMEX

Business users can subscribe to per-exchange feeds, while individual plans include access to data from all exchanges.

## Market Hours and Timezone

All futures market data is provided in Central Time (CT), the standard timezone used by major U.S. futures exchanges. This unified approach ensures that trading schedules, contract details, and market events are consistently aligned. For specific trading hours and daily maintenance periods for each product, please refer to our [schedules endpoint](https://massive.com/docs/rest/futures/schedules.md) for detailed information.

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

## Infrastructure and Reliability

Our WebSocket infrastructure is engineered for speed, stability, and scalability. We maintain direct connections to major U.S. futures exchanges, including CME, CBOT, COMEX, and NYMEX, ensuring minimal latency and reliable data delivery. Our robust systems process raw exchange feeds in real-time, enabling us to distribute live market data continuously to your subscribed applications even during periods of high trading activity.

## Data Flow: From Exchanges to You

Futures market data originates directly from the major exchanges where contracts are defined and traded. We establish and maintain direct relationships with these exchanges to capture every trade, quote, and market event as it occurs. The raw data is normalized and processed by our advanced infrastructure before being pushed over WebSocket channels, ensuring that you receive timely, accurate market insights directly from the source.

## Next Steps

Explore our detailed documentation to integrate these WebSocket feeds into your applications. With our flexible subscription model, you can selectively choose the data streams that meet your needs—whether for powering live dashboards, driving algorithmic models, or conducting real-time market analysis. By leveraging Massive’s futures WebSocket feeds, you gain access to a continuous, low-latency data pipeline that empowers agile, data-driven decision-making in the dynamic futures markets.
