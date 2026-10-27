> For the full documentation index, see: https://massive.com/docs/llms.txt

# Crypto Overview

At Massive, we provide real-time streaming access to the global cryptocurrency market through WebSockets. Our feeds include trades, quotes, time-based aggregates (per-minute and per-second), and Fair Market Value (FMV) for supported crypto pairs. By leveraging these streaming endpoints, developers, traders, and analysts can monitor dynamic market conditions, inform algorithmic trading strategies, and power responsive dashboards, without the need for repetitive requests.

## Available Feeds

| Feeds | Description | Path |
| --- | --- | --- |
| [Aggregates (Per Minute)](https://massive.com/docs/websocket/crypto/aggregates-per-minute.md) | Stream minute-by-minute aggregated OHLC (Open, High, Low, Close) and volume data for a specified cryptocurrency pair via WebSocket. These aggregates update continuously in Coordinated Universal Time (UTC). If no trades occur within a given minute, no bar is emitted, transparently indicating a period without trading activity. This endpoint provides a live feed of aggregated bars, enabling users to monitor intraday price movements, refine trading strategies, and power real-time crypto market visualizations. Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, market research. | `WS /crypto/XA` |
| [Aggregates (Per Second)](https://massive.com/docs/websocket/crypto/aggregates-per-second.md) | Stream second-by-second aggregated OHLC (Open, High, Low, Close) and volume data for a specified cryptocurrency pair via WebSocket. These aggregates update continuously in Coordinated Universal Time (UTC). If no trades occur within a given minute, no bar is emitted, transparently indicating a period without trading activity. This endpoint provides a live feed of aggregated bars, enabling users to monitor intraday price movements, refine trading strategies, and power real-time crypto market visualizations. Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, market research. | `WS /crypto/XAS` |
| [Trades](https://massive.com/docs/websocket/crypto/trades.md) | Stream trade data for crypto pairs via WebSocket. Each message delivers key trade details (price, size, exchange, conditions, and timestamps) as they occur, enabling users to track market activity, power live dashboards, and inform rapid decision-making. Use Cases: Live monitoring, algorithmic trading, market analysis, data visualization. | `WS /crypto/XT` |
| [Quotes](https://massive.com/docs/websocket/crypto/quotes.md) | Stream quote data for specified cryptocurrency pairs via WebSocket. Each message delivers current bid/ask prices, sizes, and relevant metadata from multiple exchanges as they update, allowing users to monitor evolving market conditions, guide trading decisions, and support responsive, data-driven applications. Use Cases: Live monitoring, market analysis, trading decision support, dynamic interface updates. | `WS /crypto/XQ` |
| [Fair Market Value](https://massive.com/docs/websocket/crypto/fair-market-value.md) | Stream real-time Fair Market Value (FMV) data for a specified cryptocurrency pair via WebSocket. This proprietary metric, available exclusively to Business plan users, provides an algorithmically derived, real-time estimate of the crypto pair’s fair market price. By delivering accurate, continuous valuation data, this feed supports more informed trading decisions, enhanced analytics, and improved risk management. Use Cases: Pricing strategies, algorithmic modeling, risk assessment, investor decision-making. | `WS /business/crypto/FMV` |

## Market Hours and Timezone

Unlike traditional markets, cryptocurrency trading operates continuously, 24 hours a day, 7 days a week, across multiple global exchanges. This around-the-clock activity enables you to engage with markets at any time, from anywhere.

All crypto data is standardized to Coordinated Universal Time (UTC). This global alignment simplifies time-sensitive analytics, ensuring consistent data integration and interpretation, regardless of your geographic location or the exchanges you track.

## Infrastructure and Reliability

Our WebSocket infrastructure is engineered to handle the decentralized, high-volume nature of cryptocurrency trading. We connect directly to multiple leading exchanges, ensuring minimal latency and maximum data integrity. Redundant systems, load balancing, and scalable architecture enable stable, low-latency data delivery even during periods of heightened market activity.

This robust framework supports latency-sensitive applications, such as automated trading bots, real-time order book visualizations, and high-frequency analytical models.

## Data Flow: From Exchanges to You

Cryptocurrency data originates from numerous global exchanges, each with its own market structure and liquidity. At Massive, we source data directly from major venues including Coinbase, Bitfinex, Bitstamp, and Kraken. Binance was previously included but has not been an active source since 2021. We regularly review and update our exchange coverage to ensure transparency and data accuracy.

We aggregate this exchange data, normalize differing formats, and stream a unified dataset over WebSockets. As soon as trades occur, quotes shift, or order book levels update, this information is relayed directly to your subscribed channels—keeping your systems aligned with live market conditions.

## Next Steps

Explore our documentation to seamlessly integrate these WebSocket feeds into your applications. With a flexible subscription model, you can focus on the crypto pairs and data streams most relevant to your objectives, enabling tasks such as:

* Powering live market dashboards with up-to-the-second data
* Supporting algorithmic models that respond instantly to changing conditions
* Enhancing trading strategies with deep, order book-driven insights
* Conducting real-time risk assessments and liquidity analysis

By utilizing Massive’s crypto WebSocket feeds, you unlock a continuous stream of actionable market data, positioning your applications to excel in the fast-paced, global cryptocurrency arena.
