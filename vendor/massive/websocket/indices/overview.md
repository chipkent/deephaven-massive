> For the full documentation index, see: https://massive.com/docs/llms.txt

# Indices Overview

At Massive, we provide real-time streaming access to index values and aggregates through WebSockets, enabling your applications to receive continuous updates directly as market conditions evolve. Our WebSocket feeds deliver value snapshots and time-based aggregates (both per-minute and per-second bars) for a wide range of indices. By tapping into these streaming endpoints, traders, analysts, and developers can track market benchmarks, gauge broad economic sentiment, and power dynamic dashboards -- without the need for constant polling.

## Available Feeds

| Feeds | Description | Path |
| --- | --- | --- |
| [Aggregates (Per Minute)](https://massive.com/docs/websocket/indices/aggregates-per-minute.md) | Stream minute-by-minute aggregated OHLC (Open, High, Low, Close) for a specified index via WebSocket. These aggregates update continuously in Eastern Time (ET) and capture changes in the index’s values. Unlike stocks or options, index aggregates are derived from index values rather than individual trades. If no new index updates occur within a given minute, no bar is emitted. By providing an ongoing feed of updated market snapshots, this endpoint enables users to track intraday index movements, refine analysis, and power real-time market visualizations. Use Cases: Real-time monitoring, dynamic charting, intraday trend analysis, market research. | `WS /indices/AM` |
| [Aggregates (Per Second)](https://massive.com/docs/websocket/indices/aggregates-per-second.md) | Stream second-by-second aggregated OHLC (Open, High, Low, Close) for a specified index via WebSocket. These aggregates update continuously in Eastern Time (ET) and capture changes in the index’s values. Unlike stocks or options, index aggregates are derived from index values rather than individual trades. If no new index updates occur within a given minute, no bar is emitted. By providing an ongoing feed of updated market snapshots, this endpoint enables users to track intraday index movements, refine analysis, and power real-time market visualizations. Use Cases: Real-time monitoring, dynamic charting, intraday trend analysis, market research. | `WS /indices/A` |
| [Value](https://massive.com/docs/websocket/indices/value.md) | Stream real-time index value updates for specified index tickers via WebSocket. Each message provides the index’s current value and timestamp, allowing users to monitor fluctuations in key market benchmarks or sectors throughout the trading session. This continuous feed supports live market analysis, trend identification, and integration of index-based signals into trading strategies and research. Use Cases: Market analysis, trend detection, portfolio benchmarking, trading strategy refinement. | `WS /indices/V` |

## Market Hours and Timezone

Indices typically update during the primary hours of the markets they represent, most often aligned with U.S. equity trading sessions in Eastern Time (ET):

* Regular Market Hours: Monday through Friday, 9:30 AM to 4:00 PM ET

Some indices may also update during pre-market and after-hours periods, depending on their underlying methodologies and constituent assets. All timestamps are provided as Unix timestamps (seconds since epoch, UTC). When converting these timestamps into human-readable form (e.g., market open at 9:30 AM), remember they represent UTC time, not Eastern Time (ET). To correctly align data with market hours or dates, you'll need to explicitly convert timestamps from UTC to ET during your analysis.

## Infrastructure and Reliability

Our WebSocket infrastructure is designed to handle the unique update patterns of indices, which may refresh at specific intervals rather than tick-by-tick. By directly connecting to the data sources and maintaining robust, scalable systems, we ensure timely delivery of index values and aggregate data. Redundant data centers and load balancing bolster reliability, allowing you to maintain stable, low-latency data streams even under fluctuating market conditions.

This real-time architecture supports latency-sensitive applications such as intraday benchmarking, trend detection, and sector-wide analyses, allowing you to observe and react to market shifts as they happen.

## Data Flow: From Exchanges to You

Index values are calculated and disseminated by the exchanges or designated index administrators. Massive aggregates these values and processes them into consistent, standardized formats before streaming them over WebSockets. As soon as the index administrator publishes a new value, our infrastructure relays it directly to your subscribed channels.

This direct, event-driven data flow ensures you have access to fresh index readings and time-based aggregates as soon as they become available, enabling rapid, data-driven insights.

## Next Steps

Integrate these WebSocket feeds into your applications with the help of our comprehensive documentation. With a simple subscription model, you can focus on the indices and data streams that align with your goals, enabling tasks such as:

* Real-time benchmarking against broad market indicators
* Intraday trend and volatility analysis
* Dynamic charting and visualizations
* Integration into algorithmic models that react to market-wide signals

By leveraging Massive’s indices WebSocket feeds, you gain a continuous, low-latency data pipeline for market metrics, empowering informed strategies and confident decision-making in the ever-evolving financial landscape.
