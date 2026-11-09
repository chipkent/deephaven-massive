> For the full documentation index, see: https://massive.com/docs/llms.txt

# Options Overview

At Massive, we provide real-time streaming access to U.S. options market data through WebSockets, allowing you to receive a continuous flow of updates directly into your applications. Our WebSocket feeds cover trades, quotes, and aggregate bars (both per-minute and per-second), as well as Fair Market Value (FMV) for eligible subscribers. With these streaming endpoints, developers, traders, and analysts can monitor live market conditions, power complex strategies, and update visualizations dynamically -- without the need for repeated requests.

## Available Feeds

| Feeds | Description | Path |
| --- | --- | --- |
| [Aggregates (Per Minute)](https://massive.com/docs/websocket/options/aggregates-per-minute.md) | Stream minute-by-minute aggregated OHLC (Open, High, Low, Close) and volume data for a specified options contract via WebSocket. These aggregates are updated continuously in Eastern Time (ET). Each bar is constructed solely from qualifying trades that meet specific conditions; if no eligible trades occur within a given minute, no bar is emitted. By delivering an ongoing feed of updated market snapshots, this endpoint enables users to closely monitor intraday price movements, enhance trading strategies, and support live data visualizations in the options market. Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, automated trading. | `WS /options/AM` |
| [Aggregates (Per Second)](https://massive.com/docs/websocket/options/aggregates-per-second.md) | Stream second-by-second aggregated OHLC (Open, High, Low, Close) and volume data for a specified options contract via WebSocket. These aggregates are updated continuously in Eastern Time (ET). Each bar is constructed solely from qualifying trades that meet specific conditions; if no eligible trades occur within a given minute, no bar is emitted. By delivering an ongoing feed of updated market snapshots, this endpoint enables users to closely monitor intraday price movements, enhance trading strategies, and support live data visualizations in the options market. Use Cases: Real-time monitoring, dynamic charting, intraday strategy development, automated trading. | `WS /options/A` |
| [Trades](https://massive.com/docs/websocket/options/trades.md) | Stream tick-level trade data for option contracts via WebSocket. Each message delivers key trade details (price, size, exchange, conditions, and timestamps) as they occur, enabling users to track market activity, power live dashboards, and inform rapid decision-making. Use Cases: Live monitoring, algorithmic trading, market analysis, data visualization. | `WS /options/T` |
| [Quotes](https://massive.com/docs/websocket/options/quotes.md) | Stream quote data for specified options contracts via WebSocket. Each message delivers current best bid/ask prices, sizes, and associated metadata as they update, enabling users to monitor dynamic market conditions and inform trading decisions. Due to the high bandwidth and message rates associated with options quotes, users can subscribe to a maximum of 1,000 option contracts per connection. Use Cases: Live monitoring, market analysis, trading decision support, dynamic interface updates. | `WS /options/Q` |
| [Admin & FLEX Options](https://massive.com/docs/websocket/options/admins.md) | Stream real-time administrative (admin) messages for options via WebSocket, including OPRA FLEX trade notifications. Each message delivers the raw administrative text along with its exchange, sequence number, and nanosecond SIP timestamp. FLEX (FLexible EXchange) options are customized, exchange-traded options contracts that let counterparties negotiate non-standard terms — such as the strike price, expiration date, exercise style, and settlement type — beyond what standard listed options offer. FLEX trades are disseminated by OPRA as administrative messages and surfaced through this feed. Use Cases: FLEX trade monitoring, market operations tracking, data auditing. | `WS /options/AD` |
| [Fair Market Value](https://massive.com/docs/websocket/options/fair-market-value.md) | Stream real-time Fair Market Value (FMV) data for a specified options contract via WebSocket. This proprietary metric, available exclusively to Business plan users, provides an algorithmically derived, real-time estimate of an option’s fair market price. By delivering accurate, continuous valuation data, this feed supports more informed options trading decisions, enhanced analytics, and improved risk management. Use Cases: Pricing strategies, algorithmic modeling, risk assessment, investor decision-making. | `WS /business/options/FMV` |

## Market Hours and Timezone

U.S. options trading primarily occurs during standard equity market sessions:

* Regular Market Hours: Monday through Friday, 9:30 AM to 4:00 PM Eastern Time (ET)

While some limited trading may occur outside these hours, options activity is concentrated within the regular session. All timestamps are provided as Unix timestamps (seconds since epoch, UTC). When converting these timestamps into human-readable form (e.g., market open at 9:30 AM), remember they represent UTC time, not Eastern Time (ET). To correctly align data with market hours or dates, you'll need to explicitly convert timestamps from UTC to ET during your analysis.

## Infrastructure and Reliability

Our WebSocket infrastructure is specifically engineered for the high-throughput and data-intensive nature of the U.S. options market. By co-locating with the Options Price Reporting Authority (OPRA), we minimize latency and ensure you receive updates as they are consolidated from all U.S. options exchanges. Redundant systems and load balancing further enhance reliability, allowing you to maintain steady, low-latency data streams even under peak market conditions.

This real-time delivery architecture supports latency-sensitive applications, such as algorithmic trading and intraday analytics, providing you with a continuous feed of fresh market data as it happens.

## Data Flow: From Exchanges to You

All U.S. options exchanges report their trades and quotes to OPRA, which consolidates them into a single, authoritative feed. Massive connects directly to OPRA, ensuring you receive the official National Best Bid and Offer (NBBO) and complete trade information for all listed options contracts. As soon as updates are available, we push them over your subscribed WebSocket channels, delivering near-instantaneous market insights to your applications.

## Next Steps

Utilize our comprehensive documentation to integrate these WebSocket feeds into your applications. With a simple subscription model, you can focus on the data streams and contracts that matter most to you, enabling tasks such as:

* Powering live option dashboards and analytics
* Supporting algorithmic models that react instantly to market shifts
* Enhancing risk management and position monitoring with real-time updates
* Driving sophisticated options trading strategies with low-latency data

By leveraging Massive’s options WebSocket feeds, you equip your applications to respond dynamically to evolving market conditions, unlocking real-time insights that inform competitive and profitable trading decisions.
