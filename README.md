# `deephaven-massive`

**`deephaven-massive`** bridges **Massive.com** (formerly Polygon.io) real-time, delayed, historical, fundamental, macroeconomic, alternative, and S3 flat-file data feeds directly into **Deephaven** (`deephaven.table.Table`).

## Key Features

- **100% Massive.com Catalog Coverage**:
  - **Market Asset Classes**: `m.stocks`, `m.options` (OPRA), `m.futures` (CME, CBOT, NYMEX, COMEX), `m.indices`, `m.forex`, `m.crypto`
  - **Fundamentals & Short Data**: `short_interest`, `short_volume`, `share_float`, `balance_sheets`, `income_statements`, `cash_flow_statements`, `ratios`, `dividends`, `splits`, `ipos`, SEC filings (`10-K`, `8-K`, `13F`, `Form 3`, `Form 4`, risk factors, disclosures)
  - **Economy / Federal Reserve**: `funding_conditions` (SOFR, EFFR, OBFR, TGCR, IORB, repo, commercial paper), `treasury_yields`, `inflation`, `inflation_expectations`, `labor_market`
  - **Partners & Alternative Data**: Benzinga (ratings, consensus, analyst insights, earnings, guidance, news), ETF Global (analytics, constituents, fund flows, profiles), TMX corporate events, Fable EU consumer spending
  - **S3 Flat Files (`files.massive.com`)**: Streaming `.csv.gz` bulk daily partitions with PyArrow predicate pushdown
- **Explicit, Memory-Safe Table Materializations**:
  - `.latest(seed=False, by=None)`: Keyed current snapshot via `blink_table.last_by(key_cols)` ($O(N_{\text{keys}})$ RAM)
  - `.history(seed_since=None)`: Append-only complete history via `blink_to_append_only(blink_table)` ($O(N_{\text{ticks}})$ RAM)
  - `.ring(capacity)`: Bounded FIFO circular window via `ring_table(blink_table, capacity)` ($O(K)$ RAM)
  - `.blink()`: Raw single-cycle `table_publisher` blink stream ($O(N_{\text{batch}})$ RAM)
  - `.snapshot()` & `.poll(interval, mode="replace" | "upsert" | "append")`: Static or periodically polled REST snapshots
  - `.historical(start, end, source="auto" | "rest" | "flat_files")`: Historical queries across REST or S3 Flat Files
- **Reactive Deephaven `Table` Watchlists**:
  - Pass a live Deephaven `Table` (e.g., an `input_table` watchlist or portfolio positions table) to any streamable feed (`m.stocks.quotes(watchlist).latest(seed=True)`). Adding, modifying, or removing symbols automatically updates WebSocket subscriptions and immediately evicts unsubscribed rows from `.latest()`.

## Quick Start

```python
from deephaven import input_table, dtypes as dht
from deephaven_massive import MassiveSession

m = MassiveSession(api_key="YOUR_MASSIVE_API_KEY", feed="realtime")

# 1. Keyed latest quotes snapshot (O(unique symbols) RAM)
latest_quotes = m.stocks.quotes(["AAPL", "MSFT", "NVDA"]).latest(seed=True)

# 2. Complete append-only trade history since stream start
trade_history = m.stocks.trades(["AAPL", "MSFT"]).history()

# 3. Reactive watchlist driven by a Deephaven Table
watchlist = input_table({"Symbol": dht.string}, key_cols=["Symbol"])
watchlist.add( m.empty_watchlist(["AAPL", "TSLA"]) )
live_portfolio_quotes = m.stocks.quotes(watchlist).latest(seed=True)

# 4. Option chain with flattened Greeks & Implied Volatility polled every 5s
aapl_chain = m.options.chain("AAPL", expiration="2025-06-20").poll(interval=5.0)

# 5. Short Interest, Short Volume & Fed Overnight Funding Conditions
short_int = m.stocks.short_interest(ticker="GME", settlement_date_gte="2024-01-01")
short_vol = m.stocks.short_volume(ticker="GME", date_gte="2025-01-01")
funding   = m.economy.funding_conditions(date_gte="2025-01-01")
```
