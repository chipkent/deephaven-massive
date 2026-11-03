## 21. Examples

Every example below runs in the embedded test JVM against fake REST and WebSocket servers, as
a test in `tests/test_docs.py` ([section 4.2](04-package-layout.md#42-test-layout)).

**1. Stocks Starter (15-minute delayed).**

```python
import deephaven_massive as dm

cfg = dm.MassiveConfig(delayed_subscriptions={dm.Subscription.STOCKS})   # Starter is delayed
m = dm.MassiveSession(cfg)                                  # MASSIVE_API_KEY from the environment
syms = ["AAPL", "MSFT", "NVDA"]
bars = m.stocks.bars(syms, dm.Timespan.MINUTE).append_only()   # -> delayed.massive.com/stocks
snap = m.stocks.snapshots(syms).poll(30)                    # REST; current state on every tier
daily = m.stocks.bars(syms, dm.Timespan.DAY).history("2025-01-01", "2025-06-30")   # 3 symbols -> REST
prev = m.stocks.previous_day_bar(syms).fetch()              # previous completed session (/prev)
divs = m.stocks.dividends(syms, start="2025-01-01",
                          filters={"pay_date_lte": "2025-12-31"}).fetch()
conns = m.connections()                                     # Host=delayed.massive.com Recency=DELAYED
cols = m.stocks.trades(syms).columns()                      # no network: Name, Type, Description, SuppliedBy
```

**2. Real-time multi-asset trader (Advanced plans).** No `with`: live tables must outlive the
statement.

```python
import deephaven_massive as dm

m = dm.MassiveSession()
syms = ["AAPL", "MSFT", "NVDA"]
bars = m.stocks.bars(syms, dm.Timespan.MINUTE).append_only()   # last_trade.timeframe=REAL-TIME
                                                               # -> socket.massive.com/stocks
quotes = m.stocks.quotes(syms).append_only()                # full NBBO history for as-of joins
trades = m.stocks.trades(syms).ring(100_000)
nbbo_at_trade = trades.aj(quotes, on=["Symbol", "Timestamp"], joins=["BidPrice", "AskPrice"])
current = m.stocks.quotes(syms).last_by()                   # shares the quotes stream; one row per symbol
chain = m.options.chain("NVDA", contract_type=dm.ContractType.CALL,
                        filters={"expiration_date_lte": "2026-01-16"}).poll(60)
otrades = m.options.trades(chain.where("Delta > 0.4")).blink()   # reactive; over 1,000 contracts
                                                                 # -> deferred, never fails
es = m.futures.trades(["ESZ5"]).last_by()                   # /futures on every plan
fx = m.forex.quotes(["C:EURUSD", "C:USDJPY"]).last_by()     # socket.massive.com/forex, no REST call
```

**3. Business key.**

```python
import deephaven_massive as dm

m = dm.MassiveSession()
fmv = m.stocks.fmv(dm.ALL).last_by()                        # FMV family -> business.massive.com/stocks
bars = m.stocks.bars(["AAPL"], dm.Timespan.MINUTE).append_only()
    # TICK family: socket.massive.com. With a Full Market expansion the snapshot carries
    # last_trade.timeframe=REAL-TIME; without one it carries no timeframe, and a refusal from
    # Massive names websocket_hosts.
xfmv = m.crypto.fmv(["X:BTCUSD", "X:ETHUSD"]).append_only() # business.massive.com/crypto
es = m.futures.quotes(["ESZ5"]).last_by()                   # /futures; per-venue plans receive
                                                            # their venue on the same path

# If your expansion is served from a different host, pin the TICK family. FMV is never pinned.
m2 = dm.MassiveSession(dm.MassiveConfig(websocket_hosts={dm.AssetClass.STOCKS: "socket.massive.com"}))
```

**4. Flat-file history (static results only, so `with` is right).**

```python
import deephaven_massive as dm
from deephaven.parquet import write

with dm.MassiveSession() as m:
    for year in range(2020, 2026):
        t = m.stocks.bars(dm.ALL, dm.Timespan.DAY, adjusted=False).history(
            f"{year}-01-01", f"{year}-12-31")                # AUTO -> S3 day_aggs_v1
        write(t, f"/data/us_day_bars_{year}.parquet")
    q = m.stocks.quotes(["AAPL", "SPY"]).history("2025-03-03", "2025-03-03",
                                                 source=dm.HistorySource.REST)
    es = m.futures.bars(dm.ALL, dm.Timespan.SESSION, venues={dm.FuturesVenue.CME}).history(
        "2024-01-01", "2024-12-31")                          # us_futures_cme/session_aggs_v1
    vals = m.indices.values(["I:SPX", "I:VIX"]).history("2025-06-01", "2025-06-30")
                                                             # flat files are the only source
```

**5. Reactive watchlist dashboard.**

```python
import deephaven_massive as dm
from deephaven import input_table, dtypes as dht

m = dm.MassiveSession()
wl = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])   # edited in the UI
wl_quotes = m.stocks.quotes(wl).last_by()        # subscriptions follow edits; removed rows are evicted
wl_trades = m.stocks.trades(wl).ring(10_000)
movers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
mover_quotes = m.stocks.quotes(movers).last_by()                          # follows the top 20
mover_ratios = m.stocks.financials.ratios(movers).poll(600)               # one ticker.any_of request per cycle
m.release(wl_trades)        # deterministic: T topics unsubscribed when no other table holds the stream
del wl_quotes               # released at the end of this console command, unless a UI panel shows it
```

**6. Backfill plus live.**

```python
import deephaven_massive as dm
from deephaven import merge

m = dm.MassiveSession()
q = m.stocks.trades(["AAPL"])
t = merge([q.history("2026-10-20", "2026-10-23"), q.append_only()])
```

**7. Split adjustment.** The README's "Split adjustment" section explains the bases
([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries), "Price adjustment") and holds these
examples; `tests/test_docs.py` runs every Python block of that section, in order, against the
fake REST server.

```python
import deephaven_massive as dm

m = dm.MassiveSession()
adjusted = m.stocks.bars("NVDA", dm.Timespan.DAY).history("2024-06-03", "2024-06-14")
traded = m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
    "2024-06-03", "2024-06-14"
)
splits = m.stocks.splits("NVDA").fetch()
mine = traded.raj(
    splits, on=["Symbol", "Date<ExecutionDate"], joins=["Factor=HistoricalAdjustmentFactor"]
).update(
    [
        "Factor = isNull(Factor) ? 1.0 : Factor",
        "AdjustedClose = Close * Factor",
        "AdjustedVolume = Volume / Factor",
    ]
)
```
