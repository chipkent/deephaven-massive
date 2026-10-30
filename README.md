# deephaven-massive

`deephaven-massive` turns [Massive.com](https://massive.com) (formerly Polygon.io) market data
into live and static [Deephaven](https://deephaven.io) tables. Massive's WebSocket streams
become refreshing tables, its REST endpoints become static or polled tables, and its S3 flat
files become historical tables. The library runs inside an existing Deephaven server.

## What works today

Every dataset in the column references is implemented: live streams, polled REST tables, and
history from REST or the flat files. The same code runs on every Massive plan, and a dataset
your plan lacks fails with a message that says so. Each dataset is tested against simulated
Massive servers that answer the way Massive's documentation says Massive does.

The library has also run against Massive itself: the live tests have run on the owner's own
plans, a mix of paid and free tiers. Some questions remain open because those plans cannot
settle them, such as which host a delayed plan streams from, how index daily bars and futures
sessions are stamped, and parts of the flat files. On those points the library follows its best
reading of Massive's documentation. Where it can tell, it checks such a detail in what Massive
sends and stops with a clear message rather than return rows it cannot vouch for.
[`docs/live-tests.md`](docs/live-tests.md) lists the open questions and the plan that settles
each one.

The library is not on PyPI yet. [Install](docs/getting-started/install.md) says how to install it
into a Deephaven server.

## First tables

A session needs your Massive API key in the `MASSIVE_API_KEY` environment variable. Run this in
the Deephaven console:

```python
import deephaven_massive as dm

m = dm.MassiveSession()

# A live table: each symbol's latest trade, updated as trades arrive.
latest = m.stocks.trades(["AAPL", "MSFT"]).last_by()

# A history table: Apple's daily bars for one week, read once.
bars = m.stocks.bars("AAPL", dm.Timespan.DAY)
week = bars.history("2024-06-03", "2024-06-07")

# What each column of those bars means.
columns = bars.columns()
```

Every dataset works this way. A method such as `m.stocks.trades(...)` describes the data, and a
verb picks the table you get:

| Verb | Table |
|---|---|
| `blink()`, `append_only()`, `ring(n)`, `last_by()` | live tables fed by Massive's WebSocket streams |
| `fetch()` | a table read once from Massive's REST API |
| `poll(seconds)` | a table that re-reads REST every few seconds |
| `history(start, end)` | past rows, read once from REST or from Massive's flat files |

A verb a dataset cannot serve does not exist on it, so tab completion in the console shows what
each dataset can do. `dm.datasets()` returns a table of every dataset, with what it holds and
its verbs, and works before you set a key. `help(dm)` lists the whole API.

## Read next

- [Getting started](docs/getting-started/): [installing](docs/getting-started/install.md),
  [first tables](docs/getting-started/first-tables.md),
  [table shapes](docs/getting-started/table-shapes.md),
  [watchlists](docs/getting-started/watchlists.md),
  [plans and limits](docs/getting-started/plans.md),
  [releasing tables](docs/getting-started/releasing.md),
  [diagnostics](docs/getting-started/diagnostics.md), and
  [troubleshooting](docs/getting-started/troubleshooting.md).
- Guides: [reading history](docs/guides/history.md),
  [following a schedule](docs/guides/schedules.md), and
  [joining across asset classes](docs/guides/cross-asset.md).
- [Column references](docs/datasets/), one page per namespace: every dataset's columns, their
  types, and what each one means.
- [Tester guide](docs/tester-guide.md): every step for an outside tester, from installing a
  pinned version to the console exercises, the live run, and bug reports.

## Split adjustment

When a company splits its stock, say 10 for 1, every share becomes ten shares worth a tenth of
the price. A chart of the raw prices then drops 90% overnight although nothing happened to the
company. Massive can restate the prices before a split in the split's terms so the series is
continuous: that is a *split-adjusted* price. This section says which prices you get, and how
to get the other kind.

**The facts.**

- Massive adjusts stock and option bars for splits (forward and reverse) when a request asks
  for it, and never for cash dividends.
- Its flat files are as traded: not adjusted for splits, dividends, or anything else, and
  they carry no adjustment factor.
- Live WebSocket bars are as traded: on the current share basis, which is also the adjusted
  basis until the next split. They are never adjusted afterwards.
- Index, forex, crypto, and futures bars have no corporate actions: there is nothing to adjust.

**What the library does.** `m.stocks.bars()`, `m.options.bars()`, and both `daily_open_close()`
take `adjusted=True` by default. One `history()` call never mixes adjusted and as-traded prices:
adjusted bars come only from REST, Massive's `adjusted` echo on every page is checked (a page
on the other basis fails the call), and `adjusted=False` reads REST or the flat files. Each
price and volume column's description (`columns()`, the web UI tooltips, and
[`docs/datasets/`](docs/datasets/)) says the same.

```python
import deephaven_massive as dm

m = dm.MassiveSession()

# Split-adjusted daily bars (the default): served by REST, one basis for the whole call.
# NVDA split 10 for 1 on 2024-06-10, so its 2024-06-07 close reads about 120.9.
adjusted = m.stocks.bars("NVDA", dm.Timespan.DAY).history("2024-06-03", "2024-06-14")

# As traded: its 2024-06-07 close reads about 1208. Flat files can serve these too. Daily
# bars of dm.ALL come adjusted from REST's grouped daily bars; intraday bars of dm.ALL need
# adjusted=False, since only REST adjusts and its intraday bars need explicit symbols.
traded = m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
    "2024-06-03", "2024-06-14"
)
```

**Adjusting as-traded bars yourself.** The splits dataset gives every split's cumulative
`HistoricalAdjustmentFactor`. Massive's rule: for a price on date D, take the first split whose
`ExecutionDate` is after D and multiply the price by its factor (and divide the volume by it).
`Date` and `ExecutionDate` are both ISO `yyyy-MM-dd` text, so a reverse as-of join finds that
split. A null factor means no later split, so a factor of 1. The recipe applies every row of
the splits dataset, stock dividends (`AdjustmentType` `stock_dividend`) included; whether
Massive's `adjusted=True` applies those too is not documented, so the two can differ around a
stock dividend.

```python
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

A bar longer than a day has a null `Date`, so adjust daily bars and aggregate them afterwards.

**Live tables jump at a split.** A live table holds what traded. Until the next split, that is
the same basis as adjusted history, so adjusted history and live bars join without a seam. On a
split's execution date every price, pre-market included, moves to the new basis, so a stream
that runs across a split, or a table that joins adjusted history to live bars, shows a step
there. That step is the data, not an error. Build such a table again after a split, so its
history is adjusted to the new basis. The recipe below backfills minute bars; it works for any
bar shorter than a day:

```python
from deephaven import merge


def history_and_live(symbol, start):
    """Adjusted minute bars from `start`, then live ones; call it again after a split."""
    bars = m.stocks.bars(symbol, dm.Timespan.MINUTE)
    return merge([bars.history(start), bars.append_only()])
```

**History and live bars can differ slightly.** Massive recomputes its REST bars with trades
reported late, after the live bar went out, so a minute's REST bar can differ a little from the
live bar of the same minute. In the recipe above, the history rows carry REST's values and the
rows that arrive afterwards carry the stream's.

**What can still mix bases.** A long download that runs while Massive applies a split overnight
can return pages on both bases, and nothing can detect it: download again after the split.
`m.stocks.bars(dm.ALL, dm.Timespan.MINUTE).history(...)` with the default `adjusted=True` raises,
since flat files serve only `adjusted=False` and REST needs explicit symbols. The indicators
default to Massive's own basis, adjusted, and their responses do not say which basis they used.

## Development

The project is a Python library managed with [uv](https://docs.astral.sh/uv/). The test suite
needs JDK 17 or later. Run everything from the repository root:

```bash
uv sync --upgrade                          # create or update .venv with the newest versions
uv run pre-commit install --hook-type pre-commit --hook-type pre-push   # once per clone
./scripts/check.sh                         # full gate: lint, format, types, tests + coverage
./scripts/check.sh fast                    # lint and types only; fixes what ruff can fix
```

Always go through `uv run`: bare `python`, `pytest`, `ruff`, and `mypy` use the wrong
environment. [`AGENTS.md`](AGENTS.md) has the everyday commands and the contribution rules.

### Live integration tests

The tests in `tests/**/test_*_integration.py` call Massive itself. They carry the `integration`
marker, which the default run deselects, and they skip without `MASSIVE_API_KEY` and under CI,
so neither `./scripts/check.sh` nor CI ever reaches Massive. `uv run pytest -m integration_smoke`
checks every dataset and verb once, unattended; tests that deliberately exceed Massive's limits
run only with `-m integration_limits`. The first live test of a run asks Massive which plans
the key holds (or reads them from `MASSIVE_LIVE_PLANS`), and each test runs or skips, naming the
plan it needs. [`docs/live-tests.md`](docs/live-tests.md) says how to run them, what each plan
can verify, and how to get the S3 credentials; each test's docstring says what its evidence
means.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
