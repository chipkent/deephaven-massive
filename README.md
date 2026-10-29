# deephaven-massive

`deephaven-massive` turns [Massive.com](https://massive.com) (formerly Polygon.io) market data
into live and static [Deephaven](https://deephaven.io) tables. Massive's WebSocket streams
become refreshing tables, its REST endpoints become static or polled tables, and its S3 flat
files become historical tables. The library runs inside an existing Deephaven server.

## Status

The library is being rewritten and has no usable public API yet. The REST, WebSocket, and
flat-file transports are not implemented, so it cannot produce market data tables. Until the rewrite finishes, [`plans/rework/DESIGN.md`](plans/rework/DESIGN.md) is the
specification and [`plans/rework/PLAN.md`](plans/rework/PLAN.md) is the schedule. A full README
follows when the public API lands.

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
- Live WebSocket bars are as traded and are never adjusted afterwards.
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

**Live tables jump at a split.** A live table holds what traded. On a split's execution date
every price, pre-market included, is already on the new basis, so a live table, or a table that
joins adjusted history to live bars, shows a step there. That step is the data, not an error.
Build such a table again after a split, so its history is adjusted to the new basis:

```python
from deephaven import merge


def history_and_live(symbol, start):
    """Adjusted minute bars from `start`, then live ones; call it again after a split."""
    bars = m.stocks.bars(symbol, dm.Timespan.MINUTE)
    return merge([bars.history(start), bars.append_only()])
```

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
run only with `-m integration_limits`. Declare the plans the key holds in `MASSIVE_LIVE_PLANS`,
and each test runs or skips, naming the plan it needs. [`docs/live-tests.md`](docs/live-tests.md)
says how to run them, in which order, what each plan can verify, and how to get the S3
credentials; each test's docstring says what its evidence means.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
