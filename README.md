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
environment. [`AGENTS.md`](AGENTS.md) has the full command list and the contribution rules.

### Live integration tests

The tests in `tests/**/test_*_integration.py` call Massive itself. They carry the `integration`
marker, which the default run deselects, and they skip without `MASSIVE_API_KEY`, so neither
`./scripts/check.sh` nor CI ever reaches Massive. Run them, and only them, with a key:

```bash
MASSIVE_API_KEY=... uv run pytest -m integration 2>&1 | tee live-checks.log
```

The flat-file tests also need `MASSIVE_S3_ACCESS_KEY_ID` and `MASSIVE_S3_SECRET_ACCESS_KEY`.
A few tests need more and skip without it: `MASSIVE_BUSINESS_API_KEY` (a Business key),
`MASSIVE_DELAYED_API_KEY` (a delayed key), `MASSIVE_LIVE_BASIC_SUBSCRIPTION` (a subscription on
the free Basic tier, such as `options`). Each test records what Massive sent; the "live
evidence" section at the end of the run lists it, test by test.

Four live tests deliberately exceed Massive's subscription and connection limits (1,000
topics, 501 options contracts, a wildcard, and a second connection with the same key). They
also carry the `integration_limits` marker: `-m integration` leaves them out, and they run
only on their own:

```bash
MASSIVE_API_KEY=... uv run pytest -m integration_limits 2>&1 | tee live-limits.log
```

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
