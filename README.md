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
