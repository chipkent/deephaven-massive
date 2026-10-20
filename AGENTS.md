# AGENTS.md — `deephaven-massive`

`deephaven-massive` is a Python library (`>=3.10`, managed with `uv`) that bridges **Massive.com** (formerly Polygon.io) real-time WebSocket streams, REST APIs, and S3 Flat Files (`files.massive.com`) into live and static **Deephaven** tables (`deephaven.table.Table`).

---

## 1. Quick Commands & Pre-Commit Gate

Run all commands from the repository root via `uv`:

```bash
# Install/sync environment (requires JDK 17+ for embedded deephaven-server)
uv sync --all-extras --dev

# Run the full pre-commit verification suite (Ruff check + format + Mypy + Pytest 100% cov)
./scripts/pre-commit.sh

# Install the local git pre-commit hook (.git/hooks/pre-commit -> scripts/pre-commit.sh)
./scripts/install-hooks.sh

# Individual checks during iterative development
uv run ruff check src tests            # Lint (includes Google docstring D-rules)
uv run ruff format src tests           # Auto-format (100-char line length)
uv run mypy src tests                  # Strict static type check (37 files)
uv run pytest                          # 113 tests + 100.00% statement & branch coverage gate
uv build                               # Build sdist and wheel
```

---

## 2. Non-Negotiable Quality Gates

Every change **must** pass all four gates in [`scripts/pre-commit.sh`](scripts/pre-commit.sh) before committing:

1. **100.00% Statement & Branch Coverage**: `pytest` runs with `--cov=deephaven_massive --cov-branch --cov-fail-under=100`. Never introduce untested branches or dead defensive code.
2. **Zero Ruff Errors & Formatting Diffs**: Enforces `E`, `W`, `F`, `I`, `B`, `UP`, `SIM`, `RUF`, and `D` (`pydocstyle` with `convention = "google"`).
3. **Zero Mypy Errors**: Enforced across both `src/` and `tests/`. Preserve [`src/deephaven_massive/py.typed`](src/deephaven_massive/py.typed) (PEP 561).
4. **O'Reilly-Caliber Google Docstrings & Docs Sync**: All modules, classes, methods, and functions must have crystal-clear Google-style docstrings. Any change to public signatures, defaults, or table semantics must be reflected in [`README.md`](README.md).
5. **Conventional Commits**: Commit messages and Pull Request titles must follow the [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/) specification (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `ci:`, `chore:`), enforced by [`.github/workflows/conventional-pr-check.yml`](.github/workflows/conventional-pr-check.yml).

---

## 3. Progressive Disclosure: Where to Look by Task

Do **not** read the entire codebase upfront. Consult only the files relevant to your task:

| If your task involves... | Read these files first |
| :--- | :--- |
| **User-facing API, examples, or quickstart** | [`README.md`](README.md), [`src/deephaven_massive/session.py`](src/deephaven_massive/session.py), [`src/deephaven_massive/config.py`](src/deephaven_massive/config.py) |
| **Full endpoint catalog & architectural rationale** | [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) *(Note: all domain methods return `FeedBuilder`; only `m.flat_files.*` returns `Table` directly)* |
| **Table delivery modes (`.latest`, `.history`, `.ring`, `.blink`, `.snapshot`, `.poll`, `.historical`)** | [`src/deephaven_massive/engine/builder.py`](src/deephaven_massive/engine/builder.py), [`tests/test_builder_and_base.py`](tests/test_builder_and_base.py) |
| **Micro-batching, blink tables, or REST+WS startup seeding** | [`src/deephaven_massive/engine/publisher.py`](src/deephaven_massive/engine/publisher.py), [`src/deephaven_massive/engine/poller.py`](src/deephaven_massive/engine/poller.py), [`tests/test_publisher_and_poller.py`](tests/test_publisher_and_poller.py) |
| **WebSocket multiplexing, auth, reconnects, or OPRA sharding** | [`src/deephaven_massive/engine/ws_multiplexer.py`](src/deephaven_massive/engine/ws_multiplexer.py), [`tests/test_ws_multiplexer_and_watchlist.py`](tests/test_ws_multiplexer_and_watchlist.py) |
| **Reactive Deephaven `Table` watchlists & row eviction** | [`src/deephaven_massive/engine/watchlist.py`](src/deephaven_massive/engine/watchlist.py), [`tests/test_ws_multiplexer_and_watchlist.py`](tests/test_ws_multiplexer_and_watchlist.py) |
| **PyArrow conversion, timestamps (`to_utc_ns`), or `ExecutionContext`** | [`src/deephaven_massive/engine/arrow_bridge.py`](src/deephaven_massive/engine/arrow_bridge.py), [`tests/test_arrow_bridge_and_schemas.py`](tests/test_arrow_bridge_and_schemas.py) |
| **Symbol normalization across WS, REST, and S3 (`O:`, `I:`, `C:`, `X:`)** | [`src/deephaven_massive/engine/symbols.py`](src/deephaven_massive/engine/symbols.py), [`tests/test_symbols.py`](tests/test_symbols.py) |
| **`PascalCase` schemas & JSON/CSV row normalizers** | [`src/deephaven_massive/schemas/market.py`](src/deephaven_massive/schemas/market.py), [`reference.py`](src/deephaven_massive/schemas/reference.py), [`economy.py`](src/deephaven_massive/schemas/economy.py), [`partners.py`](src/deephaven_massive/schemas/partners.py) |
| **REST pagination, operator filters (`_gte`, `_any_of`), or domain endpoints** | [`src/deephaven_massive/domains/base.py`](src/deephaven_massive/domains/base.py) and per-domain modules (`stocks.py`, `options.py`, `futures.py`, `indices.py`, `forex.py`, `crypto.py`, `economy.py`, `partners.py`) |
| **S3 Flat Files (`files.massive.com` `.csv.gz` streaming & pushdown)** | [`src/deephaven_massive/domains/flat_files.py`](src/deephaven_massive/domains/flat_files.py), [`tests/test_flat_files.py`](tests/test_flat_files.py) |
| **Embedded Deephaven JVM test fixture & test helpers** | [`tests/conftest.py`](tests/conftest.py) |

---

## 4. Critical Deephaven & Engine Invariants

When modifying `src/deephaven_massive/engine/`, preserve these hard-won invariants:

### 4.1 Deephaven Concurrency & UpdateGraph (UG) Lock (`deephaven-core` PR #7457)
- **Always bind `ExecutionContext` on background threads**: Calling `dh.arrow.to_table()`, `publisher.add()`, `input_table.add()`/`delete()`, or table operations from a background thread (`asyncio`, `Timer`, `ThreadPoolExecutor`) without an active context raises `DHError: No execution context`. Always wrap Deephaven calls in `with exec_ctx_scope(self._exec_ctx):` (from [`arrow_bridge.py`](src/deephaven_massive/engine/arrow_bridge.py)).
- **Never block inside `table_listener` callbacks**: Callbacks passed to `deephaven.table_listener.listen(..., do_replay=True)` execute *on a Deephaven UpdateGraph thread while holding the UG lock*. Acquiring a lock held by a thread waiting on the UG lock, or performing synchronous network/REST I/O inside the callback, causes an immediate **AB-BA deadlock**. Always dispatch listener diffs asynchronously onto a worker outside the UG lock (see `_enqueue_diff` in [`watchlist.py`](src/deephaven_massive/engine/watchlist.py)).
- **Serialize shared primitive mutations**: Guard `publisher.add()` (`MicroBatchPublisher._publish_lock`), `active_subs` mutations (`ReactiveWatchlistController._op_lock`), and `SnapshotPoller` cycles (`SnapshotPoller._lock`) with their dedicated locks *before* entering `exec_ctx_scope`.

### 4.2 Blink Table & Subscription Lifecycle Semantics
- **`.latest()` is $O(N_{\text{keys}})$ RAM**: Built by applying `.last_by(key_cols)` directly to the upstream `table_publisher` blink table—never via `blink_to_append_only().last_by()`.
- **Reactive Watchlist Subscribe/Unsubscribe Ordering**:
  - *Subscribe*: Stamp `sub_ts = now_ns()`, insert `(Symbol, sub_ts)` into `active_subs_table` **first**, then enable `Symbol` in `MicroBatchPublisher` (ensuring all subsequent seeded and live rows have `__ReceivedAt >= sub_ts`), and subscribe on the WebSocket.
  - *Unsubscribe*: Remove `Symbol` from `MicroBatchPublisher` **first** (blocking in-flight ticks), then delete `Symbol` from `active_subs_table` (immediately evicting the row from `.latest()` via the `natural_join` + `!isNull(SubscribedAt) && __ReceivedAt >= SubscribedAt` filter), and unsubscribe on the WebSocket.
- **`.poll(mode="replace")` Persistence**: `table_publisher` blink rows vanish after one UG cycle. To keep polled snapshots (like option chains or top movers) continuously populated between poll intervals while evicting dropped keys, `SnapshotPoller` stamps each poll batch with a monotonic `__PollId` and joins `last_by(key_cols)` against the current `__PollId` epoch.

### 4.3 Shared Stream Multiplexing & Reference Counting
- **Publisher Reuse**: `MassiveSession.get_or_create_shared_stream()` caches `(MicroBatchPublisher, finalize_seed)` by `(ws_url, ws_channel, tuple(canon_syms), filter_key)`. Calling `.latest()`, `.history()`, `.ring()`, or `.blink()` on the same feed reuses the same upstream publisher.
- **Client Reference Counting**: `MicroBatchPublisher` tracks downstream views via `retain_client()` and `release_client()`. Calling `.stop()` on a `FeedBuilder` decrements the ref-count and only stops the underlying publisher and unregisters WebSocket topics when the last active view is released.
- **Topic Ref-Counting & Sharding**: `WebSocketMultiplexer` reference-counts `(ws_url, topic)` across publishers and automatically shards connections (`?shard=N`) when `max_topics_per_connection` (default `1,000` for OPRA options) is reached.

### 4.4 Cross-Transport Normalization
- **Unified `PascalCase` Schemas**: Every schema in `src/deephaven_massive/schemas/` normalizes WebSocket single-letter keys (`sym`, `p`, `s`, `t`), REST `snake_case`/`camelCase` payloads, and S3 CSV headers into identical `PascalCase` columns (`Symbol`, `Timestamp`, `Price`, `Size`, etc.).
- **Type Narrowing Helper**: When extracting nested dictionaries in schema normalizers, use [`_submap(row, *keys)`](src/deephaven_massive/schemas/market.py#L57-L60) rather than inline `row.get("k") if isinstance(row.get("k"), Mapping) else {}` so Mypy narrows the return type to `Mapping[str, Any]`.
- **S3 Flat File Streaming**: [`FlatFilesClient`](src/deephaven_massive/domains/flat_files.py) reads `.csv.gz` streams in 16 MB chunks via `pyarrow.csv.open_csv`, applies `Symbol` predicate pushdown (`pa.compute.is_in`) on each chunk immediately, and parses list columns (`Conditions`, `Indicators`) from strings only on surviving rows (since `pyarrow.csv` does not natively parse `pa.list_(pa.int32())`).

---

## 5. Testing & Embedded JVM Gotchas

When writing or debugging tests in `tests/`, follow these rules:

1. **Single Embedded JVM per Process (`dh_exec_ctx`)**:
   - [`tests/conftest.py`](tests/conftest.py) starts a single session-scoped `deephaven_server.Server` on port `10099` with `-DPeriodicUpdateGraph.defaultRefreshIntervalMillis=25`. Never start or stop another `Server` instance in tests.
2. **Do Not Remove `_final_jvm_exit()` in `tests/conftest.py`**:
   - On macOS with Python 3.11+, CPython shutdown after running an embedded JVM/jpy server can segfault (`exit code 139`) during C++ static destructor finalization. `pytest_terminal_summary(trylast=True)` + `atexit.register(_final_jvm_exit)` flushes stdio and calls `os._exit(_EXIT_STATUS)` cleanly *after* `pytest-cov` evaluates `--cov-fail-under=100`.
3. **Never Pass a Live Self-Filtered View to `input_table.delete()`**:
   - When deleting rows from a keyed Deephaven `input_table` in tests (e.g., testing `ReactiveWatchlistController`), **do not** call `wl.delete(wl.where("Symbol == 'MSFT'"))`—passing a live derived view of `wl` back into `wl.delete()` can race and hang against the 25ms `PeriodicUpdateGraph` refresh thread. Always pass a static key table:
     ```python
     wl.delete(arrow_to_deephaven_table(pa.table({"Symbol": ["MSFT"]}), dh_exec_ctx))
     ```
4. **Deterministic Tests Over `time.sleep()`**:
   - Use `mock_session` (which configures `auto_flush=False`, `sleep_fn=lambda _: None`, and records outbound requests in `sess.recorded_requests`), and drive state transitions deterministically via `pub.flush_now()`, `poller.poll_once()`, `mux.inject_raw_message()`, and `wl_ctrl.wait_idle()`.
