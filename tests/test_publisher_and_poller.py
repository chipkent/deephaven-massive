"""Unit and integration tests for MicroBatchPublisher and SnapshotPoller."""

from __future__ import annotations

from typing import Any

import pytest

from deephaven_massive.engine.arrow_bridge import SchemaSpec
from deephaven_massive.engine.poller import SnapshotPoller
from deephaven_massive.engine.publisher import MicroBatchPublisher, _row_timestamp_ns
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.schemas.market import BARS_SCHEMA, QUOTES_SCHEMA, TRADES_SCHEMA
from tests.conftest import dh_to_pydict, dh_to_pylist


def test_row_timestamp_ns_extraction() -> None:
    """Verify _row_timestamp_ns resolves Timestamp, WindowStart, and LastUpdated fields in priority order."""
    assert _row_timestamp_ns({"Timestamp": 1700000000000000000}) == 1700000000000000000
    assert _row_timestamp_ns({"WindowStart": "2025-01-02"}) == 1735776000000000000
    assert _row_timestamp_ns({"LastUpdated": 1700000000000}) == 1700000000000000000
    assert (
        _row_timestamp_ns({"Timestamp": "not-a-timestamp", "WindowStart": 1700000000000000000})
        == 1700000000000000000
    )
    assert _row_timestamp_ns({"Symbol": "AAPL"}) is None


def test_publisher_symbol_filter_mutations(dh_exec_ctx: Any) -> None:
    """Verify MicroBatchPublisher filters rows by allowed_symbols and supports dynamic enable/disable."""
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher(
        name="test_pub_filter",
        schema_spec=TRADES_SCHEMA,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
        flush_interval_ms=20,
        max_batch_size=10,
        allowed_symbols=["aapl"],
    )
    assert pub.allowed_symbols == frozenset({"AAPL"})
    assert "__ReceivedAt" in [c.name for c in pub.blink_table.columns]
    assert "__ReceivedAt" not in [c.name for c in pub.user_blink_table.columns]

    assert pub.enqueue([]) == 0
    assert pub.flush_now() == 0
    assert pub.enqueue([{"sym": "MSFT", "p": 400.0, "s": 10}]) == 0

    pub.enable_symbols(["msft"])
    assert pub.allowed_symbols == frozenset({"AAPL", "MSFT"})
    pub.disable_symbols(["aapl"])
    assert pub.allowed_symbols == frozenset({"MSFT"})

    pub.set_allowed_symbols(None)
    assert pub.allowed_symbols is None
    pub.disable_symbols(["MSFT"])  # no-op when allowed_symbols is None
    pub.enable_symbols(["NVDA", "*"])
    assert pub.allowed_symbols == frozenset({"NVDA", "*"})


def test_publisher_micro_batch_auto_flush_and_background_thread(
    dh_exec_ctx: Any,
) -> None:
    """Verify MicroBatchPublisher flushes on max_batch_size and via the background timer thread."""
    from deephaven.stream import blink_to_append_only

    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher(
        name="test_pub_batch",
        schema_spec=TRADES_SCHEMA,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
        flush_interval_ms=20,
        max_batch_size=3,
    )
    with dh_exec_ctx:
        hist = blink_to_append_only(pub.user_blink_table)

    count = pub.enqueue(
        [
            {"sym": "AAPL", "p": 150.0, "s": 10, "i": "1", "t": 1700000000000000001},
            {"sym": "MSFT", "p": 300.0, "s": 20, "i": "2", "t": 1700000000000000002},
            {"sym": "NVDA", "p": 900.0, "s": 30, "i": "3", "t": 1700000000000000003},
        ]
    )
    assert count == 3
    hist.await_update()
    assert len(dh_to_pylist(hist)) == 3
    assert pub.is_seeded is False
    assert pub.rows_published == 3

    pub.start()
    pub.start()  # idempotent second call while thread is alive
    pub.enqueue(
        [
            {
                "Symbol": "GOOG",
                "Price": 175.0,
                "Size": 5,
                "TradeId": "4",
                "Exchange": 4,
                "Tape": 3,
                "Conditions": [],
                "Correction": 0,
                "SequenceNumber": 10,
                "Timestamp": 1700000000000000004,
                "ParticipantTimestamp": None,
                "TrfId": None,
                "TrfTimestamp": None,
            },
            {
                "Symbol": "GOOG",
                "Price": 176.0,
                "Size": 5,
                "TradeId": "5",
                "Exchange": 4,
                "Tape": 3,
                "Conditions": [],
                "Correction": 0,
                "SequenceNumber": 11,
                "Timestamp": 1700000000000000005,
                "ParticipantTimestamp": None,
                "TrfId": None,
                "TrfTimestamp": None,
            },
            {
                "Symbol": "GOOG",
                "Price": 177.0,
                "Size": 5,
                "TradeId": "6",
                "Exchange": 4,
                "Tape": 3,
                "Conditions": [],
                "Correction": 0,
                "SequenceNumber": 12,
                "Timestamp": 1700000000000000006,
                "ParticipantTimestamp": None,
                "TrfId": None,
                "TrfTimestamp": None,
            },
        ],
        already_normalized=True,
    )
    hist.await_update()
    pub.stop()
    pub.stop()  # idempotent stop when thread is None
    assert len(dh_to_pylist(hist)) == 6
    assert pub.rows_published == 6


def test_publisher_default_exec_ctx_and_unnormalized_symbols(dh_exec_ctx: Any) -> None:
    """Verify MicroBatchPublisher works when exec_ctx=None and symbol_normalizer=None."""
    with dh_exec_ctx:
        pub = MicroBatchPublisher(
            name="test_pub_no_ctx",
            schema_spec=QUOTES_SCHEMA,
            symbol_normalizer=None,
            exec_ctx=None,
            allowed_symbols=["aapl"],
        )
        _ = pub.user_blink_table
        pub.enqueue([{"sym": "AAPL", "bp": 149.9, "ap": 150.1, "t": 1700000000000000000}])
        assert pub.flush_now() == 1


def test_publisher_seed_cutover_trade_id_and_timestamp_deduplication(
    dh_exec_ctx: Any,
) -> None:
    """Verify begin_seeding/complete_seeding deduplicates buffered WebSocket trades against REST seed rows."""
    from deephaven.stream import blink_to_append_only

    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher(
        name="test_pub_seed",
        schema_spec=TRADES_SCHEMA,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    with dh_exec_ctx:
        hist = blink_to_append_only(pub.user_blink_table)

    pub.begin_seeding()
    assert pub.flush_now() == 0

    pub.enqueue(
        [
            {"sym": "AAPL", "p": 150.0, "s": 10, "i": "T100", "t": 2000000000000000000},
            {"sym": "AAPL", "p": 149.0, "s": 10, "i": "T099", "t": 1000000000000000000},
            {"sym": "AAPL", "p": 151.0, "s": 25, "i": "T102", "t": 3000000000000000000},
        ]
    )

    flushed = pub.complete_seeding(
        [
            {"sym": "AAPL", "p": 149.5, "s": 5, "i": "T098", "t": 1500000000000000000},
            {"sym": "AAPL", "p": 150.0, "s": 10, "i": "T100", "t": 2000000000000000000},
            {"sym": "AAPL", "p": 148.0, "s": 5, "i": "T097", "t": 1200000000000000000},
            {"sym": "MSFT", "p": 300.0, "s": 10},
        ]
    )
    assert flushed == 5
    hist.await_update()
    rows = dh_to_pylist(hist)
    trade_ids = [r["TradeId"] for r in rows if r["Symbol"] == "AAPL"]
    assert trade_ids == ["T098", "T100", "T097", "T102"]


def test_publisher_seed_since_cutoff_filtering(dh_exec_ctx: Any) -> None:
    """Verify complete_seeding(seed_since=...) drops seed and WebSocket rows older than the cutoff."""
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher(
        name="test_pub_seed_since",
        schema_spec=TRADES_SCHEMA,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    pub.begin_seeding()
    pub.enqueue(
        [
            {"sym": "NVDA", "p": 890.0, "s": 5, "i": "N0", "t": 1000000000000000000},
            {"sym": "NVDA", "p": 910.0, "s": 5, "i": "N2", "t": 3000000000000000000},
        ]
    )
    flushed_since = pub.complete_seeding(
        [
            {"sym": "NVDA", "p": 880.0, "s": 5, "i": "N_OLD", "t": 1500000000000000000},
            {"sym": "NVDA", "p": 900.0, "s": 5, "i": "N1", "t": 2500000000000000000},
        ],
        seed_since=2000000000000000000,
    )
    assert flushed_since == 2

    # Late snapshot seeding skips rows older than observed live ticks and flushes newer ones
    assert (
        pub.seed_late_snapshot(
            [{"sym": "NVDA", "p": 850.0, "s": 5, "i": "N_STALE", "t": 1000000000000000000}],
            auto_flush=True,
        )
        == 0
    )
    assert (
        pub.seed_late_snapshot(
            [{"sym": "NVDA", "p": 920.0, "s": 5, "i": "N_NEW", "t": 4000000000000000000}],
            auto_flush=True,
        )
        == 1
    )

    # Historical backfill filtering skips rows older than seed_since or > cutoff_by_symbol
    backfill = pub.filter_historical_backfill(
        [
            {"sym": "NVDA", "p": 800.0, "s": 5, "i": "B_TOO_OLD", "t": 1000000000000000000},
            {"sym": "NVDA", "p": 870.0, "s": 5, "i": "B_OK", "t": 2000000000000000000},
            {"sym": "NVDA", "p": 905.0, "s": 5, "i": "B_MID", "t": 2600000000000000000},
            {"sym": "NVDA", "p": 930.0, "s": 5, "i": "B_AFTER_CUTOFF", "t": 4500000000000000000},
        ],
        seed_since=1500000000000000000,
    )
    assert [r["TradeId"] for r in backfill] == ["B_OK", "B_MID"]


def test_publisher_seed_cutover_bars_quotes_and_keyless_schemas(
    dh_exec_ctx: Any,
) -> None:
    """Verify seed cutover deduplication across BARS_SCHEMA, QUOTES_SCHEMA, and keyless schemas."""
    norm = SymbolNormalizer("stocks")
    bar_pub = MicroBatchPublisher(
        name="test_bar_seed",
        schema_spec=BARS_SCHEMA,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    bar_pub.begin_seeding()
    bar_pub.enqueue(
        [
            {"sym": "AAPL", "o": 150.0, "c": 151.0, "s": 2000000000000},
            {"sym": "MSFT", "o": 300.0, "c": 301.0, "s": 2000000000000},
            {"sym": "AAPL", "o": 151.0, "c": 152.0, "s": 2060000000000},
        ]
    )
    assert (
        bar_pub.complete_seeding([{"sym": "AAPL", "o": 150.0, "c": 151.0, "s": 2000000000000}]) == 3
    )
    assert bar_pub.is_seeded is True

    q_pub = MicroBatchPublisher(
        name="test_quote_seed",
        schema_spec=QUOTES_SCHEMA,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    q_pub.begin_seeding()
    q_pub.enqueue(
        [
            {
                "sym": "AAPL",
                "bp": 150.0,
                "ap": 150.1,
                "q": 10,
                "t": 2000000000000000000,
            },
            {
                "sym": "AAPL",
                "bp": 150.1,
                "ap": 150.2,
                "q": 11,
                "t": 2000000000000000000,
            },
        ]
    )
    assert (
        q_pub.complete_seeding(
            [
                {
                    "sym": "AAPL",
                    "bp": 150.0,
                    "ap": 150.1,
                    "q": 10,
                    "t": 2000000000000000000,
                }
            ]
        )
        == 2
    )

    keyless_spec = SchemaSpec(
        name="keyless_bars",
        arrow_schema=BARS_SCHEMA.arrow_schema,
        normalizer=BARS_SCHEMA.normalizer,
        key_cols=(),
    )
    keyless_pub = MicroBatchPublisher(
        name="test_keyless_seed",
        schema_spec=keyless_spec,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    keyless_pub.begin_seeding()
    keyless_pub.enqueue([{"sym": "AAPL", "o": 151.0, "c": 152.0, "s": 2060000000000}])
    assert (
        keyless_pub.complete_seeding([{"sym": "AAPL", "o": 150.0, "c": 151.0, "s": 2000000000000}])
        == 2
    )


def test_snapshot_poller_validation_errors(dh_exec_ctx: Any) -> None:
    """Verify SnapshotPoller rejects invalid poll modes and non-positive intervals."""
    with pytest.raises(ValueError, match="Unsupported poll mode"):
        SnapshotPoller(
            "bad_mode",
            QUOTES_SCHEMA,
            list,
            mode="invalid",
            exec_ctx=dh_exec_ctx,
        )

    with pytest.raises(ValueError, match="interval_s must be > 0"):
        SnapshotPoller(
            "bad_interval",
            QUOTES_SCHEMA,
            list,
            interval_s=0,
            exec_ctx=dh_exec_ctx,
        )


def test_snapshot_poller_replace_mode_evicts_stale_keys(dh_exec_ctx: Any) -> None:
    """Verify SnapshotPoller in replace mode evicts keys missing from the latest poll cycle."""
    norm = SymbolNormalizer("stocks")
    batches: list[list[dict[str, Any]]] = [
        [
            {"sym": "AAPL", "bp": 150.0, "ap": 150.1},
            {"sym": "MSFT", "bp": 300.0, "ap": 300.2},
        ],
        [
            {"sym": "MSFT", "bp": 305.0, "ap": 305.2},
            {"sym": "NVDA", "bp": 900.0, "ap": 900.5},
        ],
        [],
    ]
    idx = 0

    def _fetch() -> list[dict[str, Any]]:
        nonlocal idx
        res = batches[min(idx, len(batches) - 1)]
        idx += 1
        return res

    poller = SnapshotPoller(
        "poll_replace",
        QUOTES_SCHEMA,
        _fetch,
        interval_s=10.0,
        mode="replace",
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    assert poller.poll_once() == 2
    assert poller.poll_id == 1
    poller.table.await_update()
    assert set(dh_to_pydict(poller.table)["Symbol"]) == {"AAPL", "MSFT"}

    assert poller.poll_once() == 2
    assert poller.poll_id == 2
    poller.table.await_update()
    d2 = dh_to_pydict(poller.table)
    assert set(d2["Symbol"]) == {"MSFT", "NVDA"}
    msft_idx = d2["Symbol"].index("MSFT")
    assert d2["BidPrice"][msft_idx] == pytest.approx(305.0)

    assert poller.poll_once() == 0
    assert poller.poll_id == 3


def test_snapshot_poller_upsert_mode_retains_prior_keys(dh_exec_ctx: Any) -> None:
    """Verify SnapshotPoller in upsert mode updates matching keys while retaining prior keys."""
    norm = SymbolNormalizer("stocks")
    batches = [
        [
            {"sym": "AAPL", "bp": 150.0, "ap": 150.1},
            {"sym": "MSFT", "bp": 300.0, "ap": 300.2},
        ],
        [
            {"sym": "MSFT", "bp": 305.0, "ap": 305.2},
            {"sym": "NVDA", "bp": 900.0, "ap": 900.5},
        ],
    ]
    idx = 0

    def _fetch() -> list[dict[str, Any]]:
        nonlocal idx
        res = batches[min(idx, 1)]
        idx += 1
        return res

    poller = SnapshotPoller(
        "poll_upsert",
        QUOTES_SCHEMA,
        _fetch,
        interval_s=10.0,
        mode="upsert",
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    poller.poll_once()
    poller.table.await_update()
    poller.poll_once()
    poller.table.await_update()
    assert set(dh_to_pydict(poller.table)["Symbol"]) == {"AAPL", "MSFT", "NVDA"}


def test_snapshot_poller_append_mode_and_background_thread(dh_exec_ctx: Any) -> None:
    """Verify SnapshotPoller in append mode accumulates every poll batch on a background thread."""
    norm = SymbolNormalizer("stocks")
    batches = [
        [
            {"sym": "AAPL", "bp": 150.0, "ap": 150.1},
            {"sym": "MSFT", "bp": 300.0, "ap": 300.2},
        ],
        [
            {"sym": "MSFT", "bp": 305.0, "ap": 305.2},
            {"sym": "NVDA", "bp": 900.0, "ap": 900.5},
        ],
    ]
    idx = 0

    def _fetch() -> list[dict[str, Any]]:
        nonlocal idx
        res = batches[min(idx, 1)]
        idx += 1
        return res

    poller = SnapshotPoller(
        "poll_append",
        QUOTES_SCHEMA,
        _fetch,
        interval_s=0.03,
        mode="append",
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    poller.start(initial_poll=True)
    poller.start(initial_poll=False)  # idempotent while thread is alive
    poller.table.await_update()
    poller.table.await_update()
    poller.stop()
    poller.stop()
    assert len(dh_to_pydict(poller.table)["Symbol"]) >= 4


def test_snapshot_poller_keyless_and_default_exec_ctx(dh_exec_ctx: Any) -> None:
    """Verify SnapshotPoller supports key_cols=(), dynamic columns, background error capture, and exec_ctx=None."""
    from deephaven_massive.schemas.reference import FINANCIAL_STATEMENT_SCHEMA

    norm = SymbolNormalizer("stocks")
    with dh_exec_ctx:
        poller = SnapshotPoller(
            "poll_no_ctx",
            QUOTES_SCHEMA,
            lambda: [{"sym": "AAPL", "bp": 150.0, "ap": 150.1}],
            interval_s=10.0,
            mode="upsert",
            key_cols=(),
            symbol_normalizer=norm,
            exec_ctx=None,
        )
        assert poller.poll_once() == 1
        assert poller.rows_published == 1
        assert poller.last_error is None

    # Dynamic column schema prefetching and empty upsert poll
    dyn_poller = SnapshotPoller(
        "poll_dyn",
        FINANCIAL_STATEMENT_SCHEMA,
        lambda: [{"tickers": ["AAPL"], "filing_date": "2025-01-15", "revenues": {"value": 100.0}}],
        interval_s=10.0,
        mode="upsert",
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    assert dyn_poller.poll_once() == 1
    dyn_poller.fetch_fn = list
    assert dyn_poller.poll_once() == 0

    # Background error capture in poller and publisher
    def _boom_fetch() -> list[dict[str, Any]]:
        raise RuntimeError("poll boom")

    err_poller = SnapshotPoller(
        "poll_err",
        QUOTES_SCHEMA,
        _boom_fetch,
        interval_s=0.01,
        mode="append",
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    err_poller.start(initial_poll=False)
    for _ in range(100):
        if err_poller.last_error is not None:
            break
        err_poller._stop_event.wait(0.01)
    err_poller.stop()
    assert isinstance(err_poller.last_error, RuntimeError)

    err_pub = MicroBatchPublisher(
        name="err_pub",
        schema_spec=QUOTES_SCHEMA,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
        flush_interval_ms=10,
    )

    def _boom_flush() -> int:
        raise RuntimeError("boom")

    assert err_pub.last_error is None
    err_pub.flush_now = _boom_flush  # type: ignore[method-assign]
    err_pub.start()
    for _ in range(100):
        if err_pub.last_error is not None:
            break
        err_pub._stop_event.wait(0.01)
    err_pub._stop_event.set()
    err_pub._flush_wakeup.set()
    if err_pub._flush_thread is not None:
        err_pub._flush_thread.join(timeout=2.0)
    assert isinstance(err_pub.last_error, RuntimeError)
