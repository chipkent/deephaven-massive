"""Unit and integration tests for MicroBatchPublisher and SnapshotPoller."""

from __future__ import annotations

import time
from typing import Any

import pytest

from deephaven_massive.engine.arrow_bridge import SchemaSpec
from deephaven_massive.engine.poller import SnapshotPoller
from deephaven_massive.engine.publisher import MicroBatchPublisher, _row_timestamp_ns
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.schemas.market import BARS_SCHEMA, QUOTES_SCHEMA, TRADES_SCHEMA
from tests.conftest import dh_to_pydict, dh_to_pylist


def test_row_timestamp_ns_variants() -> None:
    assert _row_timestamp_ns({"Timestamp": 1700000000000000000}) == 1700000000000000000
    assert _row_timestamp_ns({"WindowStart": "2025-01-02"}) is not None
    assert _row_timestamp_ns({"LastUpdated": 1700000000000}) == 1700000000000000000
    assert (
        _row_timestamp_ns({"Timestamp": "not-a-timestamp", "WindowStart": 1700000000000000000})
        == 1700000000000000000
    )
    assert _row_timestamp_ns({"Symbol": "AAPL"}) is None


def test_publisher_symbol_filtering_and_flushing(dh_exec_ctx: Any) -> None:
    norm = SymbolNormalizer("stocks")
    pub = MicroBatchPublisher(
        name="test_pub_filter",
        schema_spec=TRADES_SCHEMA,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
        flush_interval_ms=20,
        max_batch_size=3,
        allowed_symbols=["aapl"],
    )
    assert pub.allowed_symbols == frozenset({"AAPL"})
    assert "__ReceivedAt" in [c.name for c in pub.blink_table.columns]
    assert "__ReceivedAt" not in [c.name for c in pub.user_blink_table.columns]

    # Empty enqueue and empty flush
    assert pub.enqueue([]) == 0
    assert pub.flush_now() == 0

    # Filtered out symbol
    assert pub.enqueue([{"sym": "MSFT", "p": 400.0, "s": 10}]) == 0

    # Enable MSFT, disable AAPL
    pub.enable_symbols(["msft"])
    assert pub.allowed_symbols == frozenset({"AAPL", "MSFT"})
    pub.disable_symbols(["aapl"])
    assert pub.allowed_symbols == frozenset({"MSFT"})

    # Reset filter to None, then enable from None
    pub.set_allowed_symbols(None)
    assert pub.allowed_symbols is None
    pub.disable_symbols(["MSFT"])  # no-op when None
    pub.enable_symbols(["NVDA", "*"])
    assert pub.allowed_symbols == frozenset({"NVDA", "*"})

    # Enqueue 3 rows to trigger max_batch_size=3 immediate flush
    from deephaven.stream import blink_to_append_only

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
    rows = dh_to_pylist(hist)
    assert len(rows) == 3

    # Test already_normalized=True and background flush thread
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
            }
        ],
        already_normalized=True,
    )
    hist.await_update()
    pub.stop()
    pub.stop()  # idempotent stop when thread is None
    assert len(dh_to_pylist(hist)) == 4


def test_publisher_without_explicit_exec_ctx_or_normalizer(dh_exec_ctx: Any) -> None:
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


def test_publisher_seed_cutover_deduplication(dh_exec_ctx: Any) -> None:
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
    # Enqueue WS events while seeding is active:
    # 1. Duplicate TradeId ("T100") -> should be dropped
    # 2. Older timestamp than seed (t=1000 < seed t=2000) -> should be dropped
    # 3. Newer timestamp and distinct TradeId ("T102", t=3000) -> should be kept
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
            {"sym": "MSFT", "p": 300.0, "s": 10},  # No timestamp on seed row
        ]
    )
    assert flushed == 5  # 4 seed rows + 1 surviving WS row ("T102")
    hist.await_update()
    rows = dh_to_pylist(hist)
    trade_ids = [r["TradeId"] for r in rows if r["Symbol"] == "AAPL"]
    assert trade_ids == ["T098", "T100", "T097", "T102"]

    # Also test key_cols deduplication on BARS_SCHEMA (no TradeId column) and keyless schema
    bar_pub = MicroBatchPublisher(
        name="test_bar_seed",
        schema_spec=BARS_SCHEMA,
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    bar_pub.begin_seeding()
    bar_pub.enqueue(
        [
            # Same (Symbol, WindowStart) and ws_ts <= max_seed_ts -> dropped by key_cols check
            {"sym": "AAPL", "o": 150.0, "c": 151.0, "s": 2000000000000},
            # Newer bar -> kept
            {"sym": "AAPL", "o": 151.0, "c": 152.0, "s": 2060000000000},
        ]
    )
    assert (
        bar_pub.complete_seeding(
            [{"sym": "AAPL", "o": 150.0, "c": 151.0, "s": 2000000000000}]
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
        keyless_pub.complete_seeding(
            [{"sym": "AAPL", "o": 150.0, "c": 151.0, "s": 2000000000000}]
        )
        == 2
    )


def test_snapshot_poller_modes_and_validation(dh_exec_ctx: Any) -> None:
    norm = SymbolNormalizer("stocks")

    with pytest.raises(ValueError, match="Unsupported poll mode"):
        SnapshotPoller(
            "bad_mode",
            QUOTES_SCHEMA,
            lambda: [],
            mode="invalid",
            exec_ctx=dh_exec_ctx,
        )

    with pytest.raises(ValueError, match="interval_s must be > 0"):
        SnapshotPoller(
            "bad_interval",
            QUOTES_SCHEMA,
            lambda: [],
            interval_s=0,
            exec_ctx=dh_exec_ctx,
        )

    # 1. Test mode="replace" (evicts stale keys when PollId advances)
    batches = [
        [
            {"sym": "AAPL", "bp": 150.0, "ap": 150.1},
            {"sym": "MSFT", "bp": 300.0, "ap": 300.2},
        ],
        [
            {"sym": "MSFT", "bp": 305.0, "ap": 305.2},
            {"sym": "NVDA", "bp": 900.0, "ap": 900.5},
        ],
        [],  # Empty poll batch
    ]
    idx = 0

    def _fetch_replace() -> list[dict[str, Any]]:
        nonlocal idx
        res = batches[min(idx, len(batches) - 1)]
        idx += 1
        return res

    replace_poller = SnapshotPoller(
        "poll_replace",
        QUOTES_SCHEMA,
        _fetch_replace,
        interval_s=10.0,
        mode="replace",
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    assert replace_poller.poll_once() == 2
    assert replace_poller.poll_id == 1
    replace_poller.table.await_update()
    d1 = dh_to_pydict(replace_poller.table)
    assert set(d1["Symbol"]) == {"AAPL", "MSFT"}

    assert replace_poller.poll_once() == 2
    assert replace_poller.poll_id == 2
    replace_poller.table.await_update()
    d2 = dh_to_pydict(replace_poller.table)
    # AAPL was not in poll 2, so mode="replace" evicted it!
    assert set(d2["Symbol"]) == {"MSFT", "NVDA"}
    msft_idx = d2["Symbol"].index("MSFT")
    assert d2["BidPrice"][msft_idx] == pytest.approx(305.0)

    # Poll 3 returns empty list -> returns 0
    assert replace_poller.poll_once() == 0
    assert replace_poller.poll_id == 3

    # 2. Test mode="upsert" (retains AAPL from poll 1 alongside MSFT and NVDA from poll 2)
    idx_u = 0

    def _fetch_upsert() -> list[dict[str, Any]]:
        nonlocal idx_u
        res = batches[min(idx_u, 1)]
        idx_u += 1
        return res

    upsert_poller = SnapshotPoller(
        "poll_upsert",
        QUOTES_SCHEMA,
        _fetch_upsert,
        interval_s=10.0,
        mode="upsert",
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    upsert_poller.poll_once()
    upsert_poller.table.await_update()
    upsert_poller.poll_once()
    upsert_poller.table.await_update()
    du = dh_to_pydict(upsert_poller.table)
    assert set(du["Symbol"]) == {"AAPL", "MSFT", "NVDA"}

    # 3. Test mode="append" (appends all rows across polls) + background start/stop
    idx_a = 0

    def _fetch_append() -> list[dict[str, Any]]:
        nonlocal idx_a
        res = batches[min(idx_a, 1)]
        idx_a += 1
        return res

    append_poller = SnapshotPoller(
        "poll_append",
        QUOTES_SCHEMA,
        _fetch_append,
        interval_s=0.03,
        mode="append",
        symbol_normalizer=norm,
        exec_ctx=dh_exec_ctx,
    )
    append_poller.start(initial_poll=True)
    append_poller.start(initial_poll=False)  # idempotent while thread is alive
    append_poller.table.await_update()
    append_poller.table.await_update()
    append_poller.stop()
    append_poller.stop()
    da = dh_to_pydict(append_poller.table)
    assert len(da["Symbol"]) >= 4

    # 4. Test SnapshotPoller with exec_ctx=None and key_cols=()
    with dh_exec_ctx:
        no_ctx_poller = SnapshotPoller(
            "poll_no_ctx",
            QUOTES_SCHEMA,
            lambda: [{"sym": "AAPL", "bp": 150.0, "ap": 150.1}],
            interval_s=10.0,
            mode="upsert",
            key_cols=(),
            symbol_normalizer=norm,
            exec_ctx=None,
        )
        assert no_ctx_poller.poll_once() == 1
