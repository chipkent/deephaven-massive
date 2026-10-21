"""Unit and integration tests for FeedBuilder and BaseDomainClient."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from typing import Any

import pyarrow as pa
import pytest

from deephaven_massive.domains.base import (
    BaseDomainClient,
    _extract_rows_from_payload,
    _format_date_param,
    _normalize_filter_params,
)
from deephaven_massive.engine.arrow_bridge import (
    UTC_NS_TYPE,
    SchemaSpec,
    arrow_to_deephaven_table,
    exec_ctx_scope,
)
from deephaven_massive.engine.builder import FeedBuilder, _is_deephaven_table
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.schemas.economy import TREASURY_YIELDS_SCHEMA
from deephaven_massive.schemas.market import TRADES_SCHEMA
from tests.conftest import dh_to_pylist


def test_format_date_param_and_extract_rows_from_payload() -> None:
    """Verify _format_date_param and _extract_rows_from_payload normalize all Massive REST envelope shapes."""
    assert _format_date_param(None) is None
    assert _format_date_param(date(2025, 1, 2)) == "2025-01-02"
    dt_str = _format_date_param(datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc))
    assert dt_str is not None and dt_str.startswith("2025-01-02T12:00:00")
    assert _format_date_param(1700000000000) == "1700000000000"

    assert _extract_rows_from_payload(None) == []
    assert _extract_rows_from_payload("not-a-dict") == []
    assert _extract_rows_from_payload([{"a": 1}, "not-a-dict"]) == [{"a": 1}]
    assert _extract_rows_from_payload({"results": 123}) == []
    assert _extract_rows_from_payload({"results": {"values": [{"value": 50.0}]}}) == [
        {"value": 50.0}
    ]
    assert _extract_rows_from_payload({"results": {"ticker": "AAPL"}}) == [{"ticker": "AAPL"}]
    assert _extract_rows_from_payload({"tickers": [{"ticker": "MSFT"}]}) == [{"ticker": "MSFT"}]
    assert _extract_rows_from_payload({"last": {"ask": 1.1}, "symbol": "EUR/USD"}) == [
        {"ask": 1.1, "symbol": "EUR/USD"}
    ]
    assert _extract_rows_from_payload({"last": {"ask": 1.1}}) == [{"ask": 1.1}]
    assert _extract_rows_from_payload({"ticker": {"ticker": "NVDA"}}) == [{"ticker": "NVDA"}]
    assert _extract_rows_from_payload({"status": "OK"}) == []
    assert _extract_rows_from_payload(
        {"status": "open", "serverTime": "2025-01-02T14:30:00Z"},
        inject_symbol="AAPL",
        extra_fields={"Tag": "v1"},
    ) == [
        {
            "status": "open",
            "serverTime": "2025-01-02T14:30:00Z",
            "Symbol": "AAPL",
            "Tag": "v1",
        }
    ]


def test_normalize_filter_params_operators_and_tickers() -> None:
    """Verify _normalize_filter_params translates operator suffixes and multi-ticker filters."""
    assert _normalize_filter_params(
        {"d": date(2025, 1, 2), "keep": 1, "skip": None},
        {
            "date_gte": date(2025, 1, 1),
            "strike_lte": 150.0,
            "tags_any_of": ["tech", "ai"],
            "none_val": None,
            "plain": "x",
        },
        tickers="aapl",
        normalizer=None,
    ) == {
        "d": "2025-01-02",
        "keep": 1,
        "ticker": "AAPL",
        "date.gte": "2025-01-01",
        "strike.lte": 150.0,
        "tags.any_of": "tech,ai",
        "plain": "x",
    }
    assert _normalize_filter_params(
        None,
        None,
        tickers=["aapl", "msft"],
        ticker_param="ticker",
        normalizer=SymbolNormalizer("stocks"),
    ) == {"ticker.any_of": "AAPL,MSFT"}
    assert _normalize_filter_params(
        None,
        None,
        tickers=["spy", "qqq"],
        ticker_param="composite_ticker",
    ) == {"composite_ticker": "SPY,QQQ"}
    assert (
        _normalize_filter_params(
            None,
            None,
            symbol="aapl",
            tickers=[],
        )
        == {}
    )


def test_base_domain_client_rest_pagination_and_chunked_tables(
    mock_session: Any,
) -> None:
    """Verify _fetch_rest_rows and _fetch_chunked_table follow next_url cursors and enforce row limits."""
    pages = [
        {
            "results": [
                {"ticker": "AAPL", "p": 150.0, "s": 10},
                {"ticker": "AAPL", "p": 151.0, "s": 20},
            ],
            "next_url": "https://api.massive.com/v3/trades/AAPL?cursor=page2",
        },
        {
            "results": [
                {"ticker": "AAPL", "p": 152.0, "s": 30},
                {"ticker": "AAPL", "p": 153.0, "s": 40},
            ],
        },
    ]
    call_idx = 0

    def _http(
        url: str, params: Mapping[str, Any] | None, headers: Mapping[str, str]
    ) -> tuple[int, Any]:
        nonlocal call_idx
        res = pages[min(call_idx, len(pages) - 1)]
        call_idx += 1
        return 200, res

    sess = mock_session(http_handler=_http)
    base = BaseDomainClient(sess, domain=None)

    assert base._canonical_symbols(None) == ["*"]
    assert base._canonical_symbols("aapl") == ["AAPL"]
    assert base._canonical_symbols(["aapl", "msft"]) == ["AAPL", "MSFT"]

    wl_pa = pa.table({"Symbol": ["aapl", "msft"]})
    wl_dh = arrow_to_deephaven_table(wl_pa, sess.exec_ctx)
    assert base._canonical_symbols(wl_dh) == ["AAPL", "MSFT"]
    empty_wl_dh = arrow_to_deephaven_table(
        pa.table({"Symbol": pa.array([], type=pa.string())}), sess.exec_ctx
    )
    assert base._canonical_symbols(empty_wl_dh) == []
    with pytest.raises(ValueError, match="Specific symbol"):
        base._fetch_multi_symbol_historical(
            empty_wl_dh,
            TRADES_SCHEMA,
            None,
            lambda sym, s, e: (f"/v3/trades/{sym}", {}),
            source="rest",
        )

    rows = base._fetch_rest_rows("/v3/trades/AAPL", {"foo": "bar"}, limit=3)
    assert len(rows) == 3

    call_idx = 0
    all_rows = base._fetch_rest_rows("/v3/trades/AAPL")
    assert len(all_rows) == 4

    call_idx = 0
    tbl = base._fetch_chunked_table(
        "/v3/trades/AAPL",
        TRADES_SCHEMA,
        {"foo": "bar"},
        limit=3,
        chunk_size=2,
    )
    assert len(dh_to_pylist(tbl)) == 3

    call_idx = 0
    tbl_all = base._fetch_chunked_table("/v3/trades/AAPL", TRADES_SCHEMA, chunk_size=2)
    assert len(dh_to_pylist(tbl_all)) == 4


def test_base_domain_client_multi_symbol_historical_limit_and_parallel(
    mock_session: Any,
) -> None:
    """Verify _fetch_multi_symbol_historical enforces global limits sequentially and parallelizes unlimited queries."""
    pages = [
        {
            "results": [
                {"ticker": "AAPL", "p": 150.0, "s": 10},
                {"ticker": "AAPL", "p": 151.0, "s": 20},
            ],
        }
    ]
    call_idx = 0

    def _http(
        url: str, params: Mapping[str, Any] | None, headers: Mapping[str, str]
    ) -> tuple[int, Any]:
        nonlocal call_idx
        call_idx += 1
        return 200, pages[0]

    sess = mock_session(http_handler=_http)
    base = BaseDomainClient(sess, domain="stocks")

    tbl_multi_exact = base._fetch_multi_symbol_historical(
        ["AAPL", "MSFT", "NVDA"],
        TRADES_SCHEMA,
        None,
        lambda sym, s, e: (f"/v3/trades/{sym}", {}),
        limit=2,
        source="rest",
        chunk_size=1,
    )
    assert len(dh_to_pylist(tbl_multi_exact)) == 2
    assert call_idx == 1

    call_idx = 0
    tbl_multi_large_limit = base._fetch_multi_symbol_historical(
        ["AAPL", "MSFT"],
        TRADES_SCHEMA,
        None,
        lambda sym, s, e: (f"/v3/trades/{sym}", {}),
        limit=100,
        source="rest",
        chunk_size=10,
    )
    assert len(dh_to_pylist(tbl_multi_large_limit)) == 4
    assert call_idx == 2

    call_idx = 0
    tbl_multi_parallel = base._fetch_multi_symbol_historical(
        ["AAPL", "MSFT"],
        TRADES_SCHEMA,
        None,
        lambda sym, s, e: (f"/v3/trades/{sym}", {}),
        limit=None,
        source="rest",
        chunk_size=1,
    )
    assert len(dh_to_pylist(tbl_multi_parallel)) == 4
    assert call_idx == 2


def test_base_domain_client_should_use_flat_files_routing(mock_session: Any) -> None:
    """Verify _should_use_flat_files selects REST or S3 based on source mode, threshold, adjusted bars, and credentials."""
    sess = mock_session()
    base = BaseDomainClient(sess, domain="stocks")

    with pytest.raises(ValueError, match="Unsupported historical source"):
        base._should_use_flat_files("invalid", ["AAPL"], "us_stocks_sip/trades_v1")

    assert base._should_use_flat_files("rest", ["*"], "us_stocks_sip/trades_v1") is False

    with pytest.raises(ValueError, match="not supported for this feed"):
        base._should_use_flat_files("flat_files", ["AAPL"], None)

    assert base._should_use_flat_files("flat_files", ["AAPL"], "us_stocks_sip/trades_v1") is True
    assert base._should_use_flat_files("s3", ["AAPL"], "us_stocks_sip/trades_v1") is True
    with pytest.raises(ValueError, match="unadjusted bars"):
        base._should_use_flat_files(
            "s3",
            ["AAPL"],
            "us_stocks_sip/day_aggs_v1",
            adjusted=True,
        )
    assert base._should_use_flat_files("auto", ["*"], "us_stocks_sip/trades_v1") is True
    assert (
        base._should_use_flat_files("auto", ["AAPL", "MSFT", "NVDA"], "us_stocks_sip/trades_v1")
        is True
    )
    assert (
        base._should_use_flat_files(
            "auto",
            ["AAPL", "MSFT", "NVDA"],
            "us_stocks_sip/day_aggs_v1",
            adjusted=True,
        )
        is False
    )
    assert base._should_use_flat_files("auto", ["AAPL"], "us_stocks_sip/trades_v1") is False

    sess_no_s3 = mock_session(s3_access_key_id=None, s3_secret_access_key=None)
    base_no_s3 = BaseDomainClient(sess_no_s3, domain="stocks")
    assert base_no_s3._canonical_symbols("aapl") == ["AAPL"]
    assert base_no_s3._should_use_flat_files("auto", ["*"], "us_stocks_sip/trades_v1") is False
    with pytest.raises(ValueError, match="S3 credentials"):
        base_no_s3._should_use_flat_files("flat_files", ["AAPL"], "us_stocks_sip/trades_v1")


def test_feed_builder_capability_guards_and_validation(dh_exec_ctx: Any) -> None:
    """Verify _is_deephaven_table detection and NotImplementedError/ValueError guards on unsupported terminals."""
    from deephaven import dtypes as dht
    from deephaven import input_table

    assert _is_deephaven_table(None) is False
    assert _is_deephaven_table("AAPL") is False
    assert _is_deephaven_table(["AAPL"]) is False
    assert _is_deephaven_table(object()) is False

    with exec_ctx_scope(dh_exec_ctx):
        wl_tbl = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])
    assert _is_deephaven_table(wl_tbl) is True

    rest_only = FeedBuilder(
        name="rest_only_feed",
        schema_spec=TREASURY_YIELDS_SCHEMA,
        exec_ctx=dh_exec_ctx,
    )
    with pytest.raises(NotImplementedError, match="does not support WebSocket streaming"):
        rest_only.latest()
    with pytest.raises(NotImplementedError, match=r"does not support \.snapshot\(\)"):
        rest_only.snapshot()
    with pytest.raises(NotImplementedError, match=r"does not support \.poll\(\)"):
        rest_only.poll()
    with pytest.raises(NotImplementedError, match=r"does not support \.historical"):
        rest_only.historical(start="2025-01-01")
    with pytest.raises(ValueError, match="capacity must be > 0"):
        rest_only.ring(0)


def test_feed_builder_delivery_modes_and_fallback_seeding(
    dh_exec_ctx: Any, mock_session: Any
) -> None:
    """Verify FeedBuilder supports .latest(), .history(), .ring(), .blink(), .snapshot(), .poll(), and seed_fn fallbacks."""
    sess = mock_session(auto_flush=False)
    with exec_ctx_scope(dh_exec_ctx):
        fb_raw = FeedBuilder(
            name="raw_builder",
            schema_spec=TREASURY_YIELDS_SCHEMA,
            symbol_normalizer=None,
            exec_ctx=None,
            ws_multiplexer=sess.ws_multiplexer,
            ws_url="wss://socket.massive.com/stocks",
            ws_channel="T",
            symbols="aapl",
            seed_fn=lambda syms: [{"date": "2025-01-02", "yield_10_year": 4.25}],
            auto_flush=False,
        )
        assert fb_raw._canonical_symbol_list() == ["AAPL"]
        fb_raw.symbols = ["aapl", "msft"]
        assert fb_raw._canonical_symbol_list() == ["AAPL", "MSFT"]
        fb_raw.symbols = None
        assert fb_raw._canonical_symbol_list() == ["*"]

        latest_raw = fb_raw.latest(key_cols=(), seed=True)
        assert latest_raw is not None
        assert fb_raw.publisher is not None and fb_raw.publisher.rows_published == 1

        latest_by = fb_raw.latest(by=["Date"], seed=False)
        assert latest_by is not None

        hist_raw = fb_raw.history(seed_since="2025-01-01", seed=True)
        ring_raw = fb_raw.ring(capacity=10, seed=False)
        blink_raw = fb_raw.blink()
        assert hist_raw is not None and ring_raw is not None and blink_raw is not None

        snap_raw = fb_raw.historical()
        assert len(dh_to_pylist(snap_raw)) == 1
        _ = fb_raw.poll(interval_s=10.0)
        assert fb_raw.poller is not None
        assert fb_raw.publisher is not None
        fb_raw.stop()

    fb_empty = FeedBuilder(
        name="empty_snap",
        schema_spec=TRADES_SCHEMA,
        exec_ctx=dh_exec_ctx,
        snapshot_fn=list,
    )
    assert len(dh_to_pylist(fb_empty.snapshot())) == 0

    ring_ctx = sess.stocks.trades("AAPL").ring(capacity=5, seed=False)
    assert ring_ctx is not None
    poll_fb = sess.stocks.snapshots("AAPL")
    poll_ctx = poll_fb.poll(interval_s=10.0)
    assert poll_ctx is not None
    poll_ctx2 = poll_fb.poll(interval_s=10.0)
    assert poll_ctx2 is not None
    poll_fb.stop()


def test_feed_builder_reactive_watchlist_delivery_modes(
    dh_exec_ctx: Any, mock_session: Any
) -> None:
    """Verify reactive watchlist tables work with .latest() and .history() across Timestamp, WindowStart, and keyless schemas."""
    from deephaven import dtypes as dht
    from deephaven import input_table

    sess = mock_session(auto_flush=False)
    with exec_ctx_scope(dh_exec_ctx):
        empty_wl = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])
        wl_tbl = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])
        wl_tbl.add(
            arrow_to_deephaven_table(pa.Table.from_pydict({"Symbol": ["AAPL"]}), dh_exec_ctx)
        )

    assert sess.stocks.trades(empty_wl)._canonical_symbol_list() == []
    assert len(dh_to_pylist(sess.stocks.trades(empty_wl).snapshot())) == 0
    assert len(dh_to_pylist(sess.stocks.trades(wl_tbl).snapshot())) == 1

    fb_wl_trades = sess.stocks.trades(wl_tbl)
    assert fb_wl_trades._canonical_symbol_list() == ["AAPL"]
    t_latest = fb_wl_trades.latest(seed=False)
    assert t_latest is not None
    t_hist_reuse = fb_wl_trades.history(seed_since="2025-01-01")
    assert t_hist_reuse is not None
    t_ring_reuse = fb_wl_trades.ring(capacity=10, seed=True)
    assert t_ring_reuse is not None
    assert fb_wl_trades.watchlist_controller is not None
    assert fb_wl_trades.watchlist_controller.seed_fn is not None
    assert fb_wl_trades.watchlist_controller.wait_idle(timeout=5.0)
    assert fb_wl_trades._canonical_symbol_list() == ["AAPL"]
    assert sess.stop_stream(symbols="aapl") == 1

    fb_wl_bars = sess.stocks.bars(wl_tbl)
    b_hist = fb_wl_bars.history(seed=True)
    assert b_hist is not None
    assert fb_wl_bars.watchlist_controller is not None
    assert fb_wl_bars.watchlist_controller.wait_idle(timeout=5.0)
    fb_wl_bars.stop()

    no_ts_spec = SchemaSpec(
        name="no_ts_spec",
        arrow_schema=pa.schema([pa.field("Symbol", pa.string()), pa.field("Val", pa.float64())]),
        normalizer=lambda r, n: {"Symbol": "AAPL", "Val": 1.0},
        key_cols=("Symbol",),
    )
    fb_wl_nots = FeedBuilder(
        name="wl_nots",
        schema_spec=no_ts_spec,
        symbol_normalizer=sess.stocks.symbol_normalizer,
        exec_ctx=dh_exec_ctx,
        ws_multiplexer=sess.ws_multiplexer,
        ws_url="wss://socket.massive.com/stocks",
        ws_channel="T",
        symbols=wl_tbl,
        auto_flush=False,
    )
    nots_hist = fb_wl_nots.history(seed=False)
    assert nots_hist is not None
    assert fb_wl_nots.watchlist_controller is not None
    fb_wl_nots.watchlist_controller.wait_idle(timeout=5.0)
    fb_wl_nots.stop()

    win_only_spec = SchemaSpec(
        name="win_only_spec",
        arrow_schema=pa.schema(
            [pa.field("Symbol", pa.string()), pa.field("WindowStart", UTC_NS_TYPE)]
        ),
        normalizer=lambda r, n: {
            "Symbol": "AAPL",
            "WindowStart": 1700000000000000000,
        },
        key_cols=("Symbol",),
    )
    fb_wl_win = FeedBuilder(
        name="wl_win",
        schema_spec=win_only_spec,
        symbol_normalizer=sess.stocks.symbol_normalizer,
        exec_ctx=dh_exec_ctx,
        ws_multiplexer=sess.ws_multiplexer,
        ws_url="wss://socket.massive.com/stocks",
        ws_channel="AM",
        symbols=wl_tbl,
        auto_flush=True,
    )
    win_hist = fb_wl_win.history(seed=False)
    assert win_hist is not None
    assert fb_wl_win.watchlist_controller is not None
    fb_wl_win.watchlist_controller.wait_idle(timeout=5.0)
    fb_wl_win.stop()


def test_feed_builder_stream_registry_sharing_and_backfill(mock_session: Any) -> None:
    """Verify multiple builders for the same stream share a single publisher and reference-count stop()."""
    sess_auto = mock_session(auto_flush=True)
    fb_auto_seed = sess_auto.stocks.trades("AAPL")
    fb_auto_seed._seed_fn = lambda _syms: [
        {"sym": "AAPL", "p": 155.0, "s": 10, "i": "T_LIVE", "t": 1800000000000000000}
    ]
    _ = fb_auto_seed.latest(seed=True)

    fb_auto_reused = sess_auto.stocks.trades("AAPL")
    fb_auto_reused._seed_rows_fn = lambda _syms, _since: [
        {"sym": "AAPL", "p": 150.0, "s": 10, "i": "T_HIST", "t": 1750000000000000000}
    ]
    _ = fb_auto_reused.history(seed_since="2025-01-01", seed=True)
    assert fb_auto_seed.publisher is fb_auto_reused.publisher

    fb_auto_reused_empty = sess_auto.stocks.trades("AAPL")
    fb_auto_reused_empty._seed_rows_fn = lambda _syms, _since: [
        {"sym": "AAPL", "p": 156.0, "s": 10, "i": "T_NEWER", "t": 1900000000000000000}
    ]
    _ = fb_auto_reused_empty.history(seed_since="2025-01-01", seed=True)
    fb_auto_reused_empty.stop()

    fb_auto_seed.stop()
    assert "T.AAPL" in sess_auto.ws_multiplexer.active_topics("wss://socket.massive.com/stocks")
    fb_auto_reused.stop()
    assert "T.AAPL" not in sess_auto.ws_multiplexer.active_topics("wss://socket.massive.com/stocks")

    sess = mock_session(auto_flush=False)
    fb_unseeded = sess.stocks.trades("NVDA")
    _ = fb_unseeded.blink()
    assert fb_unseeded.publisher is not None and not fb_unseeded.publisher.has_snapshot_seed
    fb_seed_later = sess.stocks.trades("NVDA")
    _ = fb_seed_later.latest(seed=True)
    assert fb_unseeded.publisher.has_snapshot_seed is True
    fb_unseeded.stop()
    fb_seed_later.stop()

    fb_amd_blink = sess.stocks.trades("AMD")
    _ = fb_amd_blink.blink()
    fb_no_cutoff = sess.stocks.trades("AMD")

    def _seed_with_concurrent_live(_syms: list[str], _since: Any) -> list[dict[str, Any]]:
        assert fb_amd_blink.publisher is not None
        fb_amd_blink.publisher.enqueue(
            [{"sym": "AMD", "p": 160.0, "s": 5, "i": "A_LIVE", "t": 1800000000000000000}]
        )
        return [
            {"sym": "AMD", "p": 150.0, "s": 5, "i": "A_HIST", "t": 1750000000000000000},
            {"sym": "AMD", "p": 160.0, "s": 5, "i": "A_LIVE", "t": 1800000000000000000},
        ]

    fb_no_cutoff._seed_rows_fn = _seed_with_concurrent_live
    _ = fb_no_cutoff.history(seed_since="2025-01-01", seed=False)
    fb_no_cutoff.stop()
    fb_amd_blink.stop()


def test_feed_builder_seed_since_historical_backfill(mock_session: Any) -> None:
    """Verify .history(seed_since=...) performs single-symbol, parallel multi-symbol, and wildcard backfills."""
    sess = mock_session(auto_flush=False)
    fb_single = sess.stocks.trades(["AAPL", "*"])
    t_single = fb_single.history(seed_since="2025-01-01")
    assert t_single is not None
    assert fb_single.publisher is not None and fb_single.publisher.rows_published == 1

    fb_multi = sess.stocks.trades(["AAPL", "MSFT"])
    t_multi = fb_multi.history(seed_since="2025-01-01")
    assert t_multi is not None
    assert fb_multi.publisher is not None and fb_multi.publisher.is_seeded

    fb_star = sess.stocks.trades("*")
    t_star = fb_star.history(seed_since="2025-01-01")
    assert t_star is not None
    assert (
        fb_star.publisher is not None
        and fb_star.publisher.is_seeded
        and fb_star.publisher.rows_published == 0
    )
