"""Comprehensive tests for FeedBuilder, BaseDomainClient, FlatFilesClient, and all 7 domain clients."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
import io
from typing import Any

import pyarrow as pa
import pytest

from deephaven_massive.domains.base import (
    BaseDomainClient,
    extract_rows_from_payload,
    format_date_param,
    normalize_filter_params,
)
from deephaven_massive.domains.flat_files import (
    FlatFilesClient,
    _PrependReader,
    _date_range,
    _infer_schema_and_domain,
    _parse_date,
)
from deephaven_massive.engine.arrow_bridge import (
    SchemaSpec,
    arrow_to_deephaven_table,
    exec_ctx_scope,
)
from deephaven_massive.engine.builder import FeedBuilder, _is_deephaven_table
from deephaven_massive.schemas.economy import TREASURY_YIELDS_SCHEMA
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    INDEX_VALUES_SCHEMA,
    QUOTES_SCHEMA,
    TRADES_SCHEMA,
)
from tests.conftest import dh_to_pydict, dh_to_pylist, make_csv_gz_stream


def test_format_date_and_extract_rows_variants() -> None:
    assert format_date_param(None) is None
    assert format_date_param(date(2025, 1, 2)) == "2025-01-02"
    assert format_date_param(datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc)).startswith("2025-01-02T12:00:00")
    assert format_date_param(1700000000000) == "1700000000000"

    assert extract_rows_from_payload(None) == []
    assert extract_rows_from_payload("not-a-dict") == []  # type: ignore[arg-type]
    assert extract_rows_from_payload([{"a": 1}, "not-a-dict"]) == [{"a": 1}]
    assert extract_rows_from_payload({"results": 123}) == []
    assert extract_rows_from_payload({"results": {"values": [{"value": 50.0}]}}) == [{"value": 50.0}]
    assert extract_rows_from_payload({"results": {"ticker": "AAPL"}}) == [{"ticker": "AAPL"}]
    assert extract_rows_from_payload({"tickers": [{"ticker": "MSFT"}]}) == [{"ticker": "MSFT"}]
    assert extract_rows_from_payload({"last": {"ask": 1.1}, "symbol": "EUR/USD"}) == [
        {"ask": 1.1, "symbol": "EUR/USD"}
    ]
    assert extract_rows_from_payload({"last": {"ask": 1.1}}) == [{"ask": 1.1}]
    assert extract_rows_from_payload({"ticker": {"ticker": "NVDA"}}) == [{"ticker": "NVDA"}]
    assert extract_rows_from_payload({"status": "OK"}) == []
    assert extract_rows_from_payload(
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

    # Test normalize_filter_params variants
    from deephaven_massive.engine.symbols import SymbolNormalizer

    assert normalize_filter_params(
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
    assert normalize_filter_params(
        None,
        None,
        tickers=["aapl", "msft"],
        ticker_param="ticker",
        normalizer=SymbolNormalizer("stocks"),
    ) == {"ticker.any_of": "AAPL,MSFT"}
    assert normalize_filter_params(
        None,
        None,
        tickers=["spy", "qqq"],
        ticker_param="composite_ticker",
    ) == {"composite_ticker": "SPY,QQQ"}
    assert normalize_filter_params(
        None,
        None,
        symbol="aapl",
        tickers=[],
    ) == {}


def test_base_domain_client_pagination_chunking_and_routing(mock_session: Any) -> None:
    pages = [
        {
            "results": [{"ticker": "AAPL", "p": 150.0, "s": 10}, {"ticker": "AAPL", "p": 151.0, "s": 20}],
            "next_url": "https://api.massive.com/v3/trades/AAPL?cursor=page2",
        },
        {
            "results": [{"ticker": "AAPL", "p": 152.0, "s": 30}, {"ticker": "AAPL", "p": 153.0, "s": 40}],
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

    assert base.canonical_symbols(None) == ["*"]
    assert base.canonical_symbols("aapl") == ["AAPL"]
    assert base.canonical_symbols(["aapl", "msft"]) == ["AAPL", "MSFT"]

    # fetch_rest_rows with pagination and limit
    rows = base.fetch_rest_rows("/v3/trades/AAPL", {"foo": "bar"}, limit=3)
    assert len(rows) == 3

    # fetch_rest_rows without limit (exhausts all pages)
    call_idx = 0
    all_rows = base.fetch_rest_rows("/v3/trades/AAPL")
    assert len(all_rows) == 4

    # fetch_chunked_table with chunk_size=2 and limit=3
    call_idx = 0
    tbl = base.fetch_chunked_table(
        "/v3/trades/AAPL",
        TRADES_SCHEMA,
        {"foo": "bar"},
        limit=3,
        chunk_size=2,
    )
    assert len(dh_to_pylist(tbl)) == 3

    # fetch_chunked_table without limit (exhausts all pages)
    call_idx = 0
    tbl_all = base.fetch_chunked_table("/v3/trades/AAPL", TRADES_SCHEMA, chunk_size=2)
    assert len(dh_to_pylist(tbl_all)) == 4

    # fetch_multi_symbol_historical chunked conversion and global limit enforcement across symbols
    call_idx = 0
    tbl_multi_exact = base.fetch_multi_symbol_historical(
        ["AAPL", "MSFT", "NVDA"],
        TRADES_SCHEMA,
        None,
        lambda sym, s, e: (f"/v3/trades/{sym}", {}),
        limit=2,
        source="rest",
        chunk_size=1,
    )
    assert len(dh_to_pylist(tbl_multi_exact)) == 2
    assert call_idx == 1  # stopped after first symbol satisfied global limit=2

    # fetch_multi_symbol_historical with large limit (exhausts all symbols without breaking early)
    call_idx = 0
    tbl_multi_large_limit = base.fetch_multi_symbol_historical(
        ["AAPL", "MSFT"],
        TRADES_SCHEMA,
        None,
        lambda sym, s, e: (f"/v3/trades/{sym}", {}),
        limit=100,
        source="rest",
        chunk_size=10,
    )
    assert len(dh_to_pylist(tbl_multi_large_limit)) >= 4

    # fetch_multi_symbol_historical with limit=None (parallel ThreadPoolExecutor + chunking)
    call_idx = 0
    tbl_multi_parallel = base.fetch_multi_symbol_historical(
        ["AAPL", "MSFT"],
        TRADES_SCHEMA,
        None,
        lambda sym, s, e: (f"/v3/trades/{sym}", {}),
        limit=None,
        source="rest",
        chunk_size=2,
    )
    assert len(dh_to_pylist(tbl_multi_parallel)) >= 4

    # should_use_flat_files branches
    with pytest.raises(ValueError, match="Unsupported historical source"):
        base.should_use_flat_files("invalid", ["AAPL"], "us_stocks_sip/trades_v1")

    assert base.should_use_flat_files("rest", ["*"], "us_stocks_sip/trades_v1") is False

    with pytest.raises(ValueError, match="not supported for this feed"):
        base.should_use_flat_files("flat_files", ["AAPL"], None)

    assert base.should_use_flat_files("flat_files", ["AAPL"], "us_stocks_sip/trades_v1") is True
    assert base.should_use_flat_files("auto", ["*"], "us_stocks_sip/trades_v1") is True
    assert base.should_use_flat_files("auto", ["AAPL", "MSFT", "NVDA"], "us_stocks_sip/trades_v1") is True
    assert (
        base.should_use_flat_files(
            "auto",
            ["AAPL", "MSFT", "NVDA"],
            "us_stocks_sip/day_aggs_v1",
            adjusted=True,
        )
        is False
    )
    assert base.should_use_flat_files("auto", ["AAPL"], "us_stocks_sip/trades_v1") is False

    sess_no_s3 = mock_session(s3_access_key_id=None, s3_secret_access_key=None)
    base_no_s3 = BaseDomainClient(sess_no_s3, domain="stocks")
    assert base_no_s3.canonical_symbols("aapl") == ["AAPL"]
    assert base_no_s3.should_use_flat_files("auto", ["*"], "us_stocks_sip/trades_v1") is False
    with pytest.raises(ValueError, match="S3 credentials"):
        base_no_s3.should_use_flat_files("flat_files", ["AAPL"], "us_stocks_sip/trades_v1")


def test_flat_files_client_and_helpers(
    mock_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert _parse_date(datetime(2025, 1, 2, 10, 0)) == date(2025, 1, 2)
    assert _parse_date(date(2025, 1, 2)) == date(2025, 1, 2)
    assert _parse_date("2025-01-02") == date(2025, 1, 2)

    prep_test = _PrependReader(b"ab", io.BytesIO(b"cdef"))
    assert prep_test.readable() is True
    assert prep_test.read(4) == b"abcd"
    assert prep_test.read() == b"ef"

    assert _date_range(None, None, single_date="2025-01-02") == [date(2025, 1, 2)]
    with pytest.raises(ValueError, match="Either `date` or `start`"):
        _date_range(None, None)
    with pytest.raises(ValueError, match="must be >="):
        _date_range("2025-01-05", "2025-01-02")
    assert len(_date_range("2025-01-01", "2025-01-03")) == 3

    assert _infer_schema_and_domain("us_stocks_sip/trades_v1", None, None) == (
        TRADES_SCHEMA,
        "stocks",
    )
    assert _infer_schema_and_domain("us_options_opra/quotes_v1", None, None) == (
        QUOTES_SCHEMA,
        "options",
    )
    assert _infer_schema_and_domain("us_indices/values_v1", None, None) == (
        INDEX_VALUES_SCHEMA,
        "indices",
    )
    assert _infer_schema_and_domain("custom_dataset/minute_aggs_v1", None, None) == (
        BARS_SCHEMA,
        "stocks",
    )
    assert _infer_schema_and_domain("custom", TRADES_SCHEMA, "crypto") == (
        TRADES_SCHEMA,
        "crypto",
    )

    csv_day1 = (
        "ticker,price,size,id,sip_timestamp,conditions\n"
        "AAPL,150.25,100,T1,1700000000000000000,\"[12, 37]\"\n"
        "AAPL,150.50,100,T1b,1700000000000000000,\"[12]\"\n"
        "MSFT,300.50,200,T2,1700000000000000001,[12]\n"
        ",100.0,10,T3,1700000000000000002,[]\n"
    )
    csv_day2 = "   \n"  # Empty day file
    csv_day3 = (
        "ticker,price,size,id,sip_timestamp,conditions\n"
        "AAPL,151.00,50,T4,1700000000000000003,[]\n"
        "AAPL,152.00,50,T5,1700000000000000004,[]\n"
    )

    def _s3_opener(bucket: str, key: str) -> Any:
        if key.endswith("2025-01-01.csv.gz"):
            return make_csv_gz_stream(csv_day1)
        if key.endswith("2025-01-02.csv.gz"):
            return io.BytesIO(csv_day2.encode("utf-8"))  # uncompressed empty stream
        return io.BytesIO(csv_day3.encode("utf-8"))  # uncompressed valid CSV stream

    sess = mock_session(s3_opener=_s3_opener)
    assert sess.flat_files.dataset_for("stocks", "trades_v1") == "us_stocks_sip/trades_v1"
    with pytest.raises(ValueError, match="Unsupported S3 flat-file domain"):
        sess.flat_files.dataset_for("unknown_domain", "trades_v1")

    tbl = sess.flat_files.load(
        "us_stocks_sip/trades_v1",
        start="2025-01-01",
        end="2025-01-03",
        symbols=["AAPL"],
        limit=3,
    )
    rows = dh_to_pylist(tbl)
    assert len(rows) == 3
    assert rows[0]["Symbol"] == "AAPL"
    assert rows[0]["Conditions"] == [12, 37]

    # Also test limit reached exactly at end of batch and empty filtered rows
    tbl_exact = sess.flat_files.load(
        "us_stocks_sip/trades_v1",
        start="2025-01-01",
        end="2025-01-03",
        symbols=["AAPL"],
        limit=2,
    )
    assert len(dh_to_pylist(tbl_exact)) == 2

    tbl_none = sess.flat_files.load(
        "us_stocks_sip/trades_v1",
        date="2025-01-01",
        symbols=["UNKNOWN"],
    )
    assert len(dh_to_pylist(tbl_none)) == 0

    # Test dual-positional (prefix, table_type, start, end) and parallel multi-day read (limit=None)
    tbl_dual_pos = sess.flat_files.load(
        "us_stocks_sip",
        "trades_v1",
        "2025-01-01",
        "2025-01-03",
        tickers=["AAPL"],
    )
    assert len(dh_to_pylist(tbl_dual_pos)) == 4

    # Test keyword table_type=...
    tbl_kw_type = sess.flat_files.load(
        "us_stocks_sip",
        table_type="trades_v1",
        date="2025-01-01",
        tickers="AAPL",
    )
    assert len(dh_to_pylist(tbl_kw_type)) == 2

    # Test boto3 fallback path and missing credentials path on FlatFilesClient
    sess_no_s3 = mock_session(s3_access_key_id=None, s3_secret_access_key=None)
    ff_no_s3 = FlatFilesClient(sess_no_s3)
    with pytest.raises(ValueError, match="Massive S3 credentials are required"):
        ff_no_s3.load("us_stocks_sip/trades_v1", date="2025-01-01")

    import boto3

    csv_6char = (
        "ticker,price,size,id,sip_timestamp,conditions\n"
        "EURUSD,151.00,50,T4,1700000000000000003,[]\n"
        "EURUSD,152.00,50,T5,1700000000000000004,[]\n"
    )

    class _FakeS3Client:
        def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:
            return {"Body": make_csv_gz_stream(csv_6char)}

    monkeypatch.setattr(boto3, "client", lambda *a, **kw: _FakeS3Client())
    sess_boto = mock_session(s3_opener=None)
    tbl_boto = sess_boto.flat_files.load("us_stocks_sip/trades_v1", date="2025-01-03")
    assert len(dh_to_pylist(tbl_boto)) == 2
    tbl_star = sess_boto.flat_files.load(
        "us_stocks_sip/trades_v1", date="2025-01-03", symbols="*"
    )
    assert len(dh_to_pylist(tbl_star)) == 2

    # Test start_date / end_date / max_rows aliases and all 16 convenience methods
    ff = sess_boto.flat_files
    for conv_tbl in (
        ff.stocks_trades(start_date="2025-01-03", end_date="2025-01-03", max_rows=1),
        ff.stocks_quotes(date="2025-01-03"),
        ff.stocks_bars(date="2025-01-03", timespan="minute"),
        ff.stocks_bars(date="2025-01-03", timespan="day"),
        ff.options_trades(date="2025-01-03"),
        ff.options_quotes(date="2025-01-03"),
        ff.options_bars(date="2025-01-03", timespan="minute"),
        ff.options_bars(date="2025-01-03", timespan="day"),
        ff.futures_trades(date="2025-01-03"),
        ff.futures_quotes(date="2025-01-03"),
        ff.futures_bars(date="2025-01-03", timespan="minute"),
        ff.futures_bars(date="2025-01-03", timespan="day"),
        ff.futures_session_bars(date="2025-01-03"),
        ff.indices_values(date="2025-01-03"),
        ff.indices_bars(date="2025-01-03", timespan="minute"),
        ff.indices_bars(date="2025-01-03", timespan="day"),
        ff.forex_quotes(date="2025-01-03"),
        ff.forex_bars(date="2025-01-03", timespan="minute"),
        ff.forex_bars(date="2025-01-03", timespan="day"),
        ff.crypto_trades(date="2025-01-03"),
        ff.crypto_bars(date="2025-01-03", timespan="minute"),
        ff.crypto_bars(date="2025-01-03", timespan="day"),
    ):
        assert len(dh_to_pylist(conv_tbl)) >= 1


def test_feed_builder_terminal_methods_and_edge_cases(
    dh_exec_ctx: Any, mock_session: Any
) -> None:
    from deephaven import dtypes as dht
    from deephaven import input_table

    sess = mock_session(auto_flush=False)
    assert _is_deephaven_table(None) is False
    assert _is_deephaven_table("AAPL") is False
    assert _is_deephaven_table(["AAPL"]) is False
    assert _is_deephaven_table(object()) is False

    with exec_ctx_scope(dh_exec_ctx):
        wl_tbl = input_table(col_defs={"Symbol": dht.string}, key_cols=["Symbol"])
        wl_tbl.add(arrow_to_deephaven_table(pa.Table.from_pydict({"Symbol": ["AAPL"]}), dh_exec_ctx))
    assert _is_deephaven_table(wl_tbl) is True

    # Non-streaming builder raises NotImplementedError on streaming terminals
    rest_only = FeedBuilder(
        name="rest_only_feed",
        schema_spec=TREASURY_YIELDS_SCHEMA,
        exec_ctx=dh_exec_ctx,
    )
    with pytest.raises(NotImplementedError, match="does not support WebSocket streaming"):
        rest_only.latest()
    with pytest.raises(NotImplementedError, match="does not support .snapshot()"):
        rest_only.snapshot()
    with pytest.raises(NotImplementedError, match="does not support .poll()"):
        rest_only.poll()
    with pytest.raises(NotImplementedError, match="does not support .historical"):
        rest_only.historical(start="2025-01-01")
    with pytest.raises(ValueError, match="capacity must be > 0"):
        rest_only.ring(0)

    # Builder without symbol_normalizer and exec_ctx=None
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
        latest_raw.await_update()
        assert len(dh_to_pylist(latest_raw)) == 1
        latest_by = fb_raw.latest(by=["Date"], seed=False)
        assert latest_by is not None

        hist_raw = fb_raw.history(seed_since="2025-01-01", seed=True)
        ring_raw = fb_raw.ring(capacity=10, seed=False)
        blink_raw = fb_raw.blink()
        assert hist_raw is not None and ring_raw is not None and blink_raw is not None

        # .snapshot() and .historical() fallback to seed_fn when snapshot_fn is None
        snap_raw = fb_raw.historical()
        assert len(dh_to_pylist(snap_raw)) == 1
        poll_raw = fb_raw.poll(interval_s=10.0)
        assert fb_raw.poller is not None
        assert fb_raw.publisher is not None
        fb_raw.stop()

    # Empty snapshot returns empty table
    fb_empty = FeedBuilder(
        name="empty_snap",
        schema_spec=TRADES_SCHEMA,
        exec_ctx=dh_exec_ctx,
        snapshot_fn=lambda: [],
    )
    assert len(dh_to_pylist(fb_empty.snapshot())) == 0

    # Reactive watchlist with .latest() and .history() (covering Timestamp, WindowStart, and keyless expressions)
    fb_wl_trades = sess.stocks.trades(wl_tbl)
    assert fb_wl_trades._canonical_symbol_list() == []  # before controller starts
    t_latest = fb_wl_trades.latest(seed=False)
    assert t_latest is not None
    assert fb_wl_trades.watchlist_controller is not None
    assert fb_wl_trades.watchlist_controller.wait_idle(timeout=5.0)
    assert fb_wl_trades._canonical_symbol_list() == ["AAPL"]
    assert sess.stop_stream(symbols="aapl") == 1

    fb_wl_bars = sess.stocks.bars(wl_tbl)
    b_hist = fb_wl_bars.history(seed=False)
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

    from deephaven_massive.engine.arrow_bridge import UTC_NS_TYPE

    win_only_spec = SchemaSpec(
        name="win_only_spec",
        arrow_schema=pa.schema([pa.field("Symbol", pa.string()), pa.field("WindowStart", UTC_NS_TYPE)]),
        normalizer=lambda r, n: {"Symbol": "AAPL", "WindowStart": 1700000000000000000},
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

    # Also test ring() and poll() with exec_ctx and on_register_lifecycle, plus auto_flush=True seed
    sess_auto = mock_session(auto_flush=True)
    fb_auto_seed = sess_auto.stocks.trades("AAPL")
    _ = fb_auto_seed.latest(seed=True)
    # Reused stream via _stream_registry with seed_since backfill and auto_flush=True
    fb_auto_reused = sess_auto.stocks.trades("AAPL")
    _ = fb_auto_reused.history(seed_since="2025-01-01", seed=True)
    # Stopping first builder keeps shared publisher & WS topic alive until second builder stops
    fb_auto_seed.stop()
    assert "T.AAPL" in sess_auto.ws_multiplexer.active_topics("wss://socket.massive.com/stocks")
    fb_auto_reused.stop()
    assert "T.AAPL" not in sess_auto.ws_multiplexer.active_topics("wss://socket.massive.com/stocks")

    # Test .blink() (unseeded) followed by reused .latest(seed=True) snapshot backfill
    fb_unseeded = sess.stocks.trades("NVDA")
    _ = fb_unseeded.blink()
    assert fb_unseeded.publisher is not None and not fb_unseeded.publisher.has_snapshot_seed
    fb_seed_later = sess.stocks.trades("NVDA")
    _ = fb_seed_later.latest(seed=True)
    assert fb_unseeded.publisher.has_snapshot_seed is True
    fb_unseeded.stop()
    fb_seed_later.stop()

    ring_ctx = sess.stocks.trades("AAPL").ring(capacity=5, seed=False)
    assert ring_ctx is not None
    # Fresh stream with seed_since backfill (single symbol, multi-symbol parallel, and "*" empty concrete)
    _ = sess.stocks.trades(["AAPL", "*"]).history(seed_since="2025-01-01")
    _ = sess.stocks.trades(["AAPL", "MSFT"]).history(seed_since="2025-01-01")
    _ = sess.stocks.trades("*").history(seed_since="2025-01-01")
    poll_fb = sess.stocks.snapshots("AAPL")
    poll_ctx = poll_fb.poll(interval_s=10.0)
    assert poll_ctx is not None
    poll_fb.stop()


def test_stocks_domain_all_feeds(mock_session: Any) -> None:
    def _http(
        url: str, params: Mapping[str, Any] | None, headers: Mapping[str, str]
    ) -> tuple[int, Any]:
        if "/events" in url:
            return 200, {"results": {"events": [{"type": "ticker_change", "date": "2022-06-09", "ticker_change": {"ticker": "META"}}]}}
        return 200, {"status": "OK", "results": [{"ticker": "AAPL", "p": 150.0, "s": 100, "t": 1700000000000000000}]}

    csv_text = "ticker,price,size,id,sip_timestamp,conditions\nAAPL,150.0,100,T1,1700000000000000000,[]\n"
    sess = mock_session(
        http_handler=_http,
        s3_opener=lambda b, k: make_csv_gz_stream(csv_text),
    )
    st = sess.stocks

    # Streaming, single-symbol REST historical, multi-symbol REST historical, and S3 flat-files historical
    assert len(dh_to_pylist(st.trades(["AAPL"]).latest(seed=True))) >= 0
    assert len(dh_to_pylist(st.trades("*").latest(seed=True))) >= 0
    assert len(dh_to_pylist(st.trades("AAPL").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(st.trades(["AAPL", "MSFT"]).historical("2025-01-01", "2025-01-02", source="rest"))) == 2
    assert len(dh_to_pylist(st.trades("*").historical("2025-01-01", "2025-01-01", source="flat_files"))) == 1
    with pytest.raises(ValueError, match="Specific symbol"):
        st.trades("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert len(dh_to_pylist(st.quotes("AAPL").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(st.bars("AAPL", multiplier=1, timespan="minute").snapshot())) == 1
    assert len(dh_to_pylist(st.bars("AAPL", multiplier=1, timespan="minute").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(st.bars("AAPL", multiplier=1, timespan="day").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(st.bars("AAPL", multiplier=1, timespan="second").blink().columns) > 0

    assert st.luld("AAPL").blink() is not None
    assert st.imbalances("AAPL").blink() is not None
    assert st.fmv("AAPL").blink() is not None

    # Snapshots & market summaries
    assert len(dh_to_pylist(st.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(st.snapshots(["AAPL"], include_otc=True).snapshot())) == 1
    assert len(dh_to_pylist(st.snapshots(direction="gainers").snapshot())) == 1
    assert len(dh_to_pylist(st.top_movers("gainers").snapshot())) == 1
    assert len(dh_to_pylist(st.gainers().snapshot())) == 1
    assert len(dh_to_pylist(st.losers().snapshot())) == 1
    with pytest.raises(ValueError, match="direction must be"):
        st.snapshots(direction="sideways").snapshot()
    assert len(dh_to_pylist(st.daily_market_summary("2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(st.previous_close("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.previous_day_bar("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.daily_open_close("AAPL", "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(st.daily_ticker_summary("AAPL", "2025-01-02").snapshot())) == 1

    # Technical indicators (both .snapshot() and .historical())
    assert len(dh_to_pylist(st.sma("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.sma("AAPL").historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(st.ema("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.macd("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(st.rsi("AAPL").snapshot())) == 1

    # Fundamentals, Corporate Actions, Reference, and SEC feeds
    for fb in (
        st.short_interest("AAPL"),
        st.short_interest(),
        st.short_volume("AAPL"),
        st.short_volume(),
        st.float("AAPL"),
        st.float(),
        st.share_float("AAPL"),
        st.balance_sheets("AAPL", timeframe="quarterly"),
        st.balance_sheets(),
        st.income_statements("AAPL", timeframe="annual"),
        st.income_statements(),
        st.cash_flow_statements("AAPL"),
        st.cash_flow_statements(),
        st.financials("AAPL", timeframe="quarterly"),
        st.financials_vx(),
        st.ratios("AAPL"),
        st.ratios(),
        st.dividends("AAPL"),
        st.dividends(),
        st.splits("AAPL"),
        st.splits(),
        st.ipos("AAPL"),
        st.ipos(),
        st.tickers(search="Apple"),
        st.ticker_details("AAPL"),
        st.related_companies("AAPL"),
        st.ticker_types(),
        st.sec_filings("AAPL", form_type="10-K"),
        st.sec_filings(),
        st.sec_edgar_index("AAPL"),
        st.sec_10k_sections("AAPL", section="1A"),
        st.sec_8k_text("AAPL"),
        st.sec_risk_factors("AAPL"),
        st.sec_disclosures("AAPL"),
        st.sec_13f_holdings("AAPL", cik="0000320193"),
        st.sec_13f_holdings(),
        st.sec_form3_ownership("AAPL"),
        st.sec_insider_transactions("AAPL"),
        st.sec_insider_transactions(),
        st.sec_form4_transactions("AAPL"),
        st.news("AAPL"),
        st.news(),
        st.exchanges(),
        st.conditions(),
        st.market_holidays(),
        st.holidays(),
        st.market_status(),
    ):
        assert len(dh_to_pylist(fb.snapshot())) == 1
        assert len(dh_to_pylist(fb.historical("2025-01-01", "2025-01-02"))) == 1

    assert len(dh_to_pylist(st.ticker_events("META").snapshot())) == 1


def test_options_futures_indices_forex_crypto_economy_and_partners(
    mock_session: Any,
) -> None:
    csv_text = "ticker,price,size,id,sip_timestamp,conditions\nEURUSD,5.25,10,T1,1700000000000000000,[]\n"
    sess = mock_session(s3_opener=lambda b, k: make_csv_gz_stream(csv_text))

    # 1. Options domain
    from deephaven_massive.domains.options import _extract_option_underlying

    assert _extract_option_underlying("O:AAPL") is None
    assert _extract_option_underlying("O:AAPL250117C00150000") == "AAPL"
    opt = sess.options
    c1 = "O:AAPL250117C00150000"
    c2 = "O:AAPL250117P00150000"
    assert len(dh_to_pylist(opt.trades(c1).latest(seed=True))) >= 0
    assert len(dh_to_pylist(opt.trades([c1, c2]).latest(seed=True))) >= 0
    assert len(dh_to_pylist(opt.trades("*").latest(seed=True))) >= 0
    assert len(dh_to_pylist(opt.trades(c1).historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(opt.trades([c1, c2]).historical("2025-01-01", "2025-01-02", source="rest"))) == 2
    assert len(dh_to_pylist(opt.trades("*").historical("2025-01-01", "2025-01-01", source="flat_files"))) == 1
    with pytest.raises(ValueError, match="Specific option contract"):
        opt.trades("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert len(dh_to_pylist(opt.quotes(c1).historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(opt.bars(c1, multiplier=1, timespan="minute").snapshot())) == 1
    assert len(dh_to_pylist(opt.bars(c1, multiplier=1, timespan="minute").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(opt.bars(c1, multiplier=1, timespan="day").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert opt.bars(c1, multiplier=1, timespan="second").blink() is not None
    assert opt.fmv(c1).blink() is not None
    assert len(dh_to_pylist(opt.chain_snapshot("AAPL", strike_price=150.0).snapshot())) == 1
    assert len(dh_to_pylist(opt.chain("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(opt.contract_snapshot("AAPL", c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.contracts("AAPL").snapshot())) == 1
    assert len(dh_to_pylist(opt.contracts().historical("2025-01-01", "2025-12-31"))) == 1
    assert len(dh_to_pylist(opt.contract_details(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.previous_close(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.daily_open_close(c1, "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(opt.exchanges().snapshot())) == 1
    assert len(dh_to_pylist(opt.conditions().snapshot())) == 1
    assert len(dh_to_pylist(opt.sma(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.sma(c1).historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(opt.ema(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.macd(c1).snapshot())) == 1
    assert len(dh_to_pylist(opt.rsi(c1).snapshot())) == 1

    # 2. Futures domain
    fut = sess.futures
    assert len(dh_to_pylist(fut.trades("ESH5", venue="cme").latest(seed=True))) >= 0
    assert len(dh_to_pylist(fut.trades("ESH5").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(fut.trades(["ESH5", "NQH5"]).historical("2025-01-01", "2025-01-02", source="rest"))) == 2
    assert len(dh_to_pylist(fut.trades("*").historical("2025-01-01", "2025-01-01", source="flat_files"))) == 1
    with pytest.raises(ValueError, match="Specific futures contract"):
        fut.trades("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert len(dh_to_pylist(fut.quotes("ESH5").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(fut.bars("ESH5", multiplier=1, timespan="minute").snapshot())) == 1
    assert len(dh_to_pylist(fut.bars("ESH5", multiplier=1, timespan="minute").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(fut.bars("ESH5", multiplier=1, timespan="day").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(fut.bars("ESH5", multiplier=1, timespan="session").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert fut.bars("ESH5", multiplier=1, timespan="second").blink() is not None
    assert len(dh_to_pylist(fut.snapshots("ESH5").snapshot())) == 1
    assert len(dh_to_pylist(fut.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(fut.contracts("ES").snapshot())) == 1
    assert len(dh_to_pylist(fut.products(venue="CME").snapshot())) == 1
    assert len(dh_to_pylist(fut.schedules("ES").snapshot())) == 1
    assert len(dh_to_pylist(fut.exchanges().snapshot())) == 1
    assert len(dh_to_pylist(fut.market_status().snapshot())) == 1

    # 3. Indices domain
    idx = sess.indices
    assert len(dh_to_pylist(idx.values("I:SPX").latest(seed=True))) >= 0
    assert len(dh_to_pylist(idx.values("I:SPX").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(idx.values(["I:SPX", "I:NDX"]).historical("2025-01-01", "2025-01-02", source="rest"))) == 2
    assert len(dh_to_pylist(idx.values("*").historical("2025-01-01", "2025-01-01", source="flat_files"))) == 1
    with pytest.raises(ValueError, match="Specific index symbol"):
        idx.values("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert len(dh_to_pylist(idx.bars("I:SPX", multiplier=1, timespan="minute").snapshot())) == 1
    assert len(dh_to_pylist(idx.bars("I:SPX", multiplier=1, timespan="minute").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(idx.bars("I:SPX", multiplier=1, timespan="day").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert idx.bars("I:SPX", multiplier=1, timespan="second").blink() is not None
    assert len(dh_to_pylist(idx.snapshots("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(idx.previous_close("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.daily_open_close("I:SPX", "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(idx.tickers().snapshot())) == 1
    assert len(dh_to_pylist(idx.market_holidays().snapshot())) == 1
    assert len(dh_to_pylist(idx.market_status().snapshot())) == 1
    assert len(dh_to_pylist(idx.sma("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.sma("I:SPX").historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(idx.ema("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.macd("I:SPX").snapshot())) == 1
    assert len(dh_to_pylist(idx.rsi("I:SPX").snapshot())) == 1

    # 4. Forex domain
    fx = sess.forex
    assert len(dh_to_pylist(fx.quotes("EUR/USD").latest(seed=True))) >= 0
    assert len(dh_to_pylist(fx.quotes("EUR/USD").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(fx.quotes(["EUR/USD", "GBP/USD"]).historical("2025-01-01", "2025-01-02", source="rest"))) == 2
    assert len(dh_to_pylist(fx.quotes("*").historical("2025-01-01", "2025-01-01", source="flat_files"))) == 1
    with pytest.raises(ValueError, match="Specific currency pair"):
        fx.quotes("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert len(dh_to_pylist(fx.bars("EUR/USD", multiplier=1, timespan="minute").snapshot())) == 1
    assert len(dh_to_pylist(fx.bars("EUR/USD", multiplier=1, timespan="minute").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(fx.bars("EUR/USD", multiplier=1, timespan="day").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert fx.bars("EUR/USD", multiplier=1, timespan="second").blink() is not None
    assert fx.fmv("EUR/USD").blink() is not None
    assert len(dh_to_pylist(fx.snapshots("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(fx.snapshots(direction="gainers").snapshot())) == 1
    assert len(dh_to_pylist(fx.top_movers("gainers").snapshot())) == 1
    with pytest.raises(ValueError, match="direction must be"):
        fx.snapshots(direction="invalid").snapshot()
    assert len(dh_to_pylist(fx.last_quote("EUR", "USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.last_quote("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.daily_market_summary("2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(fx.previous_close("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.daily_open_close("EUR/USD", "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(fx.convert("EUR", "USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.tickers().snapshot())) == 1
    assert len(dh_to_pylist(fx.exchanges().snapshot())) == 1
    assert len(dh_to_pylist(fx.market_holidays().snapshot())) == 1
    assert len(dh_to_pylist(fx.market_status().snapshot())) == 1
    assert len(dh_to_pylist(fx.sma("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.sma("EUR/USD").historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(fx.ema("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.macd("EUR/USD").snapshot())) == 1
    assert len(dh_to_pylist(fx.rsi("EUR/USD").snapshot())) == 1

    # 5. Crypto domain
    cr = sess.crypto
    assert len(dh_to_pylist(cr.trades("BTC-USD").latest(seed=True))) >= 0
    assert len(dh_to_pylist(cr.trades("BTC-USD").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(cr.trades(["BTC-USD", "ETH-USD"]).historical("2025-01-01", "2025-01-02", source="rest"))) == 2
    assert len(dh_to_pylist(cr.trades("*").historical("2025-01-01", "2025-01-01", source="flat_files"))) == 1
    with pytest.raises(ValueError, match="Specific crypto pair"):
        cr.trades("*").historical("2025-01-01", "2025-01-02", source="rest")

    assert cr.quotes("BTC-USD").blink() is not None
    assert len(dh_to_pylist(cr.bars("BTC-USD", multiplier=1, timespan="minute").snapshot())) == 1
    assert len(dh_to_pylist(cr.bars("BTC-USD", multiplier=1, timespan="minute").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert len(dh_to_pylist(cr.bars("BTC-USD", multiplier=1, timespan="day").historical("2025-01-01", "2025-01-02", source="rest"))) == 1
    assert cr.bars("BTC-USD", multiplier=1, timespan="second").blink() is not None
    assert cr.fmv("BTC-USD").blink() is not None
    assert len(dh_to_pylist(cr.snapshots("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.snapshots().snapshot())) == 1
    assert len(dh_to_pylist(cr.snapshots(direction="losers").snapshot())) == 1
    assert len(dh_to_pylist(cr.top_movers("losers").snapshot())) == 1
    with pytest.raises(ValueError, match="direction must be"):
        cr.snapshots(direction="invalid").snapshot()
    assert len(dh_to_pylist(cr.last_trade("BTC", "USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.last_trade("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.daily_market_summary("2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(cr.previous_close("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.daily_open_close("BTC-USD", "2025-01-02").snapshot())) == 1
    assert len(dh_to_pylist(cr.tickers().snapshot())) == 1
    assert len(dh_to_pylist(cr.conditions().snapshot())) == 1
    assert len(dh_to_pylist(cr.exchanges().snapshot())) == 1
    assert len(dh_to_pylist(cr.market_holidays().snapshot())) == 1
    assert len(dh_to_pylist(cr.market_status().snapshot())) == 1
    assert len(dh_to_pylist(cr.sma("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.sma("BTC-USD").historical("2025-01-01", "2025-01-02"))) == 1
    assert len(dh_to_pylist(cr.ema("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.macd("BTC-USD").snapshot())) == 1
    assert len(dh_to_pylist(cr.rsi("BTC-USD").snapshot())) == 1

    # 6. Economy domain
    ec = sess.economy
    for ef in (
        ec.funding_conditions(),
        ec.treasury_yields(),
        ec.inflation(),
        ec.inflation_expectations(),
        ec.labor_market(),
    ):
        assert len(dh_to_pylist(ef.snapshot())) == 1
        assert len(dh_to_pylist(ef.historical("2025-01-01", "2025-01-02"))) == 1

    # 7. Partners domain (Benzinga, ETF Global, TMX, Fable)
    bz = sess.partners.benzinga
    eg = sess.partners.etf_global
    tmx = sess.partners.tmx
    fb_part = sess.partners.fable
    for pf in (
        bz.ratings("AAPL"),
        bz.ratings(),
        bz.analyst_insights("AAPL"),
        bz.earnings("AAPL"),
        bz.earnings(),
        bz.guidance("AAPL"),
        bz.guidance(),
        bz.consensus_ratings("AAPL"),
        bz.consensus_ratings(),
        bz.news("AAPL"),
        bz.news(),
        bz.analysts("AN1"),
        bz.analysts(),
        bz.firms("F1"),
        bz.firms(),
        bz.bulls_say_bears_say("AAPL"),
        bz.bulls_say_bears_say(),
        bz.bulls_bears_say("AAPL"),
        bz.transcripts("AAPL"),
        bz.transcripts(),
        eg.constituents("SPY"),
        eg.constituents(),
        eg.fund_flows("SPY"),
        eg.fund_flows(),
        eg.analytics("SPY"),
        eg.analytics(),
        eg.profiles("SPY"),
        eg.profiles(),
        eg.taxonomies("SPY"),
        eg.taxonomies(),
        tmx.corporate_events("AAPL"),
        tmx.corporate_events(),
        fb_part.eu_merchant_aggregates("AAPL"),
        fb_part.eu_merchant_hierarchy("AAPL"),
    ):
        assert len(dh_to_pylist(pf.snapshot())) == 1
        assert len(dh_to_pylist(pf.historical("2025-01-01", "2025-01-02"))) == 1
