"""Unit and integration tests for FlatFilesClient and S3 streaming helpers."""

from __future__ import annotations

from datetime import date, datetime
import io
from typing import Any

import pytest

from deephaven_massive.domains.flat_files import (
    FlatFilesClient,
    _PrependReader,
    _date_range,
    _infer_schema_and_domain,
    _parse_date,
)
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    INDEX_VALUES_SCHEMA,
    QUOTES_SCHEMA,
    TRADES_SCHEMA,
)
from tests.conftest import dh_to_pylist, make_csv_gz_stream


def test_flat_files_date_and_stream_helpers() -> None:
    """Verify _parse_date, _PrependReader, and _date_range handle valid inputs and boundary errors."""
    assert _parse_date(datetime(2025, 1, 2, 10, 0)) == date(2025, 1, 2)
    assert _parse_date(date(2025, 1, 2)) == date(2025, 1, 2)
    assert _parse_date("2025-01-02") == date(2025, 1, 2)

    prep_test = _PrependReader(b"ab", io.BytesIO(b"cdef"))
    assert prep_test.readable() is True
    assert prep_test.read(4) == b"abcd"
    assert prep_test.read() == b"ef"

    assert _date_range(None, None, single_date="2025-01-02") == [date(2025, 1, 2)]
    assert len(_date_range("2025-01-01", "2025-01-03")) == 3

    with pytest.raises(ValueError, match="Either `date` or `start`"):
        _date_range(None, None)
    with pytest.raises(ValueError, match="must be >="):
        _date_range("2025-01-05", "2025-01-02")


def test_flat_files_infer_schema_and_dataset_for(mock_session: Any) -> None:
    """Verify _infer_schema_and_domain and dataset_for map S3 dataset paths to schemas and domains."""
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

    sess = mock_session()
    assert (
        sess.flat_files.dataset_for("stocks", "trades_v1") == "us_stocks_sip/trades_v1"
    )
    with pytest.raises(ValueError, match="Unsupported S3 flat-file domain"):
        sess.flat_files.dataset_for("unknown_domain", "trades_v1")


def test_flat_files_load_gzip_uncompressed_and_symbol_pushdown(
    mock_session: Any,
) -> None:
    """Verify FlatFilesClient.load streams gzip and uncompressed CSVs with symbol pushdown filtering."""
    csv_day1 = (
        "ticker,price,size,id,sip_timestamp,conditions\n"
        'AAPL,150.25,100,T1,1700000000000000000,"[12, 37]"\n'
        'AAPL,150.50,100,T1b,1700000000000000000,"[12]"\n'
        "MSFT,300.50,200,T2,1700000000000000001,[12]\n"
        ",100.0,10,T3,1700000000000000002,[]\n"
    )
    csv_day2 = "   \n"
    csv_day3 = (
        "ticker,price,size,id,sip_timestamp,conditions\n"
        "AAPL,151.00,50,T4,1700000000000000003,[]\n"
        "AAPL,152.00,50,T5,1700000000000000004,[]\n"
    )

    def _s3_opener(bucket: str, key: str) -> Any:
        if key.endswith("2025-01-01.csv.gz"):
            return make_csv_gz_stream(csv_day1)
        if key.endswith("2025-01-02.csv.gz"):
            return io.BytesIO(csv_day2.encode("utf-8"))
        return io.BytesIO(csv_day3.encode("utf-8"))

    sess = mock_session(s3_opener=_s3_opener)
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


def test_flat_files_load_limits_and_call_signatures(mock_session: Any) -> None:
    """Verify FlatFilesClient.load supports exact batch limits, empty matches, and positional/keyword signatures."""
    csv_day1 = (
        "ticker,price,size,id,sip_timestamp,conditions\n"
        "AAPL,150.25,100,T1,1700000000000000000,[]\n"
        "AAPL,150.50,100,T1b,1700000000000000000,[]\n"
    )
    csv_day3 = (
        "ticker,price,size,id,sip_timestamp,conditions\n"
        "AAPL,151.00,50,T4,1700000000000000003,[]\n"
        "AAPL,152.00,50,T5,1700000000000000004,[]\n"
    )

    def _s3_opener(bucket: str, key: str) -> Any:
        if key.endswith("2025-01-01.csv.gz"):
            return make_csv_gz_stream(csv_day1)
        if key.endswith("2025-01-02.csv.gz"):
            return io.BytesIO(b"   \n")
        return io.BytesIO(csv_day3.encode("utf-8"))

    sess = mock_session(s3_opener=_s3_opener)
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

    tbl_dual_pos = sess.flat_files.load(
        "us_stocks_sip",
        "trades_v1",
        "2025-01-01",
        "2025-01-03",
        tickers=["AAPL"],
    )
    assert len(dh_to_pylist(tbl_dual_pos)) == 4

    tbl_kw_type = sess.flat_files.load(
        "us_stocks_sip",
        table_type="trades_v1",
        date="2025-01-01",
        tickers="AAPL",
    )
    assert len(dh_to_pylist(tbl_kw_type)) == 2


def test_flat_files_boto3_client_and_credentials_validation(
    mock_session: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify FlatFilesClient validates S3 credentials and streams via boto3 when s3_opener is None."""
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


def test_flat_files_all_domain_convenience_methods(mock_session: Any) -> None:
    """Verify all 16 domain-specific convenience methods on FlatFilesClient route to the expected S3 datasets."""
    csv_6char = (
        "ticker,price,size,id,sip_timestamp,conditions\n"
        "EURUSD,151.00,50,T4,1700000000000000003,[]\n"
        "EURUSD,152.00,50,T5,1700000000000000004,[]\n"
    )
    opened_keys: list[str] = []

    def _s3_opener(bucket: str, key: str) -> Any:
        opened_keys.append(key)
        return make_csv_gz_stream(csv_6char)

    sess = mock_session(s3_opener=_s3_opener)
    ff = sess.flat_files
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

    assert "us_stocks_sip/trades_v1/2025/01/2025-01-03.csv.gz" in opened_keys
    assert "us_options_opra/quotes_v1/2025/01/2025-01-03.csv.gz" in opened_keys
    assert "us_futures/session_aggs_v1/2025/01/2025-01-03.csv.gz" in opened_keys
    assert "us_indices/values_v1/2025/01/2025-01-03.csv.gz" in opened_keys
    assert "global_forex/quotes_v1/2025/01/2025-01-03.csv.gz" in opened_keys
    assert "global_crypto/trades_v1/2025/01/2025-01-03.csv.gz" in opened_keys
