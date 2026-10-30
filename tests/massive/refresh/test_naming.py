"""Tests for the naming policy: PascalCase columns, snake case, short and timestamp-like names."""

from __future__ import annotations

import pytest

from deephaven_massive._datasets import row_times, schema
from deephaven_massive._massive.refresh.naming import (
    KEY_COLUMNS,
    column_name,
    is_key_column,
    is_short,
    is_timestamp_like,
    snake,
)


@pytest.mark.parametrize(
    ("long_names", "column"),
    [
        (["prevDay", "vwap"], "PrevDayVwap"),
        (["sip_mapping", "CTA"], "SipMappingCta"),
        (["aff_10b5_one"], "Aff10b5One"),
        (["isUTC"], "IsUtc"),
        (["todaysChangePerc"], "TodaysChangePerc"),
        (["obfr_25th_percentile"], "Obfr25thPercentile"),
        (["s_and_p"], "SAndP"),
    ],
)
def test_column_names(long_names: list[str], column: str) -> None:
    assert column_name(long_names) == column


@pytest.mark.parametrize(
    ("name", "snaked"),
    [("serverTime", "server_time"), ("last_updated", "last_updated"), ("isUTC", "is_utc")],
)
def test_snake_case(name: str, snaked: str) -> None:
    assert snake(name) == snaked


@pytest.mark.parametrize(
    ("key", "short"), [("p", True), ("dav", True), ("vwap", False), ("a_b", False)]
)
def test_short_keys(key: str, short: bool) -> None:
    assert is_short(key) is short


@pytest.mark.parametrize(
    "name",
    [
        "timestamp",
        "sip_timestamp",
        "updated",
        "last_updated",
        "window_start",
        "window_end",
        "open_time",
        "serverTime",
        "lastUpdated",
        "sipTimestamp",
    ],
)
def test_timestamp_like_names(name: str) -> None:
    assert is_timestamp_like(name)


@pytest.mark.parametrize("name", ["volume", "time_zone", "updates", "window", "timestamps"])
def test_other_names_are_not_timestamp_like(name: str) -> None:
    assert not is_timestamp_like(name)


def test_the_key_columns_are_the_library_s_columns() -> None:
    assert {
        "Symbol",
        "Underlying",
        "Date",
        "Timestamp",
        "TimestampStart",
        "TimestampEnd",
        "TimestampInsert",
    } == KEY_COLUMNS
    assert all(column_name([name]) in KEY_COLUMNS for name in ("symbol", "timestamp", "date"))


@pytest.mark.parametrize(
    "column", ["Symbol", "Date", "Timestamp", "TimestampEnd", "TimestampFoo", "TimestampX2"]
)
def test_the_library_owns_every_timestamp_with_a_qualifier(column: str) -> None:
    assert is_key_column(column)


@pytest.mark.parametrize(
    "column", ["SipTimestamp", "EndTimestamp", "Timestamps", "Timestampend", "Dates", "Price"]
)
def test_other_names_are_the_vendor_s(column: str) -> None:
    assert not is_key_column(column)


def test_the_key_columns_are_the_ones_the_datasets_produce() -> None:
    # The refresh imports nothing outside the wire package, so it keeps its own copy of the
    # names the run-time key rename and the library's time columns give.
    produced = {*schema.KEY_COLUMNS.values(), row_times.START, row_times.INSERT}
    assert produced == KEY_COLUMNS
