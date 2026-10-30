"""Tests for the naming policy.

PascalCase columns, snake case, short and timestamp-like names, and the lead order.
"""

from __future__ import annotations

import pytest

from deephaven_massive._datasets import row_times, source_columns
from deephaven_massive._massive.refresh.naming import (
    KEY_COLUMNS,
    column_name,
    is_key_column,
    is_short,
    is_timestamp_like,
    lead_order,
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
    produced = {*source_columns.KEY_COLUMNS.values(), row_times.START, row_times.INSERT}
    assert produced == KEY_COLUMNS


# --- The lead order -------------------------------------------------------------------------


def _lead(concepts: str) -> list[str]:
    return lead_order(concepts.split(), str)


def test_the_lead_fields_come_first_in_their_order() -> None:
    bars = "ticker volume open close high low window_start transactions vwap"
    assert _lead(bars) == [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "ticker",
        "window_start",
        "transactions",
    ]
    assert _lead("exchange id size price conditions") == [
        "price",
        "size",
        "exchange",
        "id",
        "conditions",
    ]


def test_bid_fields_move_to_just_before_their_ask_partners() -> None:
    quotes = "ticker ask_exchange ask_price ask_size bid_exchange bid_price bid_size tape"
    assert _lead(quotes) == (
        [
            "bid_price",
            "bid_size",
            "ask_price",
            "ask_size",
            "ticker",
            "bid_exchange",
            "ask_exchange",
            "tape",
        ]
    )
    # A short spelling shares its long spelling's place; a bid with no ask partner stays put.
    assert _lead("ask bid bid_foo ask_bar x") == ["bid", "ask", "bid_foo", "ask_bar", "x"]


def test_each_nested_object_orders_its_own_fields_in_its_own_places() -> None:
    concepts = (
        "break_even_price day.decimal_volume day.close day.open fmv "
        "last_quote.ask last_quote.bid_exchange last_quote.ask_exchange last_quote.bid value"
    )
    assert _lead(concepts) == [
        "value",
        "day.open",
        "day.close",
        "day.decimal_volume",
        "break_even_price",
        "last_quote.bid",
        "last_quote.ask",
        "last_quote.bid_exchange",
        "last_quote.ask_exchange",
        "fmv",
    ]


def test_a_lead_matches_a_field_s_own_whole_name_only() -> None:
    # `previous_close` ends in `close`, and `update_rules.c.updates_volume` in `volume`.
    concepts = "previous_close update_rules.c.updates_high update_rules.c.updates_volume close"
    assert _lead(concepts) == (
        ["close", "update_rules.c.updates_high", "update_rules.c.updates_volume", "previous_close"]
    )


def test_a_key_column_keeps_its_place() -> None:
    assert _lead("symbol timestamp exchange price") == ["symbol", "timestamp", "price", "exchange"]
