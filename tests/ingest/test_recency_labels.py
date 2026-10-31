"""Tests for reading Massive's recency labels and the refusal a delayed one causes."""

import pytest

import deephaven_massive as dm
from deephaven_massive._enums import Subscription
from deephaven_massive._ingest.recency_labels import (
    RecencyLabels,
    delayed_refusal,
    read_labels,
    shows_real_time,
)


def test_no_label_reads_as_none() -> None:
    assert read_labels("OK", [{"ticker": "SPY"}]) == RecencyLabels(None, None, None)


def test_the_envelope_status_is_delayed_evidence() -> None:
    labels = read_labels("DELAYED", [{"last_trade": {"timeframe": "REAL-TIME"}}])
    assert labels.delayed == "status=DELAYED"
    assert labels.real_time == "last_trade.timeframe=REAL-TIME"


@pytest.mark.parametrize(
    "field", ["timeframe", "last_trade.timeframe", "last_quote.timeframe", "last_minute.timeframe"]
)
def test_each_timeframe_field_is_read(field: str) -> None:
    head, _, leaf = field.rpartition(".")
    row: dict[str, object] = {leaf: "DELAYED"} if not head else {head: {leaf: "DELAYED"}}
    assert read_labels("OK", [row]).delayed == f"{field}=DELAYED"


def test_the_underlying_timeframe_is_never_read() -> None:
    # Massive's option-chain sample shows the underlying delayed beside a real-time contract.
    row = {"underlying_asset": {"timeframe": "DELAYED"}, "last_trade": {"timeframe": "REAL-TIME"}}
    labels = read_labels("OK", [row])
    assert labels.delayed is None
    assert labels.real_time == "last_trade.timeframe=REAL-TIME"


def test_the_first_delayed_and_unknown_labels_are_kept() -> None:
    rows: list[dict[str, object]] = [
        {"last_trade": {"timeframe": "END-OF-DAY"}},
        {"last_quote": {"timeframe": "DELAYED"}, "last_trade": {"timeframe": "WHENEVER"}},
        {"timeframe": "DELAYED"},
    ]
    labels = read_labels("OK", rows)
    assert labels.delayed == "last_quote.timeframe=DELAYED"
    assert labels.unknown == ("last_trade.timeframe", "END-OF-DAY")


def test_a_nested_field_under_a_scalar_is_absent() -> None:
    assert read_labels("OK", [{"last_trade": 3}]) == RecencyLabels(None, None, None)


def test_a_stream_refusal_names_the_delayed_feed() -> None:
    error = delayed_refusal(
        Subscription.OPTIONS, "options streams", "last_trade.timeframe=DELAYED", streams=True
    )
    assert isinstance(error, dm.EntitlementError)
    assert str(error) == (
        "options streams would be delayed (Massive reports last_trade.timeframe=DELAYED), and "
        "dm.Subscription.OPTIONS is not in MassiveConfig.delayed_subscriptions, so the library "
        "serves only real-time options data. To use Massive's delayed options feed: "
        "dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS})"
    )


def test_a_rest_refusal_says_streams_move_too() -> None:
    error = delayed_refusal(
        Subscription.STOCKS, "stocks.snapshots data", "status=DELAYED", streams=False
    )
    assert "To accept delayed stocks data (its streams then use Massive's delayed feed too)" in (
        str(error)
    )


@pytest.mark.parametrize(
    ("evidence", "real_time"),
    [
        ("last_trade.timeframe=REAL-TIME", True),
        ("timeframe=REAL-TIME", True),
        ("timeframe='END-OF-DAY', neither DELAYED nor REAL-TIME, in the snapshot", False),
        ("no timeframe in the stocks snapshot", False),
        ("underlying_asset.timeframe=REAL-TIME", False),
    ],
)
def test_only_a_real_time_label_shows_real_time(evidence: str, *, real_time: bool) -> None:
    assert shows_real_time(evidence) is real_time
