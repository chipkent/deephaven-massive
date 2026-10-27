"""Tests for the declared timestamp units and the timestamp routine."""

import logging
import re
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from deephaven_massive import _errors
from deephaven_massive._datasets.catalog import declared_units
from deephaven_massive._datasets.timestamps import (
    FMV_NS_THRESHOLD,
    MAX_INSTANT_NS,
    MIN_INSTANT_NS,
    TimeField,
    TimeUnit,
    TimeUnitTable,
)
from deephaven_massive._enums import Transport
from deephaven_massive._errors import MassiveError
from deephaven_massive._session import CATALOG

# Field names that hold an epoch timestamp and so need a declared unit. The single-letter bar
# bounds `s` and `e` are ambiguous across models, so datasets register those explicitly.
_TIMESTAMP_LIKE = re.compile(r"^(t|timestamp|.*_timestamp|updated|.*_updated|window_.*)$")


def is_timestamp_like(field: str) -> bool:
    return _TIMESTAMP_LIKE.match(field) is not None


MS = TimeField("stocks.trades", Transport.WS, "t")
NS = TimeField("stocks.trades", Transport.FLAT_FILES, "sip_timestamp")
US = TimeField("test.micros", Transport.REST, "timestamp")
S = TimeField("test.seconds", Transport.REST, "updated")
FMV = TimeField("stocks.fmv", Transport.WS, "t")

TABLE = TimeUnitTable(
    {
        MS: TimeUnit.MILLISECONDS,
        NS: TimeUnit.NANOSECONDS,
        US: TimeUnit.MICROSECONDS,
        S: TimeUnit.SECONDS,
        FMV: TimeUnit.FMV_MAGNITUDE,
    }
)

# 2023-11-14T22:13:20Z
SECONDS = 1_700_000_000
NANOS = SECONDS * 10**9


def ints(*values: int | None) -> pa.Array:
    return pa.array(values, pa.int64())


def test_table_membership_and_lookup() -> None:
    assert MS in TABLE
    assert TimeField("x.y", Transport.WS, "t") not in TABLE
    assert TABLE.unit(US) is TimeUnit.MICROSECONDS


def test_every_fixed_unit_scales_to_nanoseconds() -> None:
    assert TABLE.to_epoch_ns(S, SECONDS) == NANOS
    assert TABLE.to_epoch_ns(MS, SECONDS * 10**3) == NANOS
    assert TABLE.to_epoch_ns(US, SECONDS * 10**6) == NANOS
    assert TABLE.to_epoch_ns(NS, NANOS) == NANOS


def test_unregistered_fields_are_refused() -> None:
    with pytest.raises(LookupError, match=r"stocks\.trades\.q on WS"):
        TABLE.unit(TimeField("stocks.trades", Transport.WS, "q"))
    with pytest.raises(LookupError):
        TABLE.to_epoch_ns(TimeField("stocks.trades", Transport.WS, "q"), 1)


def test_fmv_rule_classifies_at_the_threshold(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_errors, "_logged", set())
    with caplog.at_level(logging.INFO, logger="deephaven_massive._datasets.timestamps"):
        # The documentation sample, 13 digits: milliseconds.
        assert TABLE.to_epoch_ns(FMV, 1678220098130) == 1678220098130 * 10**6
        assert TABLE.to_epoch_ns(FMV, NANOS) == NANOS
    detected = [r.getMessage() for r in caplog.records if "FMV timestamps" in r.getMessage()]
    assert detected == ["FMV timestamps detected in ms (magnitude rule)"]
    # The threshold itself is nanoseconds (1973, so out of bounds); just below it is
    # milliseconds, which overflows.
    with pytest.raises(MassiveError, match=rf"{FMV_NS_THRESHOLD} ms\|ns is outside 1990 to 2100"):
        TABLE.to_epoch_ns(FMV, FMV_NS_THRESHOLD)
    with pytest.raises(MassiveError, match=rf"{FMV_NS_THRESHOLD - 1} ms overflows"):
        TABLE.to_epoch_ns(FMV, FMV_NS_THRESHOLD - 1)


def test_fmv_rule_logs_only_a_decided_unit(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_errors, "_logged", set())
    with caplog.at_level(logging.INFO, logger="deephaven_massive._datasets.timestamps"):
        assert TABLE.to_epoch_ns_array(FMV, ints(None, None)).to_pylist() == [None, None]
        assert not [r for r in caplog.records if "FMV timestamps detected" in r.getMessage()]
        TABLE.to_epoch_ns_array(FMV, ints(NANOS))
    detected = [r.getMessage() for r in caplog.records if "FMV timestamps" in r.getMessage()]
    assert detected == ["FMV timestamps detected in ns (magnitude rule)"]


def test_array_conversions() -> None:
    assert TABLE.to_epoch_ns_array(MS, ints(SECONDS * 1_000, None)).to_pylist() == [NANOS, None]
    assert TABLE.to_epoch_ns_array(NS, pa.array([NANOS], pa.uint64())).to_pylist() == [NANOS]
    fmv = ints(NANOS, 1678220098130, None)
    assert TABLE.to_epoch_ns_array(FMV, fmv).to_pylist() == [NANOS, 1678220098130 * 10**6, None]


def test_the_bound_includes_1990_and_all_of_2100() -> None:
    assert datetime.fromtimestamp(MIN_INSTANT_NS / 10**9, UTC) == datetime(1990, 1, 1, tzinfo=UTC)
    assert datetime.fromtimestamp(MAX_INSTANT_NS / 10**9, UTC) == datetime(2101, 1, 1, tzinfo=UTC)
    assert TABLE.to_epoch_ns(NS, MIN_INSTANT_NS) == MIN_INSTANT_NS
    assert TABLE.to_epoch_ns(NS, MAX_INSTANT_NS - 1) == MAX_INSTANT_NS - 1
    with pytest.raises(MassiveError, match=rf"{MIN_INSTANT_NS - 1} ns is outside 1990 to 2100"):
        TABLE.to_epoch_ns(NS, MIN_INSTANT_NS - 1)
    with pytest.raises(MassiveError, match=rf"{MAX_INSTANT_NS} ns is outside"):
        TABLE.to_epoch_ns(NS, MAX_INSTANT_NS)
    seconds = MAX_INSTANT_NS // 10**9
    assert TABLE.to_epoch_ns(S, seconds - 1) == MAX_INSTANT_NS - 10**9
    with pytest.raises(MassiveError, match=rf"{seconds} s is outside"):
        TABLE.to_epoch_ns(S, seconds)


def test_milliseconds_declared_as_nanoseconds_fail_the_bound() -> None:
    # A 13-digit millisecond value read as nanoseconds is in January 1970.
    with pytest.raises(
        MassiveError,
        match=r"^timestamp unit mismatch for stocks\.trades\.sip_timestamp: "
        r"1700000000000 ns is outside 1990 to 2100$",
    ):
        TABLE.to_epoch_ns_array(NS, ints(NANOS, SECONDS * 1_000, 5))


def test_nanoseconds_declared_as_milliseconds_overflow() -> None:
    with pytest.raises(
        MassiveError,
        match=rf"^timestamp unit mismatch for stocks\.trades\.t: {NANOS} ms overflows "
        r"64-bit nanoseconds$",
    ):
        TABLE.to_epoch_ns_array(MS, ints(SECONDS * 1_000, NANOS))
    # A negative value overflows too, and the message names the first offending value.
    with pytest.raises(MassiveError, match=rf"{-NANOS} ms overflows"):
        TABLE.to_epoch_ns_array(MS, ints(None, -NANOS, NANOS))


def test_seconds_declared_as_microseconds_fail_the_bound() -> None:
    with pytest.raises(MassiveError, match=rf"{SECONDS} us is outside"):
        TABLE.to_epoch_ns(US, SECONDS)


def test_values_beyond_64_bits_are_rejected() -> None:
    with pytest.raises(
        MassiveError, match=rf"^timestamp unit mismatch for stocks\.trades\.t: {2**64} does not"
    ):
        TABLE.to_epoch_ns(MS, 2**64)


def test_nulls_pass_the_bound() -> None:
    assert TABLE.to_epoch_ns_array(NS, ints(None)).to_pylist() == [None]
    assert TABLE.to_epoch_ns_array(NS, ints()).to_pylist() == []


def test_the_routine_turns_epoch_zero_into_null_outside_the_time_column() -> None:
    values = ints(NANOS, 0, None)
    assert TABLE.instants(NS, values, time_column=False).to_pylist() == [NANOS, None, None]
    assert TABLE.instants(MS, ints(0, SECONDS * 1_000), time_column=False).to_pylist() == [
        None,
        NANOS,
    ]
    # The time column keeps its 0, which then fails the bound.
    with pytest.raises(MassiveError, match=r"stocks\.trades\.sip_timestamp: 0 ns is outside"):
        TABLE.instants(NS, values, time_column=True)
    assert TABLE.instants(NS, ints(NANOS), time_column=True).to_pylist() == [NANOS]


def test_the_routine_bounds_every_column() -> None:
    with pytest.raises(MassiveError, match="5 ns is outside"):
        TABLE.instants(NS, ints(0, 5), time_column=False)


def test_labels() -> None:
    assert MS.label == "stocks.trades.t"


@pytest.mark.parametrize(
    "name",
    ["t", "timestamp", "sip_timestamp", "updated", "last_updated", "window_start", "window_end"],
)
def test_timestamp_like_names(name: str) -> None:
    assert is_timestamp_like(name)


@pytest.mark.parametrize("name", ["price", "s", "e", "date", "timeframe", "ticker"])
def test_not_timestamp_like(name: str) -> None:
    assert not is_timestamp_like(name)


def test_every_declared_timestamp_field_of_every_dataset_has_a_unit() -> None:
    table = TimeUnitTable(declared_units(CATALOG.by_name))
    for spec in CATALOG.by_name.values():
        for key_map_transport, key_map in spec.key_maps.items():
            for key in key_map:
                if is_timestamp_like(key):
                    assert TimeField(spec.name, key_map_transport, key) in table
    for field in declared_units(CATALOG.by_name):
        assert field in table


def test_fmv_threshold_is_ten_to_the_seventeenth() -> None:
    assert FMV_NS_THRESHOLD == 10**17
