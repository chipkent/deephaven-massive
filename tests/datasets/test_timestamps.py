"""Tests for the declared timestamp units and their conversions."""

import logging
import re
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from deephaven_massive import _errors
from deephaven_massive._datasets.catalog import unit_rules
from deephaven_massive._datasets.timestamps import (
    FMV_NS_THRESHOLD,
    TimeField,
    TimeUnit,
    TimeUnitTable,
    UnitRule,
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
NS = TimeField("stocks.trades", Transport.REST, "sip_timestamp")
FMV = TimeField("stocks.fmv", Transport.WS, "t")
FOREX_LAST = TimeField("forex.quotes", Transport.REST, "timestamp")

TABLE = TimeUnitTable(
    {
        MS: UnitRule(TimeUnit.MILLISECONDS),
        NS: UnitRule(TimeUnit.NANOSECONDS),
        FMV: UnitRule(TimeUnit.FMV_MAGNITUDE),
        FOREX_LAST: UnitRule(TimeUnit.MILLISECONDS, unverified=True),
    }
)

# 2023-11-14T22:13:20Z
SECONDS = 1_700_000_000


def test_table_membership() -> None:
    assert MS in TABLE
    assert FOREX_LAST in TABLE
    assert TimeField("x.y", Transport.WS, "t") not in TABLE


def test_milliseconds_and_nanoseconds() -> None:
    assert TABLE.to_epoch_ns(MS, SECONDS * 1_000) == SECONDS * 10**9
    assert TABLE.to_epoch_ns(NS, SECONDS * 10**9) == SECONDS * 10**9


def test_unregistered_fields_are_refused() -> None:
    with pytest.raises(LookupError, match=r"stocks\.trades\.q on WS"):
        TABLE.rule(TimeField("stocks.trades", Transport.WS, "q"))
    with pytest.raises(LookupError):
        TABLE.to_epoch_ns(TimeField("stocks.trades", Transport.WS, "q"), 1)


def test_fmv_rule_at_the_boundary(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_errors, "_logged", set())
    with caplog.at_level(logging.INFO, logger="deephaven_massive._datasets.timestamps"):
        assert TABLE.to_epoch_ns(FMV, FMV_NS_THRESHOLD) == FMV_NS_THRESHOLD
        # Just below the threshold is milliseconds; such a value overflows (see the test below).
        # The documentation sample, 13 digits: milliseconds.
        assert TABLE.to_epoch_ns(FMV, 1678220098130) == 1678220098130 * 10**6
    detected = [r for r in caplog.records if "FMV timestamps detected" in r.getMessage()]
    assert len(detected) == 1
    assert "in ns" in detected[0].getMessage()


def test_fmv_rule_logs_only_a_decided_unit(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_errors, "_logged", set())
    with caplog.at_level(logging.INFO, logger="deephaven_massive._datasets.timestamps"):
        nulls = pa.array([None, None], type=pa.int64())
        assert TABLE.to_epoch_ns_array(FMV, nulls).to_pylist() == [None, None]
        assert not [r for r in caplog.records if "FMV timestamps detected" in r.getMessage()]
        TABLE.to_epoch_ns_array(FMV, pa.array([1678220098130]))
    detected = [r.getMessage() for r in caplog.records if "FMV timestamps" in r.getMessage()]
    assert detected == ["FMV timestamps detected in ms (magnitude rule)"]


def test_unverified_range_check() -> None:
    assert TABLE.to_epoch_ns(FOREX_LAST, SECONDS * 1_000) == SECONDS * 10**9
    # Nanoseconds registered as milliseconds land far after 2100.
    with pytest.raises(MassiveError, match=r"timestamp unit mismatch for forex\.quotes\.timestamp"):
        TABLE.to_epoch_ns(FOREX_LAST, SECONDS * 10**9)
    # Seconds land in 1970.
    with pytest.raises(MassiveError):
        TABLE.to_epoch_ns(FOREX_LAST, SECONDS)


def test_verified_rules_skip_the_range_check() -> None:
    assert TABLE.to_epoch_ns(NS, 5) == 5


def test_array_conversions() -> None:
    values = pa.array([SECONDS * 1_000, None], pa.int64())
    assert TABLE.to_epoch_ns_array(MS, values).to_pylist() == [SECONDS * 10**9, None]
    assert TABLE.to_epoch_ns_array(NS, pa.array([7], pa.int32())).to_pylist() == [7]
    fmv = pa.array([FMV_NS_THRESHOLD, 1678220098130])
    assert TABLE.to_epoch_ns_array(FMV, fmv).to_pylist() == [
        FMV_NS_THRESHOLD,
        1678220098130 * 10**6,
    ]
    assert TABLE.to_epoch_ns_array(FMV, pa.array([1678220098130])).to_pylist() == [
        1678220098130 * 10**6
    ]


def test_array_range_check() -> None:
    good = pa.array([SECONDS * 1_000, None])
    assert TABLE.to_epoch_ns_array(FOREX_LAST, good).to_pylist() == [SECONDS * 10**9, None]
    assert TABLE.to_epoch_ns_array(FOREX_LAST, pa.array([None], pa.int64())).to_pylist() == [None]
    with pytest.raises(MassiveError, match="unit mismatch"):
        TABLE.to_epoch_ns_array(FOREX_LAST, pa.array([SECONDS * 1_000, SECONDS * 10**9]))
    with pytest.raises(MassiveError, match="unit mismatch"):
        TABLE.to_epoch_ns_array(FOREX_LAST, pa.array([SECONDS]))


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
    table = TimeUnitTable(unit_rules(CATALOG.by_name))
    for spec in CATALOG.by_name.values():
        for key_map_transport, key_map in spec.key_maps.items():
            for key in key_map:
                if is_timestamp_like(key):
                    assert TimeField(spec.name, key_map_transport, key) in table
    for field in unit_rules(CATALOG.by_name):
        assert field in table


def test_fmv_millisecond_overflow_is_rejected() -> None:
    # Just below the threshold is milliseconds; times 10**6 it exceeds a 64-bit nanosecond.
    with pytest.raises(MassiveError, match=r"timestamp unit mismatch for stocks\.fmv\.t"):
        TABLE.to_epoch_ns_array(FMV, pa.array([FMV_NS_THRESHOLD, FMV_NS_THRESHOLD - 1]))
    with pytest.raises(MassiveError, match="timestamp unit mismatch"):
        TABLE.to_epoch_ns(FMV, FMV_NS_THRESHOLD - 1)


def test_scalar_millisecond_overflow_is_rejected() -> None:
    with pytest.raises(MassiveError, match=r"timestamp unit mismatch for stocks\.trades\.t"):
        TABLE.to_epoch_ns(MS, 2**62)


def test_fmv_millisecond_value_that_overflows_is_rejected_on_both_paths() -> None:
    # Below the FMV threshold, so classified as milliseconds; 10**19 ns does not fit int64.
    value = 10_000_000_000_000
    with pytest.raises(MassiveError, match="timestamp unit mismatch"):
        TABLE.to_epoch_ns(FMV, value)
    with pytest.raises(MassiveError, match="timestamp unit mismatch"):
        TABLE.to_epoch_ns_array(FMV, pa.array([value]))


def test_values_beyond_64_bits_are_rejected() -> None:
    with pytest.raises(MassiveError, match="timestamp unit mismatch"):
        TABLE.to_epoch_ns(NS, 2**64)


def test_fmv_threshold_is_ten_to_the_seventeenth() -> None:
    assert FMV_NS_THRESHOLD == 10**17


def test_unverified_range_boundaries() -> None:
    key = TimeField("x.y", Transport.REST, "timestamp")
    table = TimeUnitTable({key: UnitRule(TimeUnit.NANOSECONDS, unverified=True)})
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    low = int((datetime(1990, 1, 1, tzinfo=UTC) - epoch).total_seconds()) * 10**9
    high = int((datetime(2101, 1, 1, tzinfo=UTC) - epoch).total_seconds()) * 10**9
    assert table.to_epoch_ns(key, low) == low  # 1990-01-01T00:00Z is inside
    assert table.to_epoch_ns(key, high - 1) == high - 1  # the last nanosecond of 2100 too
    with pytest.raises(MassiveError, match="timestamp unit mismatch"):
        table.to_epoch_ns(key, low - 1)
    with pytest.raises(MassiveError, match="timestamp unit mismatch"):
        table.to_epoch_ns(key, high)
