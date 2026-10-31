"""Tests for the unit of each epoch field and the timestamp routine."""

import logging
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from deephaven_massive import _log_limits
from deephaven_massive._datasets.timestamps import (
    FMV_NS_THRESHOLD,
    MAX_INSTANT_NS,
    MIN_INSTANT_NS,
    TimeUnit,
    first_failure,
    masked_instants,
    mismatch_message,
    time_unit,
)
from deephaven_massive._massive import Transport, WireType, WireUnit, source, sources

MS, NS, US, S, FMV = (
    TimeUnit.MILLISECONDS,
    TimeUnit.NANOSECONDS,
    TimeUnit.MICROSECONDS,
    TimeUnit.SECONDS,
    TimeUnit.FMV_MAGNITUDE,
)
TRADES_T = "stocks.trades.t"
TRADES_SIP = "stocks.trades.sip_timestamp"

# 2023-11-14T22:13:20Z
SECONDS = 1_700_000_000
NANOS = SECONDS * 10**9


def ints(*values: int | None) -> pa.Array:
    return pa.array(values, pa.int64())


def one(unit: TimeUnit, value: int) -> int:
    """Convert one epoch value of a time column through the routine; it must not fail.

    Returns:
        The nanoseconds.
    """
    nanos, failed = masked_instants(unit, ints(value), time_column=True)
    assert failed.to_pylist() == [False]
    converted: int = nanos[0].as_py()
    return converted


def failure(label: str, unit: TimeUnit, values: pa.Array, *, time_column: bool = True) -> str:
    """Run a column through the routine and word its first failure.

    Returns:
        The message of the first failing value, or `""` when none fails.
    """
    _, failed = masked_instants(unit, values, time_column=time_column)
    first = first_failure(values, failed)
    return "" if first is None else mismatch_message(label, unit, first)


# --- Units -----------------------------------------------------------------------------------


def test_each_field_takes_its_generated_unit() -> None:
    stream = source(Transport.WS, "stocks/T")
    files = source(Transport.FLAT_FILES, "us_stocks_sip/trades_v1")
    assert time_unit(stream, stream.field("SipTimestamp")) is MS
    assert time_unit(files, files.field("SipTimestamp")) is NS


@pytest.mark.parametrize("cluster", ["stocks", "options", "forex", "crypto"])
def test_the_fmv_channels_take_the_magnitude_rule(cluster: str) -> None:
    fmv = source(Transport.WS, f"{cluster}/FMV")
    epochs = [f for f in fmv.fields if f.wire_type is WireType.EPOCH]
    assert [time_unit(fmv, f) for f in epochs] == [FMV]


def test_rest_fmv_times_keep_their_unit() -> None:
    # Only the stream's FMV time is in doubt; the snapshots' fmv_last_updated is documented.
    snapshot = source(Transport.REST, "/v3/snapshot")
    assert time_unit(snapshot, snapshot.field("FmvLastUpdated")) is NS


def test_a_field_that_is_not_an_epoch_has_no_unit() -> None:
    stream = source(Transport.WS, "stocks/T")
    with pytest.raises(ValueError, match=r"^stocks/T field 'p' is not an epoch timestamp$"):
        time_unit(stream, stream.field("Price"))


def test_every_generated_epoch_has_a_unit_of_the_routine() -> None:
    for transport in Transport:
        for found in sources(transport).values():
            for field in found.fields:
                if field.wire_type is WireType.EPOCH:
                    assert field.unit is not None, (found.key, field.wire_key)
                    time_unit(found, field)


def test_the_fixed_units_are_the_wire_units() -> None:
    assert {TimeUnit(u.value) for u in WireUnit} == {S, MS, US, NS}


# --- The routine -----------------------------------------------------------------------------


def test_every_fixed_unit_scales_to_nanoseconds() -> None:
    assert one(S, SECONDS) == NANOS
    assert one(MS, SECONDS * 10**3) == NANOS
    assert one(US, SECONDS * 10**6) == NANOS
    assert one(NS, NANOS) == NANOS


def test_fmv_rule_classifies_at_the_threshold(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", set())
    with caplog.at_level(logging.INFO, logger="deephaven_massive._datasets.timestamps"):
        # The documentation sample, 13 digits: milliseconds.
        assert one(FMV, 1678220098130) == 1678220098130 * 10**6
        assert one(FMV, NANOS) == NANOS
    detected = [r.getMessage() for r in caplog.records if "FMV timestamps" in r.getMessage()]
    assert detected == ["FMV timestamps detected in ms (magnitude rule)"]
    # The threshold itself is nanoseconds (1973, so out of bounds); just below it is
    # milliseconds, which overflows.
    assert failure("f", FMV, ints(FMV_NS_THRESHOLD)).endswith(
        f"{FMV_NS_THRESHOLD} ms|ns is outside 1990 to 2100"
    )
    assert failure("f", FMV, ints(FMV_NS_THRESHOLD - 1)).endswith(
        f"{FMV_NS_THRESHOLD - 1} ms overflows 64-bit nanoseconds"
    )


def test_fmv_rule_logs_only_a_decided_unit(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_log_limits, "_logged", set())
    with caplog.at_level(logging.INFO, logger="deephaven_massive._datasets.timestamps"):
        nanos, _ = masked_instants(FMV, ints(None, None), time_column=True)
        assert nanos.to_pylist() == [None, None]
        assert not [r for r in caplog.records if "FMV timestamps detected" in r.getMessage()]
        one(FMV, NANOS)
    detected = [r.getMessage() for r in caplog.records if "FMV timestamps" in r.getMessage()]
    assert detected == ["FMV timestamps detected in ns (magnitude rule)"]


def test_array_conversions() -> None:
    def nanos(unit: TimeUnit, values: pa.Array) -> list[int | None]:
        converted, _ = masked_instants(unit, values, time_column=True)
        found: list[int | None] = converted.to_pylist()
        return found

    assert nanos(MS, ints(SECONDS * 1_000, None)) == [NANOS, None]
    assert nanos(NS, pa.array([NANOS], pa.uint64())) == [NANOS]
    fmv = ints(NANOS, 1678220098130, None)
    assert nanos(FMV, fmv) == [NANOS, 1678220098130 * 10**6, None]


def test_the_bound_includes_1990_and_all_of_2100() -> None:
    assert datetime.fromtimestamp(MIN_INSTANT_NS / 10**9, UTC) == datetime(1990, 1, 1, tzinfo=UTC)
    assert datetime.fromtimestamp(MAX_INSTANT_NS / 10**9, UTC) == datetime(2101, 1, 1, tzinfo=UTC)
    assert one(NS, MIN_INSTANT_NS) == MIN_INSTANT_NS
    assert one(NS, MAX_INSTANT_NS - 1) == MAX_INSTANT_NS - 1
    assert failure("x", NS, ints(MIN_INSTANT_NS - 1)).endswith(
        f"{MIN_INSTANT_NS - 1} ns is outside 1990 to 2100"
    )
    assert failure("x", NS, ints(MAX_INSTANT_NS)).endswith(
        f"{MAX_INSTANT_NS} ns is outside 1990 to 2100"
    )
    seconds = MAX_INSTANT_NS // 10**9
    assert one(S, seconds - 1) == MAX_INSTANT_NS - 10**9
    assert failure("x", S, ints(seconds)).endswith(f"{seconds} s is outside 1990 to 2100")


def test_milliseconds_read_as_nanoseconds_fail_the_bound() -> None:
    # A 13-digit millisecond value read as nanoseconds is in January 1970.
    assert failure(TRADES_SIP, NS, ints(NANOS, SECONDS * 1_000, 5)) == (
        "timestamp unit mismatch for stocks.trades.sip_timestamp: "
        "1700000000000 ns is outside 1990 to 2100"
    )


def test_nanoseconds_read_as_milliseconds_overflow() -> None:
    assert failure(TRADES_T, MS, ints(SECONDS * 1_000, NANOS)) == (
        f"timestamp unit mismatch for stocks.trades.t: {NANOS} ms overflows 64-bit nanoseconds"
    )
    # A negative value overflows too, and the message names the first offending value.
    assert failure(TRADES_T, MS, ints(None, -NANOS, NANOS)).endswith(
        f"{-NANOS} ms overflows 64-bit nanoseconds"
    )


def test_seconds_read_as_microseconds_fail_the_bound() -> None:
    assert failure("x", US, ints(SECONDS)).endswith(f"{SECONDS} us is outside 1990 to 2100")


def test_nulls_pass_the_bound() -> None:
    assert failure("x", NS, ints(None)) == ""
    assert failure("x", NS, ints()) == ""


def test_the_routine_turns_epoch_zero_into_null_outside_the_time_column() -> None:
    values = ints(NANOS, 0, None)
    nanos, failed = masked_instants(NS, values, time_column=False)
    assert (nanos.to_pylist(), failed.to_pylist()) == ([NANOS, None, None], [False] * 3)
    nanos, _ = masked_instants(MS, ints(0, SECONDS * 1_000), time_column=False)
    assert nanos.to_pylist() == [None, NANOS]
    # The time column keeps its 0, which then fails the bound.
    assert failure(TRADES_SIP, NS, values).endswith("sip_timestamp: 0 ns is outside 1990 to 2100")


def test_the_routine_bounds_every_column() -> None:
    assert failure("x", NS, ints(0, 5), time_column=False).endswith("5 ns is outside 1990 to 2100")


def test_the_first_failure_is_the_value_as_sent() -> None:
    values = pa.array([NANOS, 5, 7], pa.uint64())
    _, failed = masked_instants(NS, values, time_column=True)
    assert first_failure(values, failed) == 5
    _, failed = masked_instants(NS, ints(NANOS), time_column=True)
    assert first_failure(ints(NANOS), failed) is None


def test_the_masked_routine_reports_failing_rows() -> None:
    sent = pa.array([1_700_000_000_000, 0, None, 5, 2**62, -(2**62)], pa.int64())
    nanos, failed = masked_instants(TimeUnit.MILLISECONDS, sent, time_column=True)
    assert nanos.to_pylist() == [1_700_000_000_000_000_000, None, None, None, None, None]
    assert failed.to_pylist() == [False, True, False, True, True, True]
    nanos, failed = masked_instants(TimeUnit.MILLISECONDS, sent, time_column=False)
    assert failed.to_pylist() == [False, False, False, True, True, True]  # 0 is absent there


def test_the_masked_routine_applies_the_fmv_rule() -> None:
    sent = pa.array([1_700_000_000_000, 1_700_000_000_000_000_000, 5, None], pa.int64())
    nanos, failed = masked_instants(TimeUnit.FMV_MAGNITUDE, sent, time_column=True)
    assert nanos.to_pylist() == [1_700_000_000_000_000_000] * 2 + [None, None]
    assert failed.to_pylist() == [False, False, True, False]
    _, failed = masked_instants(
        TimeUnit.FMV_MAGNITUDE, pa.array([None], pa.int64()), time_column=True
    )
    assert failed.to_pylist() == [False]


def test_a_failure_is_worded_with_the_field_unit_and_value() -> None:
    assert mismatch_message("t.T", TimeUnit.MILLISECONDS, 5) == (
        "timestamp unit mismatch for t.T: 5 ms is outside 1990 to 2100"
    )
    assert mismatch_message("t.T", TimeUnit.FMV_MAGNITUDE, 2**62) == (
        "timestamp unit mismatch for t.T: 4611686018427387904 ms|ns is outside 1990 to 2100"
    )
    assert mismatch_message("t.T", TimeUnit.SECONDS, 2**40).endswith("overflows 64-bit nanoseconds")
