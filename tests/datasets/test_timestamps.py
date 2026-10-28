"""Tests for the unit of each epoch field and the timestamp routine."""

import logging
from datetime import UTC, date, datetime, time, tzinfo
from typing import Any, cast
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from deephaven_massive import _errors
from deephaven_massive._datasets.timestamps import (
    FMV_NS_THRESHOLD,
    MAX_INSTANT_NS,
    MIN_INSTANT_NS,
    TimeUnit,
    UnitMismatchError,
    day_starts,
    instants,
    masked_instants,
    mismatch_message,
    time_unit,
    to_epoch_ns,
    to_epoch_ns_array,
)
from deephaven_massive._errors import MassiveError
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
                    assert time_unit(found, field) in set(TimeUnit)


def test_the_fixed_units_are_the_wire_units() -> None:
    assert {TimeUnit(u.value) for u in WireUnit} == {S, MS, US, NS}


# --- The routine -----------------------------------------------------------------------------


def test_every_fixed_unit_scales_to_nanoseconds() -> None:
    assert to_epoch_ns("x.s", S, SECONDS) == NANOS
    assert to_epoch_ns("x.ms", MS, SECONDS * 10**3) == NANOS
    assert to_epoch_ns("x.us", US, SECONDS * 10**6) == NANOS
    assert to_epoch_ns("x.ns", NS, NANOS) == NANOS


def test_fmv_rule_classifies_at_the_threshold(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_errors, "_logged", set())
    with caplog.at_level(logging.INFO, logger="deephaven_massive._datasets.timestamps"):
        # The documentation sample, 13 digits: milliseconds.
        assert to_epoch_ns("stocks.fmv.t", FMV, 1678220098130) == 1678220098130 * 10**6
        assert to_epoch_ns("stocks.fmv.t", FMV, NANOS) == NANOS
    detected = [r.getMessage() for r in caplog.records if "FMV timestamps" in r.getMessage()]
    assert detected == ["FMV timestamps detected in ms (magnitude rule)"]
    # The threshold itself is nanoseconds (1973, so out of bounds); just below it is
    # milliseconds, which overflows.
    with pytest.raises(MassiveError, match=rf"{FMV_NS_THRESHOLD} ms\|ns is outside 1990 to 2100"):
        to_epoch_ns("stocks.fmv.t", FMV, FMV_NS_THRESHOLD)
    with pytest.raises(MassiveError, match=rf"{FMV_NS_THRESHOLD - 1} ms overflows"):
        to_epoch_ns("stocks.fmv.t", FMV, FMV_NS_THRESHOLD - 1)


def test_fmv_rule_logs_only_a_decided_unit(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_errors, "_logged", set())
    with caplog.at_level(logging.INFO, logger="deephaven_massive._datasets.timestamps"):
        assert to_epoch_ns_array("x", FMV, ints(None, None)).to_pylist() == [None, None]
        assert not [r for r in caplog.records if "FMV timestamps detected" in r.getMessage()]
        to_epoch_ns_array("x", FMV, ints(NANOS))
    detected = [r.getMessage() for r in caplog.records if "FMV timestamps" in r.getMessage()]
    assert detected == ["FMV timestamps detected in ns (magnitude rule)"]


def test_array_conversions() -> None:
    assert to_epoch_ns_array("x", MS, ints(SECONDS * 1_000, None)).to_pylist() == [NANOS, None]
    assert to_epoch_ns_array("x", NS, pa.array([NANOS], pa.uint64())).to_pylist() == [NANOS]
    fmv = ints(NANOS, 1678220098130, None)
    assert to_epoch_ns_array("x", FMV, fmv).to_pylist() == [NANOS, 1678220098130 * 10**6, None]


def test_the_bound_includes_1990_and_all_of_2100() -> None:
    assert datetime.fromtimestamp(MIN_INSTANT_NS / 10**9, UTC) == datetime(1990, 1, 1, tzinfo=UTC)
    assert datetime.fromtimestamp(MAX_INSTANT_NS / 10**9, UTC) == datetime(2101, 1, 1, tzinfo=UTC)
    assert to_epoch_ns("x", NS, MIN_INSTANT_NS) == MIN_INSTANT_NS
    assert to_epoch_ns("x", NS, MAX_INSTANT_NS - 1) == MAX_INSTANT_NS - 1
    with pytest.raises(MassiveError, match=rf"{MIN_INSTANT_NS - 1} ns is outside 1990 to 2100"):
        to_epoch_ns("x", NS, MIN_INSTANT_NS - 1)
    with pytest.raises(MassiveError, match=rf"{MAX_INSTANT_NS} ns is outside"):
        to_epoch_ns("x", NS, MAX_INSTANT_NS)
    seconds = MAX_INSTANT_NS // 10**9
    assert to_epoch_ns("x", S, seconds - 1) == MAX_INSTANT_NS - 10**9
    with pytest.raises(MassiveError, match=rf"{seconds} s is outside"):
        to_epoch_ns("x", S, seconds)


def test_milliseconds_read_as_nanoseconds_fail_the_bound() -> None:
    # A 13-digit millisecond value read as nanoseconds is in January 1970.
    with pytest.raises(
        UnitMismatchError,
        match=r"^timestamp unit mismatch for stocks\.trades\.sip_timestamp: "
        r"1700000000000 ns is outside 1990 to 2100$",
    ):
        to_epoch_ns_array(TRADES_SIP, NS, ints(NANOS, SECONDS * 1_000, 5))


def test_nanoseconds_read_as_milliseconds_overflow() -> None:
    with pytest.raises(
        MassiveError,
        match=rf"^timestamp unit mismatch for stocks\.trades\.t: {NANOS} ms overflows "
        r"64-bit nanoseconds$",
    ):
        to_epoch_ns_array(TRADES_T, MS, ints(SECONDS * 1_000, NANOS))
    # A negative value overflows too, and the message names the first offending value.
    with pytest.raises(MassiveError, match=rf"{-NANOS} ms overflows"):
        to_epoch_ns_array(TRADES_T, MS, ints(None, -NANOS, NANOS))


def test_seconds_read_as_microseconds_fail_the_bound() -> None:
    with pytest.raises(MassiveError, match=rf"{SECONDS} us is outside"):
        to_epoch_ns("x", US, SECONDS)


def test_values_beyond_64_bits_are_rejected() -> None:
    with pytest.raises(
        MassiveError, match=rf"^timestamp unit mismatch for stocks\.trades\.t: {2**64} does not"
    ):
        to_epoch_ns(TRADES_T, MS, 2**64)


def test_nulls_pass_the_bound() -> None:
    assert to_epoch_ns_array("x", NS, ints(None)).to_pylist() == [None]
    assert to_epoch_ns_array("x", NS, ints()).to_pylist() == []


def test_the_routine_turns_epoch_zero_into_null_outside_the_time_column() -> None:
    values = ints(NANOS, 0, None)
    assert instants(TRADES_SIP, NS, values, time_column=False).to_pylist() == [NANOS, None, None]
    assert instants(TRADES_T, MS, ints(0, SECONDS * 1_000), time_column=False).to_pylist() == [
        None,
        NANOS,
    ]
    # The time column keeps its 0, which then fails the bound.
    with pytest.raises(MassiveError, match=r"stocks\.trades\.sip_timestamp: 0 ns is outside"):
        instants(TRADES_SIP, NS, values, time_column=True)
    assert instants(TRADES_SIP, NS, ints(NANOS), time_column=True).to_pylist() == [NANOS]


def test_the_routine_bounds_every_column() -> None:
    with pytest.raises(MassiveError, match="5 ns is outside"):
        instants("x", NS, ints(0, 5), time_column=False)


def test_fmv_threshold_is_ten_to_the_seventeenth() -> None:
    assert FMV_NS_THRESHOLD == 10**17


def test_a_unit_mismatch_is_a_massive_error() -> None:
    # Callers that catch MassiveError see it; a transport catches the subclass to name the file.
    assert issubclass(UnitMismatchError, MassiveError)
    with pytest.raises(UnitMismatchError, match="overflows"):
        to_epoch_ns_array(TRADES_T, MS, ints(NANOS))


def test_the_masked_routine_reports_failing_rows_instead_of_raising() -> None:
    sent = pa.array([1_700_000_000_000, 0, None, 5, 2**62, -(2**62)], pa.int64())
    nanos, failed = masked_instants(TimeUnit.MILLISECONDS, sent, time_column=True)
    assert nanos.to_pylist() == [1_700_000_000_000_000_000, None, None, None, None, None]
    assert failed.to_pylist() == [False, True, False, True, True, True]
    nanos, failed = masked_instants(TimeUnit.MILLISECONDS, sent, time_column=False)
    assert failed.to_pylist() == [False, False, False, True, True, True]  # 0 is absent there


def test_the_masked_routine_applies_the_fmv_rule(caplog: pytest.LogCaptureFixture) -> None:
    sent = pa.array([1_700_000_000_000, 1_700_000_000_000_000_000, 5, None], pa.int64())
    nanos, failed = masked_instants(TimeUnit.FMV_MAGNITUDE, sent, time_column=True)
    assert nanos.to_pylist() == [1_700_000_000_000_000_000] * 2 + [None, None]
    assert failed.to_pylist() == [False, False, True, False]
    _, failed = masked_instants(
        TimeUnit.FMV_MAGNITUDE, pa.array([None], pa.int64()), time_column=True
    )
    assert failed.to_pylist() == [False]


def test_a_masked_failure_reads_as_the_routine_would_raise_it() -> None:
    assert mismatch_message("t.T", TimeUnit.MILLISECONDS, 5) == (
        "timestamp unit mismatch for t.T: 5 ms is outside 1990 to 2100"
    )
    assert mismatch_message("t.T", TimeUnit.FMV_MAGNITUDE, 2**62) == (
        "timestamp unit mismatch for t.T: 4611686018427387904 ms|ns is outside 1990 to 2100"
    )
    assert mismatch_message("t.T", TimeUnit.SECONDS, 2**40).endswith("overflows 64-bit nanoseconds")


DAY_NS = 86_400 * 10**9
HOUR_NS = 3600 * 10**9
MS_NS = 10**6
MARCH_3 = 1_740_960_000 * 10**9  # 2025-03-03T00:00Z
NEW_YORK = ZoneInfo("America/New_York")
CLOSE = time(16)
LAST_MS = time(23, 59, 59, 999_000)


def stamps(*values: int | None) -> pa.Array:
    return pa.array(values, pa.timestamp("ns", "UTC"))


def ns(moment: datetime) -> int:
    return int(moment.timestamp()) * 10**9 + moment.microsecond * 1000


def starts(values: pa.Array, zone: tzinfo, *known: time, what: str = "1-day bar") -> list[Any]:
    moved = day_starts(
        "x.bars", values, zone=zone, known=known, source="src", what=what, remedy="r"
    )
    assert moved.type == values.type
    return cast("list[Any]", moved.cast(pa.int64()).to_pylist())


def test_utc_day_stamps_move_each_known_stamp_to_its_days_start() -> None:
    # The aggregates' 00:00 UTC stays; day_aggs_v1's 23:59:59.999 UTC moves to its day's start.
    last_ms = MARCH_3 + DAY_NS - MS_NS
    assert starts(stamps(MARCH_3, last_ms, None), UTC, LAST_MS) == [MARCH_3, MARCH_3, None]
    # A stamp anywhere in the first or last millisecond counts as that boundary.
    edge = stamps(MARCH_3 + MS_NS - 1, MARCH_3 + DAY_NS - 1)
    assert starts(edge, UTC, LAST_MS) == [MARCH_3, MARCH_3]


def test_new_york_stamps_move_by_wall_clock_across_daylight_saving_time() -> None:
    # 16:00 New York is 21:00 UTC in winter and 20:00 UTC in summer; both are the close.
    winter, summer = date(2025, 3, 7), date(2025, 3, 10)  # the clocks change on 03-09
    days = [datetime.combine(d, time(), NEW_YORK) for d in (winter, summer)]
    closes = [datetime.combine(d, CLOSE, NEW_YORK) for d in (winter, summer)]
    values = stamps(*(ns(moment) for moment in [*days, *closes]))
    assert starts(values, NEW_YORK, CLOSE) == [ns(day) for day in days * 2]
    # Midnight in Chicago is 01:00 New York time all year.
    chicago = datetime(2025, 7, 1, tzinfo=ZoneInfo("America/Chicago"))
    expected = ns(datetime(2025, 7, 1, tzinfo=NEW_YORK))
    assert starts(stamps(ns(chicago)), NEW_YORK, time(1)) == [expected]


@pytest.mark.parametrize(
    ("zone", "known", "offset"),
    [
        (UTC, (LAST_MS,), MS_NS),
        (UTC, (LAST_MS,), 5 * HOUR_NS),  # midnight New York time: another day cut
        (UTC, (LAST_MS,), DAY_NS - MS_NS - 1),
        (UTC, (), DAY_NS - MS_NS),  # a longer bar must start at 00:00
        (NEW_YORK, (CLOSE,), 5 * HOUR_NS + 14 * HOUR_NS),  # 14:00, no known stamp
        (NEW_YORK, (CLOSE,), 5 * HOUR_NS + 16 * HOUR_NS + MS_NS),  # a millisecond after 16:00
        (NEW_YORK, (CLOSE,), 0),  # 19:00 New York time, the day before
        (NEW_YORK, (), 5 * HOUR_NS + 16 * HOUR_NS),  # a longer bar at the close
    ],
)
def test_an_unknown_stamp_fails_loudly(zone: tzinfo, known: tuple[time, ...], offset: int) -> None:
    with pytest.raises(
        MassiveError, match=r"^crypto\.bars: a bar from /v2/aggs/x is stamped 2025-03-0"
    ) as caught:
        day_starts(
            "crypto.bars",
            stamps(MARCH_3 + 5 * HOUR_NS, MARCH_3 + offset),
            zone=zone,
            known=known,
            source="/v2/aggs/x",
            what="bar",
            remedy="read the flat files",
        )
    message = str(caught.value)
    assert f" in {zone}), which is not a time of day Massive is known to stamp such a bar at (" in (
        message
    )
    assert message.endswith(
        "; read the flat files, and report the stamp so the day cut can be settled"
    )


def test_the_refusal_names_the_stamp_in_both_zones_and_the_accepted_times() -> None:
    close = datetime(2024, 11, 29, 14, 30, tzinfo=NEW_YORK)
    with pytest.raises(MassiveError) as caught:
        day_starts(
            "stocks.bars",
            stamps(ns(close)),
            zone=NEW_YORK,
            known=(CLOSE,),
            source="the flat files us_stocks_sip/day_aggs_v1",
            what="1-day bar",
            remedy="read REST",
        )
    assert str(caught.value) == (
        "stocks.bars: a 1-day bar from the flat files us_stocks_sip/day_aggs_v1 is stamped "
        "2024-11-29T19:30:00+00:00 (2024-11-29T14:30:00-05:00 in America/New_York), which is "
        "not a time of day Massive is known to stamp such a bar at (00:00:00.000, 16:00:00.000 "
        "in America/New_York), so it may cover other days than the one it would be labelled "
        "with; read REST, and report the stamp so the day cut can be settled"
    )
