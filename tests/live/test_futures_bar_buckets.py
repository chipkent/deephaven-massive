"""Tests of the futures bar bucket checks (`tests/_live/futures_bar_buckets.py`)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from tests._live import futures_bar_buckets as buckets
from tests._live.futures_bar_buckets import Bar, BucketInputs

_HOUR = 3_600 * 1_000_000_000
_DAY = 24 * _HOUR


def _at(day: date, hour: int = 0) -> int:
    return buckets.utc_midnight_ns(day) + hour * _HOUR


def _bar(start: int, label: str | None, base: float, *, volume: float = 10.0) -> Bar:
    return Bar(start, label, (base, base + 2, base - 2, base + 1), volume)


# Three ES-like sessions, each opening 22:00 UTC the day before its label, stamped at 00:00
# UTC of that day, each made of two minute bars.
_LABELS = ("2026-10-06", "2026-10-07", "2026-10-08")
_MINUTES = [
    _bar(_at(date(2026, 10, 5 + n), 22) + m * _HOUR, label, 100.0 + 10 * n + m, volume=5.0)
    for n, label in enumerate(_LABELS)
    for m in range(2)
]
_SESSIONS = [buckets.combined(_MINUTES[2 * n : 2 * n + 2]) for n in range(len(_LABELS))]
_SESSION_BARS = [
    Bar(_at(date(2026, 10, 5 + n)), s.label, s.prices, s.volume)
    for n, s in enumerate(_SESSIONS)
    if s is not None
]
_INPUTS = BucketInputs(
    sessions=_SESSION_BARS,
    sessions_from_ns=_at(date(2026, 10, 1)),
    minutes=_MINUTES,
    minutes_from_ns=_at(date(2026, 10, 1)),
    until_ns=_at(date(2026, 11, 1)),
)


def test_a_raw_row_reads_as_a_bar() -> None:
    row = {
        "window_start": 5,
        "session_end_date": "2026-10-06",
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 7,
    }
    assert buckets.bar_of(row) == Bar(5, "2026-10-06", (1.0, 2.0, 0.5, 1.5), 7.0)
    bare = {"window_start": 5, "open": 1, "high": 2, "low": 0.5, "close": 1.5}
    assert buckets.bar_of(bare) == Bar(5, None, (1.0, 2.0, 0.5, 1.5), 0.0)


def test_combined_bars_take_the_first_open_and_the_last_close() -> None:
    assert buckets.combined([]) is None
    both = buckets.combined(_MINUTES[:2])
    assert both == Bar(_MINUTES[0].start_ns, "2026-10-06", (100.0, 103.0, 98.0, 102.0), 10.0)
    assert both is not None
    assert buckets.bar_error(both, both) == 0
    assert buckets.bar_error(both, Bar(0, None, both.prices, 20.0)) == pytest.approx(0.5)


def test_stamps_and_days() -> None:
    stamp = buckets.utc_midnight_ns(date(2026, 10, 1))
    assert stamp == int(datetime(2026, 10, 1, tzinfo=UTC).timestamp()) * 1_000_000_000
    assert buckets.utc_day(stamp + _DAY - 1) == date(2026, 10, 1)
    assert buckets.stamp_text(stamp) == "2026-10-01T00:00Z (Wed 2026-09-30 19:00 Chicago)"


def test_even_and_odd_epoch_days() -> None:
    # 2026-10-01 is epoch day 20,727, odd.
    assert buckets.even_and_odd_days(date(2026, 10, 1)) == (date(2026, 10, 2), date(2026, 10, 1))
    assert buckets.even_and_odd_days(date(2026, 10, 2)) == (date(2026, 10, 2), date(2026, 10, 3))


@pytest.mark.parametrize(
    ("resolution", "end"),
    [
        ("1session", date(2026, 9, 2)),
        ("2session", date(2026, 9, 3)),
        ("1week", date(2026, 9, 8)),
        ("2week", date(2026, 9, 15)),
        ("1month", date(2026, 10, 1)),
        ("2month", date(2026, 11, 1)),
        ("1quarter", date(2026, 12, 1)),
        ("1year", date(2027, 9, 1)),
    ],
)
def test_bucket_ends_on_the_grid(resolution: str, end: date) -> None:
    assert buckets.bucket_end(date(2026, 9, 1), resolution) == end


def test_a_bucket_end_needs_a_session_or_longer() -> None:
    with pytest.raises(ValueError, match="1min"):
        buckets.bucket_end(date(2026, 9, 1), "1min")


def test_placements_name_the_day_of_the_first_minute() -> None:
    # ES: the first minute is 22:00 UTC, 17:00 Chicago, on the day stamped: both days agree.
    # ZC: the first minute is 00:00 UTC on the day after the Chicago evening it opens in.
    zc_minute = _bar(_at(date(2026, 10, 6)), "2026-10-06-ZC", 1.0)
    zc = _bar(_at(date(2026, 10, 6)), "2026-10-06-ZC", 1.0)
    odd = _bar(_at(date(2026, 10, 3)), "2026-10-07", 1.0)
    lonely = _bar(_at(date(2026, 10, 9)), "2026-10-09", 1.0)
    lines, verdict = buckets.placements([_SESSION_BARS[0], zc, odd, lonely], [*_MINUTES, zc_minute])
    assert "UTC and Chicago day" in lines[0]
    assert lines[1].endswith("start is 00:00 UTC of the first minute's UTC day")
    assert lines[2].endswith("neither day")
    assert lines[3].endswith("no minute")
    assert verdict == (
        "window_start is 00:00 UTC of the first minute bar's UTC day for 2 of 4 sessions, of "
        "its Chicago day for 1; no minute bar for 1"
    )


def test_a_two_session_bar_of_whole_sessions_matches_them() -> None:
    bar = buckets.combined(_SESSION_BARS[:2])
    assert bar is not None
    line, best = buckets.bucket_check("2session", bar, _INPUTS, read_from_ns=bar.start_ns)
    assert best == "whole sessions in its UTC days"
    assert "the 2 sessions from its start (2 bars): 0.00e+00, label is its last session" in line
    assert "minute bars of its UTC days (4 bars): 0.00e+00" in line
    assert "{from}" not in line


def test_a_bar_matching_no_candidate_names_the_last_session_of_each() -> None:
    bar = Bar(_SESSION_BARS[0].start_ns, "2026-10-07", (1.0, 1.0, 1.0, 1.0), 1.0)
    line, best = buckets.bucket_check("1week", bar, _INPUTS, read_from_ns=bar.start_ns)
    assert best == buckets.NO_MATCH
    assert "whole sessions in its UTC days (3 bars)" in line
    assert "its last session is 2026-10-08" in line
    assert "MATCH" not in line


def test_a_bar_clipped_at_its_from_matches_the_sessions_from_there() -> None:
    clipped = buckets.combined(_SESSION_BARS[1:])
    assert clipped is not None
    bar = Bar(_SESSION_BARS[0].start_ns, clipped.label, clipped.prices, clipped.volume)
    line, best = buckets.bucket_check("1week", bar, _INPUTS, read_from_ns=_SESSION_BARS[1].start_ns)
    assert best == "whole sessions from {from}"
    assert "label is its last session MATCH" in line


def test_a_bucket_past_the_reads_is_not_judged() -> None:
    bar = _bar(_at(date(2026, 10, 20)), None, 1.0)
    line, best = buckets.bucket_check("2month", bar, _INPUTS, read_from_ns=bar.start_ns)
    assert best == buckets.NOT_JUDGED
    assert line.endswith("not judged, the bucket ends after the reads")


def test_a_bar_before_the_shorter_reads_has_no_candidate() -> None:
    bar = _bar(_at(date(2026, 9, 1)), None, 1.0)
    line, best = buckets.bucket_check("1week", bar, _INPUTS, read_from_ns=bar.start_ns)
    assert best == buckets.NO_MATCH
    assert line.endswith("no shorter bars read")


def test_an_empty_candidate_is_named() -> None:
    bar = _bar(_at(date(2026, 10, 2)), None, 1.0)
    line, _ = buckets.bucket_check("1session", bar, _INPUTS, read_from_ns=bar.start_ns)
    assert "whole sessions in its UTC days: none" in line
    assert "the 1 sessions from its start (1 bars)" in line


def test_bucket_verdicts_count_each_best_candidate() -> None:
    verdicts = ["whole sessions in its UTC days"] * 2 + [buckets.NO_MATCH, buckets.NOT_JUDGED]
    assert buckets.bucket_verdict("2session", verdicts) == (
        "2session: whole sessions in its UTC days 2, no match 1, of 3 bars (1 not judged: they "
        "end after the reads)"
    )
    assert buckets.bucket_verdict("1week", []) == "1week: no bar judged, of 0 bars"


def test_grid_verdicts() -> None:
    a, b, c = (_bar(_at(date(2026, 10, d)), None, 1.0) for d in (4, 6, 7))
    after = _at(date(2026, 10, 3))
    assert buckets.grid_verdict([a, b], [a, b], after_ns=after).startswith("fixed grid")
    assert buckets.grid_verdict([a], [c], after_ns=after).startswith("anchored to {from}")
    assert buckets.grid_verdict([a, b], [a, c], after_ns=after).startswith("partly shared")
    assert buckets.grid_verdict([a], [], after_ns=after).startswith("no windows to compare")


def test_clipping_verdicts() -> None:
    whole = _bar(_at(date(2026, 9, 1)), None, 1.0)
    part = _bar(_at(date(2026, 9, 1)), None, 1.0, volume=4.0)
    later = _bar(_at(date(2026, 10, 1)), None, 1.0)
    assert buckets.clipping_verdict([whole], [whole]).startswith("the same bar")
    assert buckets.clipping_verdict([whole], [part]).startswith("clipped at {from}")
    found = buckets.clipping_verdict([whole], [later])
    assert found.startswith("no shared window: the reads start at 2026-09-01T00:00Z")
    assert buckets.clipping_verdict([], []).endswith("none and none")
