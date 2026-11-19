"""Tests of the index day-source comparisons (`tests/_live/index_day_sources.py`)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from tests._live import index_day_sources as sources
from tests._live.calendar import NEW_YORK

_DAY = date(2026, 10, 27)
_NEW_YORK = sources.day_window_ms(_DAY, NEW_YORK)
_CHICAGO = sources.day_window_ms(_DAY, sources.CHICAGO)
_MINUTE = 60_000


def _minute(stamp: int, price: float) -> dict[str, Any]:
    return {"t": stamp, "o": price, "h": price + 1, "l": price - 1, "c": price}


def _file_row(minute: dict[str, Any], *, shift: float = 0.0) -> dict[str, str]:
    values = {
        key: str(minute[short] + shift)
        for key, short in zip(("open", "high", "low", "close"), ("o", "h", "l", "c"), strict=True)
    }
    return {"window_start": str(minute["t"] * 1_000_000), **values}


# One minute in the New York day's first hour (outside the Chicago day), one in the middle,
# and one in the hour after it (inside the Chicago day only).
_EARLY = _minute(_NEW_YORK[0], 10.0)
_MIDDLE = _minute(_NEW_YORK[0] + 12 * 60 * _MINUTE, 20.0)
_LATE = _minute(_NEW_YORK[1], 30.0)
_ROUND_THE_CLOCK = [_EARLY, _MIDDLE, _LATE]


def test_crypto_choices_take_each_family_s_100_index_first() -> None:
    found = {
        "AAVE": [{"ticker": "I:AAVE400"}, {"ticker": "I:AAVE100"}, {"ticker": "I:AAVE10RP"}],
        "BTC": [{"ticker": "I:BTC10RP"}, {"ticker": "I:XBTC"}, {"name": "no ticker"}],
        "ETH": [],
    }
    assert sources.crypto_choices(found, each=2) == ["I:AAVE100", "I:AAVE10RP", "I:BTC10RP"]


def test_minute_reads_merge_touching_spans() -> None:
    days = [date(2026, 10, 30), date(2026, 9, 8), date(2026, 10, 26), date(2026, 10, 28)]
    assert sources.minute_read_spans(days) == [
        (date(2026, 9, 7), date(2026, 9, 9)),
        (date(2026, 10, 25), date(2026, 10, 31)),
    ]
    # The day before the 29th is the day after the 26th: touching spans merge too.
    touching = [date(2026, 10, 26), date(2026, 10, 29)]
    assert sources.minute_read_spans(touching) == [(date(2026, 10, 25), date(2026, 10, 30))]


def test_the_chicago_day_starts_an_hour_after_the_new_york_day() -> None:
    assert _CHICAGO[0] - _NEW_YORK[0] == 60 * _MINUTE
    assert _CHICAGO[1] - _NEW_YORK[1] == 60 * _MINUTE


def test_prices_read_rest_bars_and_file_text() -> None:
    assert sources.prices({"o": 1, "h": 2, "l": 0, "c": 1.5}) == (1.0, 2.0, 0.0, 1.5)
    row = {"open": "1", "high": "2", "low": "0", "close": "1.5"}
    assert sources.prices(row) == (1.0, 2.0, 0.0, 1.5)
    assert sources.prices({"open": "1", "high": "2", "low": None, "close": "1"}) is None
    assert sources.prices({"open": "1", "high": "", "low": "0", "close": "1"}) is None


def test_a_rebuild_takes_the_minutes_of_its_window() -> None:
    assert sources.rebuilt(_ROUND_THE_CLOCK, *_NEW_YORK) == (10.0, 21.0, 9.0, 20.0)
    assert sources.rebuilt(_ROUND_THE_CLOCK, *_CHICAGO) == (20.0, 31.0, 19.0, 30.0)
    assert sources.rebuilt([], *_CHICAGO) is None


def test_window_verdicts() -> None:
    new_york = sources.rebuilt(_ROUND_THE_CLOCK, *_NEW_YORK)
    chicago = sources.rebuilt(_ROUND_THE_CLOCK, *_CHICAGO)
    rebuilds = {"New York": new_york, "Chicago": chicago}
    assert sources.window_verdict(chicago, rebuilds) == ("Chicago day", 0.0)
    assert sources.window_verdict(new_york, rebuilds) == ("New York day", 0.0)
    assert sources.window_verdict(new_york, {"New York": new_york, "Chicago": new_york}) == (
        "both days",
        0.0,
    )
    verdict, error = sources.window_verdict((1.0, 1.0, 1.0, 1.0), rebuilds)
    assert verdict == "neither day"
    assert error == pytest.approx(20 / 21)  # the New York rebuild's high, 21 against 1
    assert sources.window_verdict(None, rebuilds) == ("no bar", None)
    assert sources.window_verdict(new_york, {"New York": None, "Chicago": None}) == (
        "no minute bars",
        None,
    )


def test_windows_differ_only_with_a_minute_in_the_hour_between_them() -> None:
    assert sources.windows_differ([_EARLY], _DAY)
    assert sources.windows_differ([_LATE], _DAY)
    assert not sources.windows_differ([_MIDDLE], _DAY)


def test_a_minute_file_holding_rest_s_minutes_has_no_gap() -> None:
    found = sources.compare_minute_file(
        [_file_row(_EARLY), _file_row(_MIDDLE)], _ROUND_THE_CLOCK, _DAY
    )
    assert not found.has_gaps
    assert not found.values_differ
    assert (found.rows, found.first_ms, found.last_ms) == (2, _EARLY["t"], _MIDDLE["t"])


def test_a_minute_file_s_gaps_extras_and_other_prices_are_found() -> None:
    extra = _minute(_NEW_YORK[0] + _MINUTE, 11.0)
    found = sources.compare_minute_file(
        [_file_row(_MIDDLE, shift=1.0), _file_row(_LATE), _file_row(extra)],
        _ROUND_THE_CLOCK,
        _DAY,
    )
    assert found.has_gaps
    assert found.values_differ
    assert found.lacks == (_EARLY["t"],)
    assert found.adds == (extra["t"],)
    assert found.outside == (_LATE["t"],)
    assert [stamp for stamp, _, _ in found.differing] == [_MIDDLE["t"]]


@pytest.mark.parametrize(
    "rows",
    [
        [_file_row(_EARLY)],  # lacks the middle minute
        [_file_row(_EARLY), _file_row(_MIDDLE), _file_row(_minute(_NEW_YORK[0] + 1, 1.0))],
        [_file_row(_EARLY), _file_row(_MIDDLE), _file_row(_LATE)],  # outside its day
    ],
)
def test_each_kind_of_gap_alone_is_a_gap(rows: list[dict[str, str]]) -> None:
    found = sources.compare_minute_file(rows, _ROUND_THE_CLOCK, _DAY)
    assert found.has_gaps
    assert not found.values_differ


def test_an_empty_minute_file_has_a_gap() -> None:
    found = sources.compare_minute_file([], _ROUND_THE_CLOCK, _DAY)
    assert found.has_gaps
    assert sources.compare_minute_file([], [], _DAY).has_gaps  # nothing to compare
    assert (found.rows, found.first_ms, found.last_ms) == (0, None, None)


def test_a_file_row_without_prices_is_not_compared() -> None:
    row = {"window_start": str(_MIDDLE["t"] * 1_000_000), "open": None}
    assert sources.compare_minute_file([row], _ROUND_THE_CLOCK, _DAY).differing == ()


_CLEAN = sources.compare_minute_file([_file_row(_MIDDLE)], [_MIDDLE], _DAY)
_GAP = sources.compare_minute_file([_file_row(_MIDDLE)], [_EARLY, _MIDDLE], _DAY)
_OTHER_PRICES = sources.compare_minute_file([_file_row(_MIDDLE, shift=1.0)], [_MIDDLE], _DAY)
_EMPTY = sources.compare_minute_file([], [_MIDDLE], _DAY)


def _tally(
    index: str,
    *,
    window: str,
    differ: bool,
    day_file: str,
    minute_file: sources.MinuteFileComparison = _CLEAN,
) -> sources.IndexTally:
    tally = sources.IndexTally(index)
    tally.add_verdict(sources.RANGE_SOURCE, window, 0.0)
    tally.add_verdict("/prev", "no bar", None)
    if differ:
        tally.windows_differ_days.add(_DAY)
    tally.add_files(_DAY, range_verdict=window, day_file=day_file, minute_file=minute_file)
    return tally


def test_a_tally_counts_each_kind_of_file_finding_apart() -> None:
    tally = sources.IndexTally("I:VIX")
    tally.add_verdict("range", "Chicago day", 1e-9)
    tally.add_verdict("range", "Chicago day", 1e-7)
    tally.add_verdict("range", "both days", None)
    chicago = "Chicago day"
    days = [date(2026, 10, d) for d in range(24, 32)]
    tally.add_files(days[0], range_verdict=chicago, day_file=None, minute_file=None)
    tally.add_files(days[1], range_verdict=chicago, day_file=chicago, minute_file=_CLEAN)
    tally.add_files(days[2], range_verdict=chicago, day_file="New York day", minute_file=None)
    tally.add_files(days[3], range_verdict=chicago, day_file="neither day", minute_file=None)
    tally.add_files(days[4], range_verdict=chicago, day_file=None, minute_file=_GAP)
    tally.add_files(days[5], range_verdict=chicago, day_file=None, minute_file=_OTHER_PRICES)
    tally.add_files(days[6], range_verdict=chicago, day_file="no bar", minute_file=_EMPTY)
    tally.add_files(days[7], range_verdict="both days", day_file="New York day", minute_file=None)
    tally.windows_differ_days.add(_DAY)
    assert tally.verdicts["range"] == {"Chicago day": 2, "both days": 1}
    assert tally.largest == {"range": 1e-7}
    assert tally.compared_days == set(days[1:6]) | {days[7]}
    assert tally.other_window_days == {days[2]}
    assert tally.no_window_days == {days[3]}
    assert tally.minute_gap_days == {days[4]}
    assert tally.minute_value_days == {days[5]}
    assert tally.wrong_value_days == {days[3], days[5]}
    assert tally.absent_days == {days[6]}
    assert tally.range_window() == "Chicago day"
    assert tally.line() == (
        "I:VIX (around the clock): range: Chicago day 2, both days 1; largest relative error "
        "to the closest window: range 1.00e-07; of 6 days with files: day file in another "
        "window than the range 1, in no window 1, minute gaps 1, minute values differ 1; "
        "absent from the files on 1"
    )


def test_a_regular_hours_tally_cannot_tell_the_windows_apart() -> None:
    tally = _tally("I:SPX", window="both days", differ=False, day_file="both days")
    assert tally.range_window() == "no day tells"
    assert tally.line().startswith("I:SPX (regular hours): range: both days 1; /prev: no bar 1")
    assert (
        sources.IndexTally("I:DJI")
        .line()
        .endswith(
            "largest relative error to the closest window: none; of 0 days with files: day file in "
            "another window than the range 0, in no window 0, minute gaps 0, minute values differ "
            "0; absent from the files on 0"
        )
    )


def test_a_tally_of_both_windows_is_mixed() -> None:
    tally = _tally("I:BTC100", window="New York day", differ=True, day_file="New York day")
    assert tally.range_window() == "New York day"
    tally.add_verdict(sources.RANGE_SOURCE, "neither day", 0.1)
    assert tally.range_window() == "mixed or neither"


def test_the_summary_answers_each_question() -> None:
    chicago, new_york = "Chicago day", "New York day"
    tallies = [
        _tally("I:SPX", window="both days", differ=False, day_file="both days"),
        _tally("I:RUT", window="both days", differ=False, day_file="both days", minute_file=_GAP),
        _tally("I:AAVE100", window=chicago, differ=True, day_file="neither day", minute_file=_GAP),
        # A day file of the New York day beside a Chicago range bar is a window, not bad data.
        _tally("I:BTC100", window=chicago, differ=True, day_file=new_york),
        _tally("I:VIX", window=new_york, differ=True, day_file=new_york),
    ]
    absent = sources.IndexTally("I:ETH100")
    absent.add_files(_DAY, range_verdict=chicago, day_file="no bar", minute_file=_EMPTY)
    tallies.append(absent)
    lines = sources.summary_lines(tallies)
    assert len(lines) == 9
    assert lines[6] == (
        "(a) REST day bars cover the Chicago day for 2 of 3 indices whose New York and Chicago "
        "days differ (New York day 1, mixed or neither 0); 3 regular-hours indices cannot tell "
        "the two apart"
    )
    assert lines[7] == (
        "(b) day file in another window than the range: regular hours [] of 3, around the clock "
        "['I:BTC100'] of 3; day file in no window: regular hours [] of 3, around the clock "
        "['I:AAVE100'] of 3; minute-file gaps: regular hours ['I:RUT'] of 3, around the clock "
        "['I:AAVE100'] of 3; minute-file prices differ: regular hours [] of 3, around the clock "
        "[] of 3; absent from the files: ['I:ETH100']"
    )
    assert lines[8] == (
        "(c) I:AAVE100's mismatch: isolated: no other around-the-clock index has wrong values; "
        "its minute files have gaps on 1 days"
    )


@pytest.mark.parametrize(
    ("aave", "bitcoin", "verdict"),
    [
        (None, _CLEAN, "not read: I:AAVE100 was not among the indices"),
        (_GAP, _CLEAN, "no wrong values seen on these days; its minute files have gaps on 1 days"),
        (
            _OTHER_PRICES,
            _OTHER_PRICES,
            (
                "not isolated: other around-the-clock indices have wrong values too "
                "['I:BTC100']; its minute files have gaps on 0 days"
            ),
        ),
    ],
)
def test_the_aave_verdicts(
    aave: sources.MinuteFileComparison | None,
    bitcoin: sources.MinuteFileComparison,
    verdict: str,
) -> None:
    chicago = "Chicago day"
    tallies = [
        _tally("I:BTC100", window=chicago, differ=True, day_file=chicago, minute_file=bitcoin)
    ]
    if aave is not None:
        tallies.append(
            _tally("I:AAVE100", window=chicago, differ=True, day_file=chicago, minute_file=aave)
        )
    assert sources.summary_lines(tallies)[-1] == f"(c) I:AAVE100's mismatch: {verdict}"


def test_the_open_close_must_match_the_chicago_day_where_the_days_differ() -> None:
    tally = sources.IndexTally("I:AAVE100")
    rebuilds = {"New York": (1.0, 2.0, 0.5, 1.5), "Chicago": (1.1, 2.0, 0.5, 1.6)}
    days = [date(2026, 11, d) for d in range(9, 15)]
    tally.windows_differ_days.update(days[:5])
    tally.add_open_close(days[0], "Chicago day", values=rebuilds["Chicago"], rebuilds=rebuilds)
    tally.add_open_close(days[1], "New York day", values=rebuilds["New York"], rebuilds=rebuilds)
    tally.add_open_close(days[2], "neither day", values=(9.0, 9.0, 9.0, 9.0), rebuilds=rebuilds)
    # No open and close, or no minutes to rebuild a day from, says nothing either way.
    tally.add_open_close(days[3], "no bar", values=None, rebuilds=rebuilds)
    tally.add_open_close(days[4], "no minute bars", values=(1.0, 2.0, 0.5, 1.5), rebuilds={})
    # A day whose windows hold the same minutes can't tell them apart.
    tally.add_open_close(days[5], "New York day", values=(1.0, 2.0, 0.5, 1.5), rebuilds=rebuilds)
    assert tally.open_close_judged == set(days[:3])
    rebuilt = "rebuilds {'New York': (1.0, 2.0, 0.5, 1.5), 'Chicago': (1.1, 2.0, 0.5, 1.6)}"
    alone = f"not the Chicago day alone; {rebuilt}"
    new_york = "the open and close (1.0, 2.0, 0.5, 1.5) matched New York day"
    neither = "the open and close (9.0, 9.0, 9.0, 9.0) matched neither day"
    assert tally.open_close_misses == [
        f"I:AAVE100 2026-11-10: {new_york}, {alone}",
        f"I:AAVE100 2026-11-11: {neither}, {alone}",
    ]


def test_an_open_close_whose_windows_rebuild_alike_is_not_judged() -> None:
    # The windows hold different minutes, but the extra hour moved no price: either window
    # gives the bar sent, so the day neither passes nor fails the check.
    tally = sources.IndexTally("I:BTC1RP")
    same = (1.0, 2.0, 0.5, 1.5)
    tally.windows_differ_days.add(_DAY)
    rebuilds = {"New York": same, "Chicago": same}
    tally.add_open_close(_DAY, "both days", values=same, rebuilds=rebuilds)
    assert tally.open_close_judged == set()
    assert tally.open_close_misses == []


def test_the_open_close_check_fails_on_a_miss_and_when_nothing_was_judged() -> None:
    passed = sources.IndexTally("I:AAVE100")
    passed.open_close_judged.add(_DAY)
    unjudged = sources.IndexTally("I:SPX")
    assert sources.open_close_failure([passed, unjudged]) is None
    assert sources.open_close_failure([unjudged, sources.IndexTally("I:NDX")]) == (
        "no day of ['I:SPX', 'I:NDX'] told the open and close's window apart: on no day did an "
        "index's New York and Chicago days rebuild to different prices with an open and close "
        "sent"
    )
    assert sources.open_close_failure([]) is not None
    missed = sources.IndexTally("I:BTC10RP")
    missed.open_close_judged.add(_DAY)
    missed.open_close_misses.extend(["first miss", "second miss"])
    assert sources.open_close_failure([passed, missed]) == (
        "the daily open and close is not the Chicago day:\nfirst miss\nsecond miss"
    )
