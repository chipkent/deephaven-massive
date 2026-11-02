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


def test_a_minute_file_holding_rest_s_minutes_agrees() -> None:
    found = sources.compare_minute_file(
        [_file_row(_EARLY), _file_row(_MIDDLE)], _ROUND_THE_CLOCK, _DAY
    )
    assert found.agrees
    assert (found.rows, found.first_ms, found.last_ms) == (2, _EARLY["t"], _MIDDLE["t"])


def test_a_minute_file_s_gaps_extras_and_other_prices_disagree() -> None:
    extra = _minute(_NEW_YORK[0] + _MINUTE, 11.0)
    found = sources.compare_minute_file(
        [_file_row(_MIDDLE, shift=1.0), _file_row(_LATE), _file_row(extra)],
        _ROUND_THE_CLOCK,
        _DAY,
    )
    assert not found.agrees
    assert found.lacks == (_EARLY["t"],)
    assert found.adds == (extra["t"],)
    assert found.outside == (_LATE["t"],)
    assert [stamp for stamp, _, _ in found.differing] == [_MIDDLE["t"]]


def test_an_empty_minute_file_does_not_agree() -> None:
    found = sources.compare_minute_file([], _ROUND_THE_CLOCK, _DAY)
    assert not found.agrees
    assert not sources.compare_minute_file([], [], _DAY).agrees  # nothing to compare
    assert (found.rows, found.first_ms, found.last_ms) == (0, None, None)


def test_a_file_row_without_prices_is_not_compared() -> None:
    row = {"window_start": str(_MIDDLE["t"] * 1_000_000), "open": None}
    assert sources.compare_minute_file([row], _ROUND_THE_CLOCK, _DAY).differing == ()


def _tally(index: str, *, window: str, differ: bool, disagree: bool) -> sources.IndexTally:
    tally = sources.IndexTally(index)
    tally.add_verdict(sources.RANGE_SOURCE, window, 0.0)
    tally.add_verdict("/prev", "no bar", None)
    if differ:
        tally.windows_differ_days.add(_DAY)
    tally.add_files(_DAY, day_file=not disagree, minute_file=None)
    return tally


def test_a_tally_counts_verdicts_errors_and_file_days() -> None:
    tally = sources.IndexTally("I:VIX")
    tally.add_verdict("range", "Chicago day", 1e-9)
    tally.add_verdict("range", "Chicago day", 1e-7)
    tally.add_verdict("range", "both days", None)
    tally.add_files(date(2026, 10, 26), day_file=None, minute_file=None)
    agreeing = sources.compare_minute_file([_file_row(_MIDDLE)], [_MIDDLE], _DAY)
    tally.add_files(date(2026, 10, 27), day_file=True, minute_file=agreeing)
    gap = sources.compare_minute_file([_file_row(_MIDDLE)], [_EARLY, _MIDDLE], _DAY)
    tally.add_files(date(2026, 10, 28), day_file=None, minute_file=gap)
    tally.add_files(date(2026, 10, 29), day_file=False, minute_file=None)
    missing = sources.compare_minute_file([], [_MIDDLE], _DAY)
    tally.add_files(date(2026, 10, 30), day_file=None, minute_file=missing)
    tally.windows_differ_days.add(_DAY)
    assert tally.verdicts["range"] == {"Chicago day": 2, "both days": 1}
    assert tally.largest == {"range": 1e-7}
    assert tally.compared_days == {date(2026, 10, d) for d in (27, 28, 29)}
    assert tally.disagreeing_days == {date(2026, 10, 28), date(2026, 10, 29)}
    assert tally.absent_days == {date(2026, 10, 30)}
    assert tally.range_window() == "Chicago day"
    assert tally.line() == (
        "I:VIX (around the clock): range: Chicago day 2, both days 1; largest relative error "
        "to the closest window: range 1.00e-07; file and REST disagree on 2 of 3 days; absent "
        "from the files on 1"
    )


def test_a_regular_hours_tally_cannot_tell_the_windows_apart() -> None:
    tally = _tally("I:SPX", window="both days", differ=False, disagree=False)
    assert tally.range_window() == "no day tells"
    assert tally.line().startswith("I:SPX (regular hours): range: both days 1; /prev: no bar 1")
    assert (
        sources.IndexTally("I:DJI")
        .line()
        .endswith(
            "largest relative error to the closest window: none; file and REST disagree on 0 of 0 "
            "days; absent from the files on 0"
        )
    )


def test_a_tally_of_both_windows_is_mixed() -> None:
    tally = _tally("I:BTC100", window="New York day", differ=True, disagree=False)
    assert tally.range_window() == "New York day"
    tally.add_verdict(sources.RANGE_SOURCE, "neither day", 0.1)
    assert tally.range_window() == "mixed or neither"


def test_the_summary_answers_each_question() -> None:
    tallies = [
        _tally("I:SPX", window="both days", differ=False, disagree=False),
        _tally("I:RUT", window="both days", differ=False, disagree=True),
        _tally("I:AAVE100", window="Chicago day", differ=True, disagree=True),
        _tally("I:BTC100", window="Chicago day", differ=True, disagree=False),
        _tally("I:VIX", window="New York day", differ=True, disagree=False),
    ]
    absent = sources.IndexTally("I:ETH100")
    absent.add_files(_DAY, day_file=None, minute_file=sources.compare_minute_file([], [], _DAY))
    tallies.append(absent)
    lines = sources.summary_lines(tallies)
    assert len(lines) == 9
    assert lines[6] == (
        "(a) REST day bars cover the Chicago day for 2 of 3 indices whose New York and Chicago "
        "days differ (New York day 1, mixed or neither 0); 3 regular-hours indices cannot tell "
        "the two apart"
    )
    assert lines[7] == (
        "(b) the files disagree with REST on some day for 1 of 3 regular-hours indices "
        "['I:RUT'] and 1 of 3 around-the-clock ones ['I:AAVE100']; absent from the files on "
        "some day: ['I:ETH100']"
    )
    assert (
        lines[8] == "(c) I:AAVE100's mismatch: isolated: no other around-the-clock index disagrees"
    )


@pytest.mark.parametrize(
    ("aave", "bitcoin", "verdict"),
    [
        (None, False, "not read: I:AAVE100 was not among the indices"),
        (False, False, "no mismatch seen on these days"),
        (True, True, "not isolated: other around-the-clock indices disagree too ['I:BTC100']"),
    ],
)
def test_the_aave_verdicts(aave: bool | None, bitcoin: bool, verdict: str) -> None:
    tallies = [_tally("I:BTC100", window="Chicago day", differ=True, disagree=bitcoin)]
    if aave is not None:
        tallies.append(_tally("I:AAVE100", window="Chicago day", differ=True, disagree=aave))
    assert sources.summary_lines(tallies)[-1] == f"(c) I:AAVE100's mismatch: {verdict}"
