import copy
import typing
from collections.abc import Sequence
from datetime import date, datetime

import numpy as np
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._types import (
    ALL,
    DateLike,
    FilterValue,
    FuturesSpan,
    StreamSpan,
    Symbols,
    _All,
)


def test_all_is_the_exported_singleton() -> None:
    assert dm.ALL is ALL
    assert _All("ALL") is ALL
    assert copy.copy(ALL) is ALL
    assert copy.deepcopy(ALL) is ALL
    assert hash(ALL) == hash(_All.ALL)
    assert not isinstance(ALL, str)


def test_symbols_alias() -> None:
    assert set(typing.get_args(Symbols)) == {str, Sequence[str], Table, _All}


def test_date_and_filter_aliases() -> None:
    assert set(typing.get_args(DateLike)) == {str, date, datetime, np.datetime64}
    assert set(typing.get_args(FilterValue)) == {
        str,
        int,
        float,
        bool,
        date,
        datetime,
        np.datetime64,
        Sequence[str | int | float | date | np.datetime64],
    }


def test_bars_overload_aliases_are_enum_literals() -> None:
    assert typing.get_args(StreamSpan) == (dm.Timespan.SECOND, dm.Timespan.MINUTE)
    futures = set(typing.get_args(FuturesSpan))
    assert futures == set(dm.Timespan) - {dm.Timespan.DAY}


def test_all_shows_its_public_name() -> None:
    assert repr(ALL) == "dm.ALL"
    assert str(ALL) == "ALL"
    assert f"{ALL}" == "ALL"
