"""Tests for `m.stocks.financials`: the arguments its statements take.

The checks every dataset of the `m.stocks` tree shares (catalog gates, fixtures, list
arguments) are in `test_stocks.py`.
"""

import functools
from collections.abc import Callable

import pytest

import deephaven_massive as dm
from deephaven_massive.queries import Query


def factory(session: dm.MassiveSession, name: str) -> Callable[..., Query]:
    target: object = session.stocks
    for part in name.split("."):
        target = getattr(target, part)
    assert callable(target)
    method = target

    @functools.wraps(method)
    def make(*args: object, **kwargs: object) -> Query:
        query = method(*args, **kwargs)
        assert isinstance(query, Query)
        return query

    return make


@pytest.mark.parametrize(
    "name",
    [
        "financials.balance_sheets",
        "financials.income_statements",
        "financials.cash_flow_statements",
    ],
)
def test_statements_take_a_timeframe(massive_session: dm.MassiveSession, name: str) -> None:
    make = factory(massive_session, name)
    query = make("AAPL", timeframe=dm.Timeframe.QUARTERLY)
    assert dict(query._request.params) == {"timeframe": dm.Timeframe.QUARTERLY}
    assert make()._request.params == ()
    with pytest.raises(TypeError, match=r"timeframe must be dm\.Timeframe"):
        make(timeframe="annual")


@pytest.mark.parametrize(
    "name", ["financials.income_statements", "financials.cash_flow_statements"]
)
def test_flow_statements_offer_the_trailing_twelve_months(
    massive_session: dm.MassiveSession, name: str
) -> None:
    trailing = factory(massive_session, name)(timeframe=dm.Timeframe.TRAILING_TWELVE_MONTHS)
    assert dict(trailing._request.params) == {"timeframe": dm.Timeframe.TRAILING_TWELVE_MONTHS}


def test_balance_sheets_refuse_the_trailing_twelve_months(
    massive_session: dm.MassiveSession,
) -> None:
    with pytest.raises(ValueError, match="balance sheets have no trailing twelve months"):
        massive_session.stocks.financials.balance_sheets(
            timeframe=dm.Timeframe.TRAILING_TWELVE_MONTHS
        )
