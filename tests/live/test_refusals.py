"""Tests of how a live test words Massive's refusal (`tests/_live/refusals.py`)."""

from __future__ import annotations

from datetime import date

from deephaven_massive._errors import EntitlementError, ErrorSource
from deephaven_massive._massive import WireEntitlementError
from tests._live import refusals

_TIMEFRAME = (
    "Your plan doesn't include this data timeframe. Please upgrade your plan at "
    "https://massive.com/pricing"
)
"""Massive's text of a timeframe refusal."""


def _refusal(target: str, detail: str = _TIMEFRAME) -> WireEntitlementError:
    return WireEntitlementError(
        f"HTTP 403 from GET {target} on api.massive.com (request_id 1): {detail}", status=403
    )


def test_a_timeframe_refusal_is_matched_by_its_phrase() -> None:
    assert refusals.is_timeframe_refusal(_refusal("/v2/aggs/ticker/AAPL/prev"))
    assert refusals.is_timeframe_refusal(
        Exception("Your plan doesn\u2019t include this data timeframe")
    )
    assert not refusals.is_timeframe_refusal(_refusal("/v3/x?timeframe=DELAYED", "not entitled"))
    assert not refusals.is_timeframe_refusal(Exception("timeframe"))


def test_the_refused_request() -> None:
    option = "/v2/aggs/ticker/O%3ANVDA240621C00120000/range/1/day/2023-06-12/2023-06-05"
    assert refusals.refused_request(_refusal(option)) == ("options", date(2023, 6, 5))
    futures = "/futures/v1/aggs/ESZ3?window_start.gte=2023-10-28"
    assert refusals.refused_request(_refusal(futures)) == ("futures", date(2023, 10, 28))
    assert refusals.refused_request(_refusal("/v2/aggs/ticker/C%3AEURUSD/prev")) == (
        "currencies",
        None,
    )
    assert refusals.refused_request(Exception("no target")) == (None, None)


def test_a_refusal_line_keeps_the_message_on_one_line() -> None:
    error = _refusal("/v3/quotes/AAPL", "not\n  entitled")
    assert refusals.refusal_line(error) == (
        "HTTP 403 from GET /v3/quotes/AAPL on api.massive.com (request_id 1): not entitled"
    )


def test_a_refusal_line_names_a_status_its_message_leaves_out() -> None:
    s3 = WireEntitlementError("AccessDenied on us_stocks_sip/x.csv.gz", status=403)
    assert refusals.refusal_line(s3) == "HTTP 403: AccessDenied on us_stocks_sip/x.csv.gz"
    stream = EntitlementError("options.trades stream: not authorized", source=ErrorSource.MASSIVE)
    assert refusals.refusal_line(stream) == "options.trades stream: not authorized"


def test_a_timeframe_refusal_line_names_the_earliest_day_asked() -> None:
    old = _refusal("/v2/aggs/ticker/AAPL/range/1/day/2023-01-02/2023-01-09")
    assert refusals.refusal_line(old) == f"{old}; earliest day asked: 2023-01-02"
    undated = _refusal("/v2/aggs/ticker/AAPL/prev")
    assert refusals.refusal_line(undated) == str(undated)
