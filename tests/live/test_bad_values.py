"""Tests of the bad-value live checks (`tests/_live/bad_values.py`) against the fake REST server.

Every request the checks make runs with a real `RestClient` against `FakeRestServer`, so the
library encodes and sends it: a parameter of the wrong form fails here, in CI, rather than on
the owner's live run. Scripted replies then show each verdict.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

import pytest

from deephaven_massive._massive import (
    RequestGovernor,
    RestClient,
    WireAuthError,
    WireEntitlementError,
    WireError,
)

# The governor's fake clock, so no pacing wait sleeps, and the reply and envelope builders are
# plain values of the fakes, which no fixture hands over; the server comes from `fake_rest`.
from tests._fakes.clock import FakeClock
from tests._fakes.rest import Reply, envelope
from tests._live import bad_values, plans

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests._fakes.rest import FakeRestServer

_DAY = date(2026, 11, 11)
"""The day the requests are built for: a Wednesday, when ES's front contract is `ESZ6`."""

_AGGS = "/v2/aggs/ticker/AAPL/range"
_CONVERSION = "/v1/conversion/AUD/USD"
_DIVIDENDS = "/stocks/v1/dividends"
_SMA = "/v1/indicators/sma/AAPL"

_SENT: dict[str, list[tuple[str, dict[str, str]]]] = {
    bad_values.PRECISION: [(_CONVERSION, {"amount": "1.2345678", "precision": "5"})],
    bad_values.AMOUNT: [(_CONVERSION, {"amount": "0"}), (_CONVERSION, {"amount": "-100"})],
    bad_values.CURRENCY_CODE: [("/v1/conversion/aud/usd", {"amount": "100"})],
    bad_values.INDICATOR_TIMESPAN: [(_SMA, {"timespan": "second", "limit": "5"})],
    bad_values.WINDOW: [(_SMA, {"window": "0", "limit": "5"})],
    bad_values.ASSET_CLASS: [
        ("/v3/reference/conditions", {"asset_class": "futures"}),
        ("/v3/reference/exchanges", {"asset_class": "indices"}),
        ("/v3/reference/tickers/types", {"asset_class": "futures"}),
    ],
    bad_values.ENUM_SORT: [("/v3/reference/conditions", {"sort": "exchange", "limit": "5"})],
    bad_values.SORT_FIELD: [
        (_DIVIDENDS, {"ticker": "AAPL", "sort": "no_such_field.asc", "limit": "5"})
    ],
    bad_values.SORT_WHITESPACE: [
        (_DIVIDENDS, {"ticker": "AAPL", "sort": "ticker, ex_dividend_date.desc", "limit": "5"})
    ],
    bad_values.LIMIT: [(_DIVIDENDS, {"ticker": "AAPL", "limit": "0"})],
    bad_values.ENUM_FILTER: [("/v3/reference/tickers", {"market": "equities", "limit": "5"})],
    # The list is joined as sent, its second item keeping its space.
    bad_values.FILTER_WHITESPACE: [
        (
            _DIVIDENDS,
            {"ticker.any_of": "AAPL, MSFT", "ex_dividend_date.gte": "2025-11-11", "limit": "50"},
        )
    ],
    bad_values.TRAILING_BALANCE_SHEET: [
        (
            "/stocks/financials/v1/balance-sheets",
            {"tickers": "AAPL", "timeframe": "trailing_twelve_months", "limit": "5"},
        )
    ],
    bad_values.FUTURES_DAY: [
        (
            "/futures/v1/aggs/ESZ6",
            {"resolution": "1day", "window_start.gte": "2026-10-28", "limit": "5"},
        )
    ],
    bad_values.SESSION_OUTSIDE_FUTURES: [
        (f"{_AGGS}/1/session/2026-10-28/2026-11-04", {"limit": "5"})
    ],
    bad_values.MULTIPLIER: [(f"{_AGGS}/0/day/2026-10-28/2026-11-04", {"limit": "5"})],
    bad_values.UNDERLYING_CHAIN: [
        ("/v3/snapshot/options/C:EURUSD", {"limit": "5"}),
        ("/v3/snapshot/options/X:BTCUSD", {"limit": "5"}),
    ],
    bad_values.UNDERLYING_CHAIN_CONTRACT: [
        ("/v3/snapshot/options/O:AAPL250117C00150000", {"limit": "5"})
    ],
    bad_values.UNDERLYING_CONTRACTS: [
        ("/v3/reference/options/contracts", {"underlying_ticker": "C:EURUSD", "limit": "5"}),
        ("/v3/reference/options/contracts", {"underlying_ticker": "X:BTCUSD", "limit": "5"}),
    ],
    bad_values.CONTRACT_SYMBOL: [("/v3/reference/options/contracts/AAPL", {})],
    bad_values.START_AFTER_END: [
        (f"{_AGGS}/1/day/2026-11-04/2026-10-28", {"limit": "5"}),
        (
            _DIVIDENDS,
            {
                "ticker": "AAPL",
                "ex_dividend_date.gte": "2026-10-12",
                "ex_dividend_date.lte": "2025-11-11",
                "limit": "5",
            },
        ),
    ],
    bad_values.START_AFTER_NOW: [(f"{_AGGS}/1/day/2026-11-21/2026-12-01", {"limit": "5"})],
}
"""What each check sends, as the fake server decodes it, for requests built on `_DAY`."""


def _ignore(line: str) -> None:
    """Drop one line of evidence."""


@pytest.fixture
def client(fake_rest: FakeRestServer) -> Iterator[RestClient]:
    governor = RequestGovernor((*plans.TIERS, "other"), clock=FakeClock())
    made = RestClient("test-key", base_url=fake_rest.url, max_concurrency=1, governor=governor)
    yield made
    made.close()


def test_every_check_has_its_requests_listed_here() -> None:
    assert set(bad_values.bad_requests(_DAY)) == set(_SENT)


@pytest.mark.parametrize("check", sorted(_SENT))
def test_each_check_sends_its_refused_value_as_built(
    fake_rest: FakeRestServer, client: RestClient, check: str
) -> None:
    fake_rest.reply_default(Reply(envelope([])))
    answers = bad_values.ask_all(client, check, _ignore, today=_DAY)
    assert fake_rest.get_queries(one_value=True) == _SENT[check]
    assert len(answers) == len(_SENT[check])


def test_a_refusal_is_an_error_with_massives_status_and_message(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    refusal = {"status": "ERROR", "request_id": "r", "error": "precision must be 0 to 4"}
    fake_rest.reply(_CONVERSION, Reply(refusal, status=400))
    lines: list[str] = []
    answers = bad_values.ask_all(client, bad_values.PRECISION, lines.append, today=_DAY)
    assert answers == (bad_values.Answer("ERROR", 400, 0, None),)
    assert "verdict ERROR (HTTP 400" in lines[-1]
    assert lines[-1].endswith("precision must be 0 to 4)")


def test_an_error_envelope_sent_with_200_is_an_error(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    fake_rest.reply(_SMA, Reply({"status": "ERROR", "request_id": "r", "error": "bad window"}))
    answers = bad_values.ask_all(client, bad_values.WINDOW, _ignore, today=_DAY)
    assert answers == (bad_values.Answer("ERROR", 200, 0, None),)


def test_a_200_without_rows_is_empty(fake_rest: FakeRestServer, client: RestClient) -> None:
    fake_rest.reply(_DIVIDENDS, Reply(envelope([])))
    lines: list[str] = []
    answers = bad_values.ask_all(client, bad_values.LIMIT, lines.append, today=_DAY)
    assert answers == (bad_values.Answer("EMPTY", 200, 0, None),)
    assert lines[-1].endswith("verdict EMPTY (HTTP 200, no rows)")


def test_rows_that_cannot_be_compared_are_data_to_read(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    values = [{"timestamp": 1_700_000_000_000, "value": 101.5}]
    fake_rest.reply(_SMA, Reply({"status": "OK", "request_id": "r", "results": {"values": values}}))
    lines: list[str] = []
    # Without `today`, the requests are built for today in New York.
    answers = bad_values.ask_all(client, bad_values.WINDOW, lines.append)
    assert answers == (bad_values.Answer("DATA", 200, 1, None),)
    assert any('"value": 101.5' in line for line in lines)
    assert "read the rows above" in lines[-1]


def test_data_counts_the_rows_that_answer_the_request(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    rows = [{"asset_class": "futures", "id": 1}, {"asset_class": "stocks", "id": 2}]
    fake_rest.reply_default(Reply(envelope(rows)))
    lines: list[str] = []
    answers = bad_values.ask_all(client, bad_values.ASSET_CLASS, lines.append, today=_DAY)
    # Each endpoint gets the same two rows; the exchanges asked for indices, which neither is.
    assert [a.matched for a in answers] == [1, 0, 1]
    assert lines[-1].endswith("2 rows, 1 of them of asset_class futures)")


@pytest.mark.parametrize(
    ("row", "matched"),
    [
        ({"underlying_asset": {"ticker": "C:EURUSD"}}, 1),
        ({"underlying_asset": {"ticker": "SPY"}}, 0),
        ({"underlying_asset": "C:EURUSD"}, 0),  # not an object
        ({"details": {}}, 0),
    ],
)
def test_a_nested_field_is_compared_at_its_depth(
    fake_rest: FakeRestServer, client: RestClient, *, row: dict[str, Any], matched: int
) -> None:
    fake_rest.reply_default(Reply(envelope([row])))
    answers = bad_values.ask_all(client, bad_values.UNDERLYING_CHAIN, _ignore, today=_DAY)
    assert answers[0].matched == matched


@pytest.mark.parametrize(
    ("converted", "matched"),
    [
        (0, [1, 0]),
        (-72.5, [0, 1]),
        (72.5, [0, 0]),
        (False, [0, 0]),  # a bool is no amount
    ],
)
def test_a_conversion_answers_its_amount_when_converted_has_its_sign(
    fake_rest: FakeRestServer, client: RestClient, *, converted: object, matched: list[int]
) -> None:
    body = {"status": "success", "request_id": "r", "from": "AUD", "to": "USD"}
    fake_rest.reply_default(Reply({**body, "converted": converted, "initialAmount": 0}))
    answers = bad_values.ask_all(client, bad_values.AMOUNT, _ignore, today=_DAY)
    # The requests send amounts 0 and -100, in that order.
    assert [a.matched for a in answers] == matched


def test_a_conversion_is_compared_by_its_pair(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    body = {"status": "success", "request_id": "r", "from": "AUD", "to": "USD"}
    fake_rest.reply_default(Reply({**body, "converted": 72.12345, "initialAmount": 100}))
    answers = bad_values.ask_all(client, bad_values.CURRENCY_CODE, _ignore, today=_DAY)
    assert answers == (bad_values.Answer("DATA", 200, 1, 1),)


# 1.2345678 x 0.68438 = 0.844913510964, and x 0.68442 = 0.844962893676
_LAST = {"ask": 0.68442, "bid": 0.68438, "exchange": 48, "timestamp": 1794440590000}


@pytest.mark.parametrize(
    ("converted", "last", "matched", "note"),
    [
        (
            0.84491,
            _LAST,
            1,
            (
                "converted=0.84491: amount x last.bid = 0.844913510964 rounded to 5 decimals; "
                "amount x last.ask = 0.844962893676 matches no rounding"
            ),
        ),
        (
            0.84496,
            _LAST,
            1,
            (
                "converted=0.84496: amount x last.bid = 0.844913510964 matches no rounding; "
                "amount x last.ask = 0.844962893676 rounded to 5 decimals"
            ),
        ),
        (
            0.8449,
            _LAST,
            0,
            (
                "converted=0.8449: amount x last.bid = 0.844913510964 rounded to 4 decimals; "
                "amount x last.ask = 0.844962893676 matches no rounding"
            ),
        ),
        (
            0.84,
            _LAST,
            0,
            (
                "converted=0.84: amount x last.bid = 0.844913510964 rounded to 2 decimals; "
                "amount x last.ask = 0.844962893676 rounded to 2 decimals"
            ),
        ),
        (
            0.84,
            {"bid": True},
            0,
            "converted=0.84: no amount x last.bid; no amount x last.ask",
        ),
    ],
)
def test_a_conversion_s_precision_is_read_from_the_decimals_it_was_rounded_to(
    fake_rest: FakeRestServer,
    client: RestClient,
    *,
    converted: float,
    last: dict[str, object],
    matched: int,
    note: str,
) -> None:
    body = {"status": "success", "request_id": "r", "from": "AUD", "to": "USD"}
    row = {"converted": converted, "initialAmount": 1.2345678, "last": last}
    fake_rest.reply_default(Reply({**body, **row}))
    lines: list[str] = []
    answers = bad_values.ask_all(client, bad_values.PRECISION, lines.append, today=_DAY)
    assert answers == (bad_values.Answer("DATA", 200, 1, matched),)
    assert lines[-2] == f"precision=5: {note}"


def test_a_server_failure_is_no_judgment_of_the_value_and_raises(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    fake_rest.reply(_DIVIDENDS, Reply({"request_id": "r", "message": "busy"}, status=503))
    with pytest.raises(WireError) as raised:
        bad_values.ask_all(client, bad_values.LIMIT, _ignore, today=_DAY)
    assert raised.value.status == 503


def test_no_row_of_a_range_that_holds_no_day_matches(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    fake_rest.reply_default(Reply(envelope([{"t": 1_700_000_000_000, "c": 1.0}])))
    answers = bad_values.ask_all(client, bad_values.START_AFTER_END, _ignore, today=_DAY)
    assert [(a.rows, a.matched) for a in answers] == [(1, 0), (1, 0)]


def test_an_unreadable_200_is_recorded_and_raises(
    fake_rest: FakeRestServer, client: RestClient
) -> None:
    fake_rest.reply(_DIVIDENDS, Reply(b"not json"))
    lines: list[str] = []
    with pytest.raises(WireError) as raised:
        bad_values.ask_all(client, bad_values.LIMIT, lines.append, today=_DAY)
    assert raised.value.unreadable
    assert "no verdict on the value" in lines[-1]


def test_an_empty_200_is_empty(fake_rest: FakeRestServer, client: RestClient) -> None:
    # The indicators answered `window=0` this way: Massive's answer, though no page.
    fake_rest.reply(_SMA, Reply(b""))
    lines: list[str] = []
    answers = bad_values.ask_all(client, bad_values.WINDOW, lines.append, today=_DAY)
    assert answers == (bad_values.Answer("EMPTY", 200, 0, None),)
    assert lines[-1] == "window=0: verdict EMPTY (HTTP 200, empty body)"


@pytest.mark.parametrize(
    "reply",
    [
        Reply({"status": "ERROR", "request_id": "r", "error": "unknown key"}, status=401),
        Reply({"status": "NOT_AUTHORIZED", "request_id": "r", "message": "upgrade"}, status=403),
        Reply({"status": "NOT_AUTHORIZED", "request_id": "r", "message": "upgrade"}),
    ],
)
def test_a_refusal_of_the_key_or_plan_is_recorded_and_raises(
    fake_rest: FakeRestServer, client: RestClient, reply: Reply
) -> None:
    fake_rest.reply(_DIVIDENDS, reply)
    lines: list[str] = []
    with pytest.raises((WireAuthError, WireEntitlementError)):
        bad_values.ask_all(client, bad_values.LIMIT, lines.append, today=_DAY)
    assert "no verdict on the value" in lines[-1]


def test_no_response_is_no_answer_and_raises(client: RestClient) -> None:
    client.close()
    with pytest.raises(WireError) as raised:
        bad_values.ask_all(client, bad_values.LIMIT, _ignore, today=_DAY)
    assert raised.value.status is None
