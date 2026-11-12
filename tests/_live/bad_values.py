"""What Massive answers to a value the library could refuse before sending: requests and verdicts.

A library check of an argument value earns its place only when Massive, sent the value, returns
wrong or empty data with no error; a value Massive refuses or serves correctly is sent. These
checks record which is which: a conversion `precision` of 5, a `sort` field the path does not
list, a start after the end, and more. `bad_requests` builds, for each check, the smallest
requests that carry the value straight to Massive's REST endpoint through the wire client, past
any library check. `ask` sends one, records through `evidence` Massive's status, message, and
row count, and ends with one verdict line:

- `ERROR`: Massive refused the request; the line gives its HTTP status and message.
- `EMPTY`: HTTP 200 with no rows, or with an empty body.
- `DATA`: HTTP 200 with rows; the line says how many of them answer the request as sent, where
  a row can be compared with it, and otherwise says to read the rows recorded above it.

The live tests that send the requests are marked `settles`. `tests/live/test_bad_values.py`
sends every request to the fake REST server, so a parameter of the wrong form fails in CI
rather than on the owner's live run.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any, Final, TypeGuard
from zoneinfo import ZoneInfo

from deephaven_massive._massive import (
    Transport,
    WireAuthError,
    WireEntitlementError,
    WireError,
    set_reply_capture,
    source,
)
from tests._live.contracts import front_contract

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven_massive._massive import CapturedReply, RestClient

__all__ = [
    "AMOUNT",
    "ASSET_CLASS",
    "CONTRACT_SYMBOL",
    "CURRENCY_CODE",
    "ENUM_FILTER",
    "ENUM_SORT",
    "FILTER_EMPTY_ITEM",
    "FILTER_WHITESPACE",
    "FUTURES_DAY",
    "INDICATOR_TIMESPAN",
    "LIMIT",
    "MULTIPLIER",
    "PRECISION",
    "SESSION_OUTSIDE_FUTURES",
    "SORT_FIELD",
    "SORT_WHITESPACE",
    "START_AFTER_END",
    "START_AFTER_NOW",
    "TRAILING_BALANCE_SHEET",
    "UNDERLYING_CHAIN",
    "UNDERLYING_CHAIN_CONTRACT",
    "UNDERLYING_CONTRACTS",
    "UNDERLYING_CONTRACTS_CONTRACT",
    "WINDOW",
    "Answer",
    "BadRequest",
    "ask",
    "ask_all",
    "bad_requests",
]

PRECISION: Final = "forex conversion precision outside 0 to 4"
AMOUNT: Final = "forex conversion amount not positive"
CURRENCY_CODE: Final = "forex conversion currency not three upper-case letters"
INDICATOR_TIMESPAN: Final = "indicator timespan the endpoint does not list"
WINDOW: Final = "indicator window below 1"
ASSET_CLASS: Final = "reference asset_class the endpoint does not list"
ENUM_SORT: Final = "sort value a listed-sort path does not list"
SORT_FIELD: Final = "sort part naming no field of a text-sort path"
SORT_WHITESPACE: Final = "sort part with surrounding whitespace"
LIMIT: Final = "limit below 1"
ENUM_FILTER: Final = "enum filter value the parameter does not list"
FILTER_WHITESPACE: Final = "filter list item with surrounding whitespace"
FILTER_EMPTY_ITEM: Final = "filter list with an empty item"
TRAILING_BALANCE_SHEET: Final = "balance sheets for the trailing twelve months"
FUTURES_DAY: Final = "futures bars of a day resolution"
SESSION_OUTSIDE_FUTURES: Final = "session bars outside futures"
MULTIPLIER: Final = "bar multiplier below 1"
UNDERLYING_CHAIN: Final = "option chain of a currency or crypto pair"
UNDERLYING_CHAIN_CONTRACT: Final = "option chain of an option contract"
UNDERLYING_CONTRACTS: Final = "option contracts of a currency or crypto pair"
UNDERLYING_CONTRACTS_CONTRACT: Final = "option contracts of an option contract"
CONTRACT_SYMBOL: Final = "option contract details of a stock ticker"
START_AFTER_END: Final = "start after end"
START_AFTER_NOW: Final = "start after now"

_NEW_YORK: Final = ZoneInfo("America/New_York")

_AGGS: Final = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_CONVERSION: Final = "/v1/conversion/{from}/{to}"
_SMA: Final = "/v1/indicators/sma/{stockTicker}"
_DIVIDENDS: Final = "/stocks/v1/dividends"
_CONDITIONS: Final = "/v3/reference/conditions"
_CONTRACT: Final = "O:AAPL250117C00150000"
"""An option contract, sent where the chain takes its underlying."""

_PAIRS: Final = ("C:EURUSD", "X:BTCUSD")
"""A forex and a crypto pair: underlyings the library refuses, as it does an option contract."""

_SERVER_FAILURES: Final = range(500, 600)
"""Statuses of a server that failed, which say nothing about the value sent."""

_NO_JUDGMENT: Final = frozenset({429, *_SERVER_FAILURES})
"""Statuses that are no judgment of the value: a rate limit, and a server failure."""

_PRECISION_AMOUNT: Final = 1.2345678
"""The amount the precision check converts: times a rate of 5 decimals, its exact conversion
has about 12, so the decimals of `converted` show the precision Massive rounded to."""

_PRECISION_SENT: Final = 5
"""The precision the precision check sends, one more than Massive's specification lists."""

_ROUNDING_TOLERANCE: Final = 1e-9
"""How far `converted` may stray from a rounding of the exact conversion: float noise only."""

_ROWS_SHOWN: Final = 2
"""How many rows of a 200 the evidence shows."""

_EXCERPT: Final = 300
"""How many characters of each shown row, and of a 200's other fields, the evidence keeps."""

Row = dict[str, Any]
"""One decoded row, as the wire client returns it."""


@dataclass(frozen=True, kw_only=True)
class BadRequest:
    """One request that carries a value the library refuses, as Massive receives it.

    Attributes:
        label: The refused value as the evidence names it, such as `"precision=5"`.
        path: The REST path template, as Massive's specification spells it.
        names: The path parameters, by wire name.
        query: The query parameters, by wire name, each in the form its parameter takes: a
            sequence of text for a list parameter such as `ticker.any_of`.
        bucket: The rate bucket of the data's class (`"other"` for reference data).
        match: Tells whether a row answers the request as sent; None when no row can be
            compared with it.
        matching: What a matching row is, for the verdict line, such as `"of asset_class
            futures"`; empty when `match` is None.
        note: Describes each recorded row in one more evidence line, such as the decimals a
            conversion was rounded to; None for no such line.
    """

    label: str
    path: str
    names: Mapping[str, object]
    query: Mapping[str, object]
    bucket: str
    match: Callable[[Row], bool] | None = None
    matching: str = ""
    note: Callable[[Row], str] | None = None


@dataclass(frozen=True)
class Answer:
    """What Massive answered to one `BadRequest`.

    Attributes:
        verdict: `ERROR`, `EMPTY`, or `DATA`.
        status: The HTTP status: 200 for `EMPTY` and `DATA`, and for an `ERROR` envelope
            sent with 200.
        rows: How many rows a 200 held; 0 for `ERROR`.
        matched: How many of the rows answer the request as sent; None when they cannot be
            compared with it, or there are none.
    """

    verdict: str
    status: int
    rows: int
    matched: int | None


def bad_requests(today: date) -> dict[str, tuple[BadRequest, ...]]:
    """Return every check's requests, each carrying the value the check refuses.

    Args:
        today: The day the requests are sent, New York time, from which their date ranges are
            counted, so each stays within a Basic plan's two years.

    Returns:
        The requests by check (`PRECISION` and the other constants of this module).
    """
    return {
        **_conversion_requests(),
        **_indicator_requests(),
        **_reference_requests(),
        **_list_requests(today),
        **_option_requests(),
        **_bar_requests(today),
    }


def ask(client: RestClient, request: BadRequest, evidence: Callable[[str], None]) -> Answer:
    """Send one request to Massive and record its answer and verdict.

    Args:
        client: The wire REST client.
        request: The request.
        evidence: Records one line of what Massive sent.

    Returns:
        The answer.

    An HTTP 200 with an empty body is Massive's answer too, though the client cannot read it
    as a page: it is recorded as `EMPTY` (HTTP 200, empty body).

    Raises:
        WireError: If the request got no answer about its value: no HTTP response (a
            transport failure after retries, or a closed client), a server failure (5xx)
            after retries, a rate limit (429) the governor gave up waiting out, a refusal
            of the key or the plan (401, 403, or a `NOT_AUTHORIZED` envelope), or a 200
            whose body is not empty and that the client cannot read. It records the last
            two first.
    """
    rest_path = source(Transport.REST, request.path)
    names, query = _shown(request.names), _shown(request.query)
    evidence(f"{request.label}: GET {request.path} path={names} query={query}")
    replies: list[CapturedReply] = []
    # The live tests' process installs no other sink, so this one replaces none.
    set_reply_capture(replies.append)
    try:
        page = client.get(rest_path, request.names, request.query, bucket=request.bucket)
    except WireError as error:
        if error.status is None or error.status in _NO_JUDGMENT:
            raise
        if error.unreadable and replies and replies[-1].status == 200 and not replies[-1].text:
            evidence(f"{request.label}: verdict EMPTY (HTTP 200, empty body)")
            return Answer("EMPTY", 200, 0, None)
        if error.unreadable or isinstance(error, WireAuthError | WireEntitlementError):
            # A reply that judges the key or the plan, or cannot be read: recorded for the
            # owner, but no refusal of the value. The live fixtures turn a plan refusal into
            # a skip or a failure.
            evidence(f"{request.label}: no verdict on the value: {error.message}")
            raise
        evidence(f"{request.label}: verdict ERROR (HTTP {error.status}: {error.message})")
        return Answer("ERROR", error.status, 0, None)
    finally:
        set_reply_capture(None)
    rows = page.rows
    # The body's own fields beside the rows (status, count, a message), without the next page.
    body = page.body if isinstance(page.body, dict) else {}
    scalars = {k: v for k, v in body.items() if k != "next_url" and not isinstance(v, dict | list)}
    evidence(f"{request.label}: HTTP 200, {len(rows)} rows, body {_excerpt(scalars)}")
    for row in rows[:_ROWS_SHOWN]:
        evidence(f"{request.label}: row {_excerpt(row)}")
        if request.note is not None:
            evidence(f"{request.label}: {request.note(row)}")
    if not rows:
        evidence(f"{request.label}: verdict EMPTY (HTTP 200, no rows)")
        return Answer("EMPTY", 200, 0, None)
    if request.match is None:
        evidence(
            f"{request.label}: verdict DATA (HTTP 200, {len(rows)} rows, which cannot be "
            "compared with the request: read the rows above)"
        )
        return Answer("DATA", 200, len(rows), None)
    matched = sum(1 for row in rows if request.match(row))
    evidence(
        f"{request.label}: verdict DATA (HTTP 200, {len(rows)} rows, {matched} of them "
        f"{request.matching})"
    )
    return Answer("DATA", 200, len(rows), matched)


def ask_all(
    client: RestClient,
    check: str,
    evidence: Callable[[str], None],
    *,
    today: date | None = None,
) -> tuple[Answer, ...]:
    """Send every request of one check and record each answer.

    Args:
        client: The wire REST client.
        check: The check, one of this module's constants, such as `PRECISION`.
        evidence: Records one line of what Massive sent.
        today: The day the requests are sent; None for today in New York.

    Returns:
        The answers, in request order.
    """
    day = datetime.now(_NEW_YORK).date() if today is None else today
    return tuple(ask(client, request, evidence) for request in bad_requests(day)[check])


def _shown(query: Mapping[str, object]) -> dict[str, object]:
    """Return query values as the evidence shows them: a date as ISO text.

    Args:
        query: The query parameters.

    Returns:
        The same parameters, each date written `YYYY-MM-DD`.
    """
    return {k: v.isoformat() if isinstance(v, date) else v for k, v in query.items()}


def _excerpt(value: object) -> str:
    """Return the start of a value's JSON text.

    Args:
        value: A decoded row, or the scalar fields of a body.

    Returns:
        Its JSON, cut to `_EXCERPT` characters.
    """
    return json.dumps(value, default=str)[:_EXCERPT]


def _converted_like(amount: int, row: Row) -> bool:
    """Tell whether a conversion's `converted` has the sign of the amount sent.

    Args:
        amount: The amount sent: 0 or below.
        row: The conversion.

    Returns:
        True when `converted` is a number that is 0 for an amount of 0, or below 0 for an
        amount below 0.
    """
    converted = row.get("converted")
    if isinstance(converted, bool) or not isinstance(converted, int | float):
        return False
    return converted == 0 if amount == 0 else converted < 0


def _exact(row: Row) -> dict[str, tuple[float, float] | None]:
    """Return, per side of a conversion's quote, its `converted` and its exact conversion.

    Args:
        row: The conversion, with `converted`, `initialAmount`, and `last.bid` and `.ask`.

    Returns:
        For `bid` and `ask`, `converted` and the amount times that price; None when one of
        the three is not a number.
    """
    converted, amount = row.get("converted"), row.get("initialAmount")
    last = row.get("last") or {}
    found: dict[str, tuple[float, float] | None] = {}
    for side in ("bid", "ask"):
        price = last.get(side)
        if _number(converted) and _number(amount) and _number(price):
            found[side] = (converted, amount * price)
        else:
            found[side] = None
    return found


def _number(value: object) -> TypeGuard[float]:
    """Tell whether a decoded value is a number, and not a boolean.

    Args:
        value: The value.

    Returns:
        True for an int or a float other than a boolean.
    """
    return isinstance(value, int | float) and not isinstance(value, bool)


def _rounds_to(exact: float, decimals: int, converted: float) -> bool:
    """Tell whether an exact conversion rounded to `decimals` places is `converted`.

    Args:
        exact: The amount times a price.
        decimals: The places.
        converted: Massive's `converted`.

    Returns:
        True when the two are within `_ROUNDING_TOLERANCE`.
    """
    return abs(round(exact, decimals) - converted) <= _ROUNDING_TOLERANCE


def _rounded_to(decimals: int, row: Row) -> bool:
    """Tell whether a conversion's `converted` is its exact conversion rounded to `decimals`.

    Args:
        decimals: The precision sent.
        row: The conversion.

    Returns:
        True when the amount times `last.bid` or `last.ask`, rounded to `decimals` places, is
        `converted`.
    """
    return any(
        pair is not None and _rounds_to(pair[1], decimals, pair[0]) for pair in _exact(row).values()
    )


def _rounding(row: Row) -> str:
    """Describe the decimals a conversion's `converted` was rounded to, for the evidence.

    Args:
        row: The conversion.

    Returns:
        A line naming `converted` and, for each side of the quote, the fewest decimals, 0 to
        12, at which the amount times that price rounds to `converted`, or that none does.
        A conversion rounded to 5 decimals whose fifth is 0 reads as 4.
    """
    sides = []
    for side, pair in _exact(row).items():
        if pair is None:
            sides.append(f"no amount x last.{side}")
            continue
        converted, exact = pair
        decimals = next((n for n in range(13) if _rounds_to(exact, n, converted)), None)
        rounded = "matches no rounding" if decimals is None else f"rounded to {decimals} decimals"
        sides.append(f"amount x last.{side} = {exact:.12f} {rounded}")
    return f"converted={row.get('converted')}: " + "; ".join(sides)


def _field_is(key: str, wanted: object, row: Row) -> bool:
    """Tell whether a row's field holds a value, for a `BadRequest.match`.

    Args:
        key: The field's key; a dotted key walks into nested objects
            (`underlying_asset.ticker`).
        wanted: The value.
        row: The row.

    Returns:
        True when the field is present and equal to `wanted`.
    """
    found: object = row
    for part in key.split("."):
        if not isinstance(found, dict):
            return False
        found = found.get(part)
    return found == wanted


def _listed_ticker(row: Row) -> bool:
    """Tell whether a dividend row is of AAPL or MSFT, the tickers beside the empty item.

    Args:
        row: The row.

    Returns:
        True when its `ticker` is `AAPL` or `MSFT`.
    """
    return row.get("ticker") in ("AAPL", "MSFT")


def _never(_row: Row) -> bool:
    """Match no row, for a request whose range holds no time.

    Args:
        _row: The row.

    Returns:
        False.
    """
    return False


def _conversion_requests() -> dict[str, tuple[BadRequest, ...]]:
    """Return the forex conversion's requests.

    Returns:
        The requests by check.
    """
    aud_usd = {"from": "AUD", "to": "USD"}
    return {
        PRECISION: (
            BadRequest(
                label=f"precision={_PRECISION_SENT}",
                path=_CONVERSION,
                names=aud_usd,
                query={"amount": _PRECISION_AMOUNT, "precision": str(_PRECISION_SENT)},
                bucket="currencies",
                match=partial(_rounded_to, _PRECISION_SENT),
                matching=(
                    f"with `converted` the amount times `last.bid` or `last.ask` rounded to "
                    f"{_PRECISION_SENT} decimals"
                ),
                note=_rounding,
            ),
        ),
        AMOUNT: tuple(
            BadRequest(
                label=f"amount={amount}",
                path=_CONVERSION,
                names=aud_usd,
                query={"amount": amount},
                bucket="currencies",
                match=partial(_converted_like, amount),
                matching="with `converted` of the amount's sign",
            )
            for amount in (0, -100)
        ),
        CURRENCY_CODE: (
            BadRequest(
                label="from=aud, to=usd",
                path=_CONVERSION,
                names={"from": "aud", "to": "usd"},
                query={"amount": 100},
                bucket="currencies",
                match=lambda row: row.get("from") == "AUD" and row.get("to") == "USD",
                matching="converting AUD to USD",
            ),
        ),
    }


def _indicator_requests() -> dict[str, tuple[BadRequest, ...]]:
    """Return the simple moving average's requests, of AAPL.

    Returns:
        The requests by check.
    """
    return {
        INDICATOR_TIMESPAN: (
            BadRequest(
                label="timespan=second",
                path=_SMA,
                names={"stockTicker": "AAPL"},
                query={"timespan": "second", "limit": 5},
                bucket="stocks",
            ),
        ),
        WINDOW: (
            BadRequest(
                label="window=0",
                path=_SMA,
                names={"stockTicker": "AAPL"},
                query={"window": 0, "limit": 5},
                bucket="stocks",
            ),
        ),
    }


def _reference_requests() -> dict[str, tuple[BadRequest, ...]]:
    """Return the reference endpoints' requests: one class each endpoint does not list.

    Returns:
        The requests by check.
    """
    refused = (
        ("/v3/reference/conditions", "futures"),
        ("/v3/reference/exchanges", "indices"),
        ("/v3/reference/tickers/types", "futures"),
    )
    return {
        ASSET_CLASS: tuple(
            BadRequest(
                label=f"{path} asset_class={value}",
                path=path,
                names={},
                query={"asset_class": value},
                bucket="other",
                match=partial(_field_is, "asset_class", value),
                matching=f"of asset_class {value}",
            )
            for path, value in refused
        ),
    }


def _list_requests(today: date) -> dict[str, tuple[BadRequest, ...]]:
    """Return the requests of the list endpoints' `sort`, `limit`, and filters.

    Args:
        today: The day the requests are sent.

    Returns:
        The requests by check.
    """
    return {
        ENUM_SORT: (
            BadRequest(
                label="conditions sort=exchange",
                path=_CONDITIONS,
                names={},
                query={"sort": "exchange", "limit": 5},
                bucket="other",
            ),
        ),
        SORT_FIELD: (
            BadRequest(
                label="dividends sort=no_such_field.asc",
                path=_DIVIDENDS,
                names={},
                query={"ticker": "AAPL", "sort": "no_such_field.asc", "limit": 5},
                bucket="stocks",
            ),
        ),
        SORT_WHITESPACE: (
            BadRequest(
                label="dividends sort='ticker, ex_dividend_date.desc'",
                path=_DIVIDENDS,
                names={},
                query={"ticker": "AAPL", "sort": "ticker, ex_dividend_date.desc", "limit": 5},
                bucket="stocks",
            ),
        ),
        LIMIT: (
            BadRequest(
                label="dividends limit=0",
                path=_DIVIDENDS,
                names={},
                query={"ticker": "AAPL", "limit": 0},
                bucket="stocks",
            ),
        ),
        ENUM_FILTER: (
            BadRequest(
                label="tickers market=equities",
                path="/v3/reference/tickers",
                names={},
                query={"market": "equities", "limit": 5},
                bucket="other",
                match=partial(_field_is, "market", "equities"),
                matching="of market equities",
            ),
        ),
        FILTER_WHITESPACE: (
            BadRequest(
                label="dividends ticker.any_of=['AAPL', ' MSFT']",
                path=_DIVIDENDS,
                names={},
                # A year of quarterly dividends of both fits one page, so a missing MSFT row
                # is no row on a later page.
                query={
                    "ticker.any_of": ["AAPL", " MSFT"],
                    "ex_dividend_date.gte": today - timedelta(days=365),
                    "limit": 50,
                },
                bucket="stocks",
                match=partial(_field_is, "ticker", "MSFT"),
                matching="of MSFT, the item sent with a space",
            ),
        ),
        FILTER_EMPTY_ITEM: (
            BadRequest(
                label="dividends ticker.any_of=['AAPL', '', 'MSFT']",
                path=_DIVIDENDS,
                names={},
                query={
                    "ticker.any_of": ["AAPL", "", "MSFT"],
                    "ex_dividend_date.gte": today - timedelta(days=365),
                    "limit": 50,
                },
                bucket="stocks",
                match=_listed_ticker,
                matching="of AAPL or MSFT, the tickers sent",
            ),
        ),
        TRAILING_BALANCE_SHEET: (
            BadRequest(
                label="balance sheets timeframe=trailing_twelve_months",
                path="/stocks/financials/v1/balance-sheets",
                names={},
                query={"tickers": "AAPL", "timeframe": "trailing_twelve_months", "limit": 5},
                bucket="stocks",
                match=partial(_field_is, "timeframe", "trailing_twelve_months"),
                matching="with timeframe trailing_twelve_months",
            ),
        ),
    }


def _option_requests() -> dict[str, tuple[BadRequest, ...]]:
    """Return the options requests: a pair or a contract as the underlying, a stock as a contract.

    Returns:
        The requests by check.
    """
    return {
        UNDERLYING_CHAIN: tuple(
            BadRequest(
                label=f"chain of {pair}",
                path="/v3/snapshot/options/{underlyingAsset}",
                names={"underlyingAsset": pair},
                query={"limit": 5},
                bucket="options",
                match=partial(_field_is, "underlying_asset.ticker", pair),
                matching=f"with underlying_asset.ticker {pair}",
            )
            for pair in _PAIRS
        ),
        UNDERLYING_CHAIN_CONTRACT: (
            BadRequest(
                label=f"chain of {_CONTRACT}",
                path="/v3/snapshot/options/{underlyingAsset}",
                names={"underlyingAsset": _CONTRACT},
                query={"limit": 5},
                bucket="options",
                match=partial(_field_is, "underlying_asset.ticker", _CONTRACT),
                matching=f"with underlying_asset.ticker {_CONTRACT}",
            ),
        ),
        UNDERLYING_CONTRACTS: tuple(
            BadRequest(
                label=f"contracts underlying_ticker={pair}",
                path="/v3/reference/options/contracts",
                names={},
                query={"underlying_ticker": pair, "limit": 5},
                bucket="options",
                match=partial(_field_is, "underlying_ticker", pair),
                matching=f"with underlying_ticker {pair}",
            )
            for pair in _PAIRS
        ),
        UNDERLYING_CONTRACTS_CONTRACT: (
            BadRequest(
                label=f"contracts underlying_ticker={_CONTRACT}",
                path="/v3/reference/options/contracts",
                names={},
                query={"underlying_ticker": _CONTRACT, "limit": 5},
                bucket="options",
                match=partial(_field_is, "underlying_ticker", _CONTRACT),
                matching=f"with underlying_ticker {_CONTRACT}",
            ),
        ),
        CONTRACT_SYMBOL: (
            BadRequest(
                label="contract details of AAPL",
                path="/v3/reference/options/contracts/{options_ticker}",
                names={"options_ticker": "AAPL"},
                query={},
                bucket="options",
                match=partial(_field_is, "ticker", "AAPL"),
                matching="describing the ticker AAPL",
            ),
        ),
    }


def _bar_requests(today: date) -> dict[str, tuple[BadRequest, ...]]:
    """Return the bar and date-range requests, of AAPL and the front E-mini contract.

    Args:
        today: The day the requests are sent.

    Returns:
        The requests by check.
    """
    week_ago, two_weeks_ago = today - timedelta(days=7), today - timedelta(days=14)
    empty = "inside the range, which holds no day"
    return {
        FUTURES_DAY: (
            BadRequest(
                label="futures resolution=1day",
                path="/futures/v1/aggs/{ticker}",
                names={"ticker": front_contract("ES", today)},
                query={"resolution": "1day", "window_start.gte": two_weeks_ago, "limit": 5},
                bucket="futures",
            ),
        ),
        SESSION_OUTSIDE_FUTURES: (
            _stock_bars(
                "stock bars timespan=session", 1, "session", start=two_weeks_ago, end=week_ago
            ),
        ),
        MULTIPLIER: (
            _stock_bars("stock bars multiplier=0", 0, "day", start=two_weeks_ago, end=week_ago),
        ),
        START_AFTER_END: (
            _stock_bars(
                "bars from a week ago to two weeks ago",
                1,
                "day",
                start=week_ago,
                end=two_weeks_ago,
                empty=empty,
            ),
            BadRequest(
                label="dividends ex_dividend_date from a month ago to a year ago",
                path=_DIVIDENDS,
                names={},
                query={
                    "ticker": "AAPL",
                    "ex_dividend_date.gte": today - timedelta(days=30),
                    "ex_dividend_date.lte": today - timedelta(days=365),
                    "limit": 5,
                },
                bucket="stocks",
                match=_never,
                matching=empty,
            ),
        ),
        START_AFTER_NOW: (
            _stock_bars(
                "bars from 10 to 20 days ahead",
                1,
                "day",
                start=today + timedelta(days=10),
                end=today + timedelta(days=20),
                empty="inside the range, which holds no bar yet",
            ),
        ),
    }


def _stock_bars(
    label: str, multiplier: int, timespan: str, *, start: date, end: date, empty: str = ""
) -> BadRequest:
    """Return a request for AAPL's bars from the aggregates range.

    Args:
        label: The refused value as the evidence names it.
        multiplier: The bar's multiplier.
        timespan: The bar's timespan, sent as text so a value the path does not list reaches
            Massive.
        start: The range's `from`.
        end: The range's `to`.
        empty: For a range that holds no day, what a row inside it would be, so that no row
            matches; empty for a range that holds days.

    Returns:
        The request.
    """
    return BadRequest(
        label=label,
        path=_AGGS,
        names={
            "stocksTicker": "AAPL",
            "multiplier": multiplier,
            "timespan": timespan,
            "from": start,
            "to": end,
        },
        query={"limit": 5},
        bucket="stocks",
        match=_never if empty else None,
        matching=empty,
    )
