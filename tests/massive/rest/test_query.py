from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from enum import StrEnum

import pytest

from deephaven_massive._massive import spec
from deephaven_massive._massive.rest import query
from deephaven_massive._massive.spec.types import Param, ParamKind, RestPath, Transport, WireUnit


class _Order(StrEnum):
    ASC = "asc"
    DESC = "desc"


_TICKER = Param(name="stockTicker", kind=ParamKind.STRING, in_path=True)
_DATE = Param(name="date", kind=ParamKind.DATE, in_path=True)
_PARAMS = (
    _TICKER,
    _DATE,
    Param(name="order", kind=ParamKind.ENUM, values=("asc", "desc")),
    Param(name="limit", kind=ParamKind.INTEGER),
    Param(name="strike_price.gte", kind=ParamKind.NUMBER),
    Param(name="adjusted", kind=ParamKind.BOOLEAN),
    Param(name="timestamp.gte", kind=ParamKind.TIMESTAMP, unit=WireUnit.NS),
    Param(name="published_utc.gte", kind=ParamKind.DATE),
    Param(name="ticker.any_of", kind=ParamKind.LIST),
    Param(name="cursor", kind=ParamKind.STRING),
)
_PATH = RestPath(key="/v1/open-close/{stockTicker}/{date}", fields=(), params=_PARAMS)
_ORIGIN = ("https", "api.massive.com", 443)


def _target(**query_params: object) -> str:
    return query.request_target(
        _PATH, {"stockTicker": "AAPL", "date": date(2026, 1, 2)}, query_params
    )


# The request target.


def test_the_target_fills_the_template_and_encodes_the_query_in_order() -> None:
    assert (
        _target(limit=10, order=_Order.ASC) == "/v1/open-close/AAPL/2026-01-02?limit=10&order=asc"
    )


def test_a_target_without_query_values_has_no_question_mark() -> None:
    assert _target() == "/v1/open-close/AAPL/2026-01-02"
    assert _target(limit=None) == "/v1/open-close/AAPL/2026-01-02"


def test_path_values_are_percent_encoded_whole() -> None:
    target = query.request_target(_PATH, {"stockTicker": "BRK/B X", "date": date(2026, 1, 2)}, {})
    assert target == "/v1/open-close/BRK%2FB%20X/2026-01-02"


def test_query_values_are_percent_encoded() -> None:
    eastern = timezone(timedelta(hours=-5))
    target = _target(
        **{
            "ticker.any_of": ["A&B", "C D"],
            "published_utc.gte": datetime(2026, 1, 2, tzinfo=eastern),
        }
    )
    assert target == (
        "/v1/open-close/AAPL/2026-01-02?ticker.any_of=A%26B%2CC%20D"
        "&published_utc.gte=2026-01-02T05%3A00%3A00Z"
    )


def test_the_arguments_are_not_modified() -> None:
    path_params: dict[str, object] = {"stockTicker": "AAPL", "date": date(2026, 1, 2)}
    query_params: dict[str, object] = {"ticker.any_of": ["A"], "limit": None}
    query.request_target(_PATH, path_params, query_params)
    assert path_params == {"stockTicker": "AAPL", "date": date(2026, 1, 2)}
    assert query_params == {"ticker.any_of": ["A"], "limit": None}


@pytest.mark.parametrize(
    "key",
    [
        "/stocks/financials/v1/balance-sheets",
        "/stocks/financials/v1/cash-flow-statements",
        "/stocks/financials/v1/income-statements",
        "/stocks/filings/10-K/vX/sections",
    ],
)
def test_the_generated_period_end_filters_take_dates(key: str) -> None:
    # The vendor types these filters as bare strings; their descriptions give the date format.
    rest_path = spec.source(Transport.REST, key)
    query_params = {"period_end.gte": date(2024, 1, 1), "period_end.lt": date(2025, 1, 1)}
    target = query.request_target(rest_path, {}, query_params)
    assert target == f"{key}?period_end.gte=2024-01-01&period_end.lt=2025-01-01"


@pytest.mark.parametrize(
    ("key", "name"), [("/benzinga/v2/news", "published"), ("/v2/reference/news", "published_utc")]
)
def test_the_generated_news_publication_bounds_send_an_instant_or_a_date(
    key: str, name: str
) -> None:
    # Both news paths filter on the publication instant: an aware datetime is sent as an
    # ISO-8601 instant, never cut to its day, and a date as the day.
    rest_path = spec.source(Transport.REST, key)
    start = datetime(2024, 5, 28, 20, 27, 41, tzinfo=UTC)
    query_params = {f"{name}.gte": start, f"{name}.lt": date(2024, 5, 29)}
    target = query.request_target(rest_path, {}, query_params)
    assert target == f"{key}?{name}.gte=2024-05-28T20%3A27%3A41Z&{name}.lt=2024-05-29"


@pytest.mark.parametrize(
    "path_params",
    [
        {"stockTicker": "AAPL"},
        {"stockTicker": "AAPL", "date": None},
        {"stockTicker": "AAPL", "date": date(2026, 1, 2), "extra": 1},
    ],
)
def test_path_parameters_must_be_exactly_the_placeholders(path_params: dict[str, object]) -> None:
    with pytest.raises(ValueError, match=r"takes path parameters \['date', 'stockTicker'\]"):
        query.request_target(_PATH, path_params, {})


@pytest.mark.parametrize("name", ["apiKey", "stockTicker"])
def test_a_query_name_must_be_a_query_parameter(name: str) -> None:
    with pytest.raises(ValueError, match=f"has no query parameter '{name}'"):
        _target(**{name: "x"})


# next_url.


def test_origin_of_fills_the_default_port_and_lowers_the_host() -> None:
    assert query.origin_of("https://API.Massive.com") == _ORIGIN
    assert query.origin_of("http://127.0.0.1:8080/x") == ("http", "127.0.0.1", 8080)
    assert query.origin_of("HTTP://h") == ("http", "h", 80)


@pytest.mark.parametrize("url", ["/v3/trades", "ftp://h/x", "https://", "api.massive.com"])
def test_origin_of_refuses_anything_but_an_absolute_http_url(url: str) -> None:
    with pytest.raises(ValueError, match="is not an absolute http or https URL"):
        query.origin_of(url)


def test_next_url_keeps_the_query_byte_for_byte_and_drops_api_key() -> None:
    url = "https://api.massive.com:443/v3/trades/AAPL?cursor=YXA9MTA%3D&apiKey=secret&limit=5"
    assert query.next_target(url, _ORIGIN) == "/v3/trades/AAPL?cursor=YXA9MTA%3D&limit=5"


def test_next_url_drops_an_encoded_api_key_name_and_a_bare_one() -> None:
    url = "https://api.massive.com/v3/x?api%4Bey=s&apiKey&cursor=c"
    assert query.next_target(url, _ORIGIN) == "/v3/x?cursor=c"


def test_next_url_with_only_an_api_key_has_no_question_mark() -> None:
    assert query.next_target("https://api.massive.com/v3/x?apiKey=s", _ORIGIN) == "/v3/x"
    assert query.next_target("https://api.massive.com", _ORIGIN) == "/"


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/v3/x?cursor=c",
        "http://api.massive.com/v3/x",
        "https://api.massive.com:8443/v3/x",
        "https://api.massive.com.evil.example/v3/x",
    ],
)
def test_next_url_on_another_origin_is_refused(url: str) -> None:
    with pytest.raises(ValueError, match=r"leaves https://api\.massive\.com:443"):
        query.next_target(url, _ORIGIN)


@pytest.mark.parametrize(
    "url",
    [
        "https://user:pw@api.polygon.io/v3/x?cursor=c&apiKey=secret",
        "https://api.massive.com:99999/v3/x?apiKey=secret",
        "ftp://api.massive.com/v3/x?apiKey=secret",
        "/v3/x?api%4Bey=secret",
    ],
)
def test_a_refused_next_url_is_quoted_without_credentials(url: str) -> None:
    with pytest.raises(ValueError, match="leaves") as caught:
        query.next_target(url, _ORIGIN)
    message = str(caught.value)
    assert "secret" not in message
    assert "pw" not in message


def test_a_refused_next_url_keeps_the_rest_of_its_query() -> None:
    url = "https://user:pw@api.polygon.io/v3/x?cursor=c&apiKey=secret"
    with pytest.raises(ValueError, match="leaves") as caught:
        query.next_target(url, _ORIGIN)
    assert str(caught.value) == (
        "next_url 'https://api.polygon.io/v3/x?cursor=c' leaves https://api.massive.com:443"
    )


def test_next_url_ignores_user_info_and_fragment() -> None:
    url = "https://user:pw@api.massive.com/v3/x?cursor=c#frag"
    assert query.next_target(url, _ORIGIN) == "/v3/x?cursor=c"
