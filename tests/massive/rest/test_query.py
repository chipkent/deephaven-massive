from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from enum import Enum, IntEnum, StrEnum

import numpy as np
import pytest

from deephaven_massive._massive.rest import query
from deephaven_massive._massive.spec.types import Param, ParamKind, RestPath, WireUnit


class _Order(StrEnum):
    ASC = "asc"
    DESC = "desc"


class _Code(Enum):
    ONE = 1


class _Limit(IntEnum):
    BIG = 50_000


class _NanoDatetime(datetime):
    """A datetime that carries nanoseconds below the microsecond, like `pandas.Timestamp`."""

    nanosecond = 789


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
_BY_NAME = {p.name: p for p in _PARAMS}
_ORIGIN = ("https", "api.massive.com", 443)


def _encode(name: str, value: object) -> str:
    return query.encode_value(_BY_NAME[name], value)


def _target(**query_params: object) -> str:
    return query.request_target(
        _PATH, {"stockTicker": "AAPL", "date": date(2026, 1, 2)}, query_params
    )


# Values by kind.


def test_every_kind_has_an_encoder() -> None:
    assert set(query._ENCODERS) == set(ParamKind)


def test_text_is_sent_unchanged_and_an_enum_by_its_value() -> None:
    assert _encode("cursor", "abc") == "abc"
    assert _encode("order", _Order.DESC) == "desc"
    assert _encode("order", "asc") == "asc"


def test_an_enum_value_outside_the_parameter_values_is_refused() -> None:
    with pytest.raises(ValueError, match=r"'order' takes one of \['asc', 'desc'\], not 'up'"):
        _encode("order", "up")


@pytest.mark.parametrize("value", [1, None, b"x", _Code.ONE])
def test_text_parameters_refuse_other_types(value: object) -> None:
    with pytest.raises(TypeError, match="'cursor' takes text"):
        _encode("cursor", value)


def test_integers_and_numbers() -> None:
    assert _encode("limit", 50_000) == "50000"
    assert _encode("strike_price.gte", 100) == "100"
    assert _encode("strike_price.gte", 0.1) == "0.1"
    assert _encode("strike_price.gte", 1e-7) == "1e-07"


def test_number_subclasses_are_sent_by_value_never_by_their_repr() -> None:
    assert _encode("limit", _Limit.BIG) == "50000"
    assert _encode("strike_price.gte", _Limit.BIG) == "50000"
    # numpy's float64 subclasses float, and its repr is "np.float64(1.5)".
    assert _encode("strike_price.gte", np.float64(1.5)) == "1.5"


def test_an_integer_of_any_size_is_sent_exactly() -> None:
    assert _encode("limit", 10**400) == "1" + "0" * 400
    assert _encode("strike_price.gte", -(10**400)) == "-1" + "0" * 400


@pytest.mark.parametrize(("name", "value"), [("limit", True), ("limit", 1.0), ("limit", "5")])
def test_an_integer_refuses_bool_float_and_text(name: str, value: object) -> None:
    with pytest.raises(TypeError, match=f"'{name}' takes integer values"):
        _encode(name, value)


@pytest.mark.parametrize("value", [False, "1.5", None])
def test_a_number_refuses_bool_and_text(value: object) -> None:
    with pytest.raises(TypeError, match=r"'strike_price\.gte' takes number values"):
        _encode("strike_price.gte", value)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_a_number_refuses_non_finite_values(value: float) -> None:
    with pytest.raises(ValueError, match="takes a finite number"):
        _encode("strike_price.gte", value)


def test_a_bool_is_true_or_false() -> None:
    assert _encode("adjusted", value=True) == "true"
    assert _encode("adjusted", value=False) == "false"
    with pytest.raises(TypeError, match="'adjusted' takes a bool, not 1"):
        _encode("adjusted", 1)


def test_dates_are_iso() -> None:
    assert _encode("published_utc.gte", date(2026, 1, 2)) == "2026-01-02"
    assert _encode("timestamp.gte", date(2026, 1, 2)) == "2026-01-02"


def test_an_instant_for_a_date_parameter_is_iso_date_time() -> None:
    eastern = timezone(timedelta(hours=-5))
    value = datetime(2026, 1, 2, 9, 30, 0, 123456, tzinfo=eastern)
    assert _encode("published_utc.gte", value) == "2026-01-02T09:30:00.123456-05:00"


def test_an_instant_for_a_timestamp_parameter_is_exact_integer_nanoseconds() -> None:
    # 2262-04-11 is near the int64 nanosecond limit, where a float loses the microseconds.
    value = datetime(2262, 4, 11, 23, 47, 16, 854775, tzinfo=UTC)
    assert _encode("timestamp.gte", value) == "9223372036854775000"
    early = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    assert _encode("timestamp.gte", early) == "-1000"


@pytest.mark.parametrize(
    ("unit", "expected"),
    [
        (WireUnit.NS, "1767225600123000000"),
        (WireUnit.US, "1767225600123000"),
        (WireUnit.MS, "1767225600123"),
    ],
)
def test_an_instant_is_sent_in_the_parameter_unit(unit: WireUnit, expected: str) -> None:
    param = Param(name="timestamp", kind=ParamKind.TIMESTAMP, unit=unit)
    value = datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)
    assert query.encode_value(param, value) == expected


def test_nanoseconds_below_the_microsecond_are_kept() -> None:
    value = _NanoDatetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    param = Param(name="timestamp", kind=ParamKind.TIMESTAMP, unit=WireUnit.NS)
    assert query.encode_value(param, value) == "1767225600123456789"


@pytest.mark.parametrize("unit", [WireUnit.US, WireUnit.MS])
def test_nanoseconds_below_a_coarser_unit_are_refused_not_dropped(unit: WireUnit) -> None:
    value = _NanoDatetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)
    param = Param(name="timestamp", kind=ParamKind.TIMESTAMP, unit=unit)
    with pytest.raises(ValueError, match=f"takes whole {unit} since the epoch"):
        query.encode_value(param, value)


def test_an_instant_in_seconds_is_exact() -> None:
    param = Param(name="timestamp", kind=ParamKind.TIMESTAMP, unit=WireUnit.S)
    assert query.encode_value(param, datetime(2026, 1, 1, tzinfo=UTC)) == "1767225600"


@pytest.mark.parametrize(("unit", "micros"), [(WireUnit.MS, 123456), (WireUnit.S, 1000)])
def test_an_instant_finer_than_the_unit_is_refused(unit: WireUnit, micros: int) -> None:
    param = Param(name="timestamp", kind=ParamKind.TIMESTAMP, unit=unit)
    value = datetime(2026, 1, 1, 0, 0, 0, micros, tzinfo=UTC)
    with pytest.raises(ValueError, match=f"'timestamp' takes whole {unit} since the epoch"):
        query.encode_value(param, value)


@pytest.mark.parametrize("name", ["timestamp.gte", "published_utc.gte"])
def test_a_naive_datetime_is_refused(name: str) -> None:
    with pytest.raises(ValueError, match="a naive datetime has no instant"):
        _encode(name, datetime(2026, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None))


@pytest.mark.parametrize("name", ["timestamp.gte", "published_utc.gte"])
def test_a_date_parameter_refuses_text(name: str) -> None:
    with pytest.raises(TypeError, match="takes a date or an aware datetime"):
        _encode(name, "2026-01-02")


def test_a_list_is_joined_with_commas() -> None:
    assert _encode("ticker.any_of", ["AAPL", "MSFT"]) == "AAPL,MSFT"
    assert _encode("ticker.any_of", ("X:BTCUSD",)) == "X:BTCUSD"


@pytest.mark.parametrize("value", ["AAPL", {"AAPL"}, ["AAPL", 1], 5])
def test_a_list_refuses_text_sets_and_non_text_items(value: object) -> None:
    with pytest.raises(TypeError, match=r"'ticker\.any_of' takes a sequence of text"):
        _encode("ticker.any_of", value)


def test_an_empty_list_is_refused_rather_than_sent_as_no_filter() -> None:
    with pytest.raises(ValueError, match=r"'ticker\.any_of' takes at least one value"):
        _encode("ticker.any_of", [])


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
        "&published_utc.gte=2026-01-02T00%3A00%3A00-05%3A00"
    )


def test_the_arguments_are_not_modified() -> None:
    path_params: dict[str, object] = {"stockTicker": "AAPL", "date": date(2026, 1, 2)}
    query_params: dict[str, object] = {"ticker.any_of": ["A"], "limit": None}
    query.request_target(_PATH, path_params, query_params)
    assert path_params == {"stockTicker": "AAPL", "date": date(2026, 1, 2)}
    assert query_params == {"ticker.any_of": ["A"], "limit": None}


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
