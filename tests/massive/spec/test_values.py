"""Tests for the wire text of REST parameter values, by kind."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from enum import Enum, IntEnum, StrEnum

import numpy as np
import pytest

from deephaven_massive._massive.spec import values
from deephaven_massive._massive.spec.types import Param, ParamKind, WireUnit


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


_PARAMS = (
    Param(name="order", kind=ParamKind.ENUM, values=("asc", "desc")),
    Param(name="limit", kind=ParamKind.INTEGER),
    Param(name="strike_price.gte", kind=ParamKind.NUMBER),
    Param(name="adjusted", kind=ParamKind.BOOLEAN),
    Param(name="timestamp.gte", kind=ParamKind.TIMESTAMP, unit=WireUnit.NS),
    Param(name="published_utc.gte", kind=ParamKind.DATE),
    Param(name="ticker.any_of", kind=ParamKind.LIST),
    Param(name="cursor", kind=ParamKind.STRING),
)
_BY_NAME = {p.name: p for p in _PARAMS}


def _encode(name: str, value: object) -> str:
    return values.encode_value(_BY_NAME[name], value)


# Values by kind.


def test_every_kind_has_an_encoder() -> None:
    assert set(values._ENCODERS) == set(ParamKind)


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
    # Positional notation, never an exponent, and no trailing zeros.
    assert _encode("strike_price.gte", 1e-7) == "0.0000001"
    assert _encode("strike_price.gte", 1e20) == "100000000000000000000"
    assert _encode("strike_price.gte", 2.0) == "2"
    assert _encode("strike_price.gte", 150.250) == "150.25"
    assert _encode("strike_price.gte", -0.0) == "0"
    assert _encode("strike_price.gte", -1.5e-3) == "-0.0015"
    assert _encode("strike_price.gte", 10**30) == "1" + "0" * 30  # an int is sent exactly


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


def test_an_instant_for_a_date_parameter_is_iso_date_time_in_utc() -> None:
    eastern = timezone(timedelta(hours=-5))
    value = datetime(2026, 1, 2, 9, 30, 0, 123456, tzinfo=eastern)
    assert _encode("published_utc.gte", value) == "2026-01-02T14:30:00.123456Z"
    assert _encode("published_utc.gte", datetime(2026, 1, 2, tzinfo=UTC)) == "2026-01-02T00:00:00Z"


def test_a_label_names_the_value_in_every_message() -> None:
    param = _BY_NAME["limit"]
    with pytest.raises(TypeError, match=r"^stocks\.x filter 'limit' takes integer values"):
        values.encode_value(param, "5", label="stocks.x filter 'limit'")


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
    assert values.encode_value(param, value) == expected


def test_nanoseconds_below_the_microsecond_are_kept() -> None:
    value = _NanoDatetime(2026, 1, 1, 0, 0, 0, 123456, tzinfo=UTC)
    param = Param(name="timestamp", kind=ParamKind.TIMESTAMP, unit=WireUnit.NS)
    assert values.encode_value(param, value) == "1767225600123456789"


@pytest.mark.parametrize("unit", [WireUnit.US, WireUnit.MS])
def test_nanoseconds_below_a_coarser_unit_are_refused_not_dropped(unit: WireUnit) -> None:
    value = _NanoDatetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)
    param = Param(name="timestamp", kind=ParamKind.TIMESTAMP, unit=unit)
    with pytest.raises(ValueError, match=f"takes whole {unit} since the epoch"):
        values.encode_value(param, value)


def test_an_instant_in_seconds_is_exact() -> None:
    param = Param(name="timestamp", kind=ParamKind.TIMESTAMP, unit=WireUnit.S)
    assert values.encode_value(param, datetime(2026, 1, 1, tzinfo=UTC)) == "1767225600"


@pytest.mark.parametrize(("unit", "micros"), [(WireUnit.MS, 123456), (WireUnit.S, 1000)])
def test_an_instant_finer_than_the_unit_is_refused(unit: WireUnit, micros: int) -> None:
    param = Param(name="timestamp", kind=ParamKind.TIMESTAMP, unit=unit)
    value = datetime(2026, 1, 1, 0, 0, 0, micros, tzinfo=UTC)
    with pytest.raises(ValueError, match=f"'timestamp' takes whole {unit} since the epoch"):
        values.encode_value(param, value)


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
