from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, cast

import pytest

from deephaven_massive import _enums
from deephaven_massive._massive.spec.types import (
    DriftReport,
    Field,
    Membership,
    Param,
    ParamKind,
    RestPath,
    Source,
    Transport,
    WireType,
    WireUnit,
)

if TYPE_CHECKING:
    from _typeshed import DataclassInstance


def _price() -> Field:
    return Field(wire_key="p", column="Price", wire_type=WireType.DOUBLE)


def _time() -> Field:
    return Field(wire_key="t", column="SipTimestamp", wire_type=WireType.EPOCH, unit=WireUnit.MS)


# Enums.


def test_transport_matches_the_library_enum() -> None:
    # The library's own `Transport` feeds the diagnostics tables; until it is replaced by this
    # one, the two must not drift apart.
    assert [(m.name, m.value) for m in Transport] == [(m.name, m.value) for m in _enums.Transport]


def test_wire_type_values_are_the_generated_spellings() -> None:
    assert [t.value for t in WireType] == [
        "long",
        "double",
        "bool",
        "string",
        "date",
        "epoch",
        "iso-instant",
        "int-array",
        "json-text",
    ]


def test_wire_unit_values() -> None:
    assert [u.value for u in WireUnit] == ["s", "ms", "us", "ns"]


def test_param_kind_values() -> None:
    assert [k.value for k in ParamKind] == [
        "string",
        "integer",
        "number",
        "boolean",
        "enum",
        "date",
        "timestamp",
        "list",
    ]


# Field.


def test_field_defaults() -> None:
    price = _price()
    assert (price.unit, price.required) == (None, False)


def test_an_epoch_field_keeps_its_unit() -> None:
    assert _time().unit is WireUnit.MS


def test_an_epoch_without_a_unit_is_rejected() -> None:
    with pytest.raises(ValueError, match="'t': an epoch needs a unit"):
        Field(wire_key="t", column="SipTimestamp", wire_type=WireType.EPOCH)


@pytest.mark.parametrize("wire_type", [t for t in WireType if t is not WireType.EPOCH])
def test_a_unit_on_anything_but_an_epoch_is_rejected(wire_type: WireType) -> None:
    with pytest.raises(ValueError, match="'x': an epoch needs a unit"):
        Field(wire_key="x", column="X", wire_type=wire_type, unit=WireUnit.NS)


# Membership.


def test_a_membership_without_projection() -> None:
    membership = Membership(kind="trades", group="stocks", position=0)
    assert membership.projection is None


def test_a_projection_is_copied_and_read_only() -> None:
    mapping = {"last_updated": "timestamp"}
    membership = Membership(kind="values", group="indices", position=0, projection=mapping)
    mapping["value"] = "value"
    assert membership.projection == {"last_updated": "timestamp"}
    with pytest.raises(TypeError):
        cast("dict[str, str]", membership.projection)["value"] = "value"


def test_memberships_with_equal_projections_are_equal_and_hashable() -> None:
    first = Membership(kind="values", group="indices", position=0, projection={"a": "b"})
    second = Membership(kind="values", group="indices", position=0, projection={"a": "b"})
    other = Membership(kind="values", group="indices", position=0, projection={"a": "c"})
    assert first == second
    assert hash(first) == hash(second)
    assert first != other


def test_a_negative_position_is_rejected() -> None:
    with pytest.raises(ValueError, match="trades/stocks: negative position"):
        Membership(kind="trades", group="stocks", position=-1)


def test_an_empty_projection_is_rejected() -> None:
    with pytest.raises(ValueError, match="values/indices: empty projection"):
        Membership(kind="values", group="indices", position=0, projection={})


# Param.


def test_an_enum_param_keeps_its_values() -> None:
    order = Param(name="order", kind=ParamKind.ENUM, values=("asc", "desc"))
    assert (order.values, order.in_path) == (("asc", "desc"), False)


def test_an_enum_param_without_values_is_rejected() -> None:
    with pytest.raises(ValueError, match="'order': an enum needs values"):
        Param(name="order", kind=ParamKind.ENUM)


def test_values_on_a_non_enum_param_are_rejected() -> None:
    with pytest.raises(ValueError, match="'limit': an enum needs values"):
        Param(name="limit", kind=ParamKind.INTEGER, values=("1",))


# Source.


def test_field_finds_a_column() -> None:
    source = Source(key="T", transport=Transport.WS, fields=(_price(), _time()))
    assert source.field("SipTimestamp") == _time()
    assert source.field("Price") == _price()


def test_field_names_the_source_when_the_column_is_missing() -> None:
    source = Source(key="T", transport=Transport.WS, fields=(_price(),))
    with pytest.raises(KeyError, match="source 'T' has no column 'Size'"):
        source.field("Size")


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        (
            (_price(), Field(wire_key="p", column="Other", wire_type=WireType.LONG)),
            r"repeated wire keys \['p'\]",
        ),
        (
            (_price(), Field(wire_key="q", column="Price", wire_type=WireType.LONG)),
            r"repeated columns \['Price'\]",
        ),
    ],
    ids=["wire key", "column"],
)
def test_a_repeated_field_is_rejected(fields: tuple[Field, ...], message: str) -> None:
    with pytest.raises(ValueError, match=f"source 'T': {message}"):
        Source(key="T", transport=Transport.WS, fields=fields)


def test_a_repeated_membership_is_rejected() -> None:
    memberships = (
        Membership(kind="trades", group="stocks", position=0),
        Membership(kind="trades", group="stocks", position=1),
        Membership(kind="trades", group="crypto", position=0),
    )
    with pytest.raises(ValueError, match=r"repeated memberships \['trades/stocks'\]"):
        Source(key="T", transport=Transport.WS, fields=(), memberships=memberships)


def test_a_source_in_several_kinds_is_accepted() -> None:
    memberships = (
        Membership(kind="trades", group="stocks", position=0),
        Membership(kind="last_trade", group="stocks", position=0),
    )
    source = Source(key="T", transport=Transport.WS, fields=(), memberships=memberships)
    assert source.memberships == memberships


# RestPath.


def test_a_rest_path_is_a_rest_source() -> None:
    path = RestPath(key="/v3/trades/{stockTicker}", fields=(_price(),))
    assert isinstance(path, Source)
    assert path.transport is Transport.REST
    assert path.template == "/v3/trades/{stockTicker}"
    assert (path.result_key, path.params) == ("results", ())


def test_a_rest_path_transport_cannot_be_passed() -> None:
    kwargs: dict[str, Any] = {"key": "/x", "fields": (), "transport": Transport.WS}
    with pytest.raises(TypeError, match="transport"):
        RestPath(**kwargs)


def test_a_rest_path_checks_the_source_invariants() -> None:
    with pytest.raises(ValueError, match=r"repeated wire keys \[.p.\]"):
        RestPath(key="/x", fields=(_price(), _price()))


def test_a_rest_path_keeps_its_result_key_and_params() -> None:
    ticker = Param(name="stocksTicker", kind=ParamKind.STRING, in_path=True)
    path = RestPath(
        key="/v1/last/crypto/{from}/{to}", fields=(), result_key="last", params=(ticker,)
    )
    assert (path.result_key, path.params) == ("last", (ticker,))


# DriftReport.


def test_a_drift_report_defaults_to_no_drift() -> None:
    assert DriftReport() == DriftReport(extra=frozenset(), missing=frozenset())


# Every value type.


@pytest.mark.parametrize(
    "value",
    [
        _price(),
        Membership(kind="trades", group="stocks", position=0),
        Param(name="limit", kind=ParamKind.INTEGER),
        Source(key="T", transport=Transport.WS, fields=(_price(),)),
        RestPath(key="/x", fields=()),
        DriftReport(extra=frozenset({"zz"})),
    ],
    ids=lambda value: type(value).__name__,
)
def test_values_are_frozen_and_hashable(value: DataclassInstance) -> None:
    attribute = dataclasses.fields(value)[0].name
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(value, attribute, None)
    assert hash(value) == hash(value)
