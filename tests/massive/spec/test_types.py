from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, cast

import pytest

from deephaven_massive import _enums
from deephaven_massive._massive.spec.types import (
    ENVELOPE_KEYS,
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


def test_a_field_records_a_declared_conversion() -> None:
    size = Field(
        wire_key="x", column="Size", wire_type=WireType.LONG, declared_type=WireType.STRING
    )
    assert (size.wire_type, size.declared_type) == (WireType.LONG, WireType.STRING)
    assert _price().declared_type is None


def test_a_declared_conversion_may_keep_the_type() -> None:
    # An identifier typed as text that the vendor also sends as an integer stays text.
    trade_id = Field(
        wire_key="i", column="Id", wire_type=WireType.STRING, declared_type=WireType.STRING
    )
    assert trade_id.declared_type is trade_id.wire_type


# Membership.


def test_a_membership_without_projection() -> None:
    membership = Membership(kind="trades", group="stocks", position=0)
    assert membership.projection is None


def test_a_projection_is_copied_and_read_only() -> None:
    mapping = {"last_updated": "Timestamp"}
    membership = Membership(kind="values", group="indices", position=0, projection=mapping)
    mapping["value"] = "Value"
    assert membership.projection == {"last_updated": "Timestamp"}
    with pytest.raises(TypeError):
        cast("dict[str, str]", membership.projection)["value"] = "Value"


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


def test_a_timestamp_param_keeps_its_unit() -> None:
    since = Param(name="timestamp.gte", kind=ParamKind.TIMESTAMP, unit=WireUnit.MS)
    assert since.unit is WireUnit.MS


def test_a_timestamp_param_without_a_unit_is_rejected() -> None:
    with pytest.raises(ValueError, match="'timestamp': a timestamp needs a unit"):
        Param(name="timestamp", kind=ParamKind.TIMESTAMP)


@pytest.mark.parametrize(
    "kind", [k for k in ParamKind if k not in {ParamKind.TIMESTAMP, ParamKind.ENUM}]
)
def test_a_unit_on_anything_but_a_timestamp_param_is_rejected(kind: ParamKind) -> None:
    with pytest.raises(ValueError, match="'p': a timestamp needs a unit"):
        Param(name="p", kind=kind, unit=WireUnit.MS)


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


def test_a_websocket_key_names_the_cluster_and_channel() -> None:
    stocks = Source(key="stocks/T", transport=Transport.WS, fields=(_price(),))
    options = Source(key="options/T", transport=Transport.WS, fields=(_price(),))
    assert stocks != options


# Projections.


def test_a_projection_of_known_wire_keys_is_accepted() -> None:
    membership = Membership(
        kind="values", group="indices", position=0, projection={"p": "Value", "t": "Timestamp"}
    )
    source = Source(
        key="indices/V",
        transport=Transport.WS,
        fields=(_price(), _time()),
        memberships=(membership,),
    )
    assert source.memberships == (membership,)


@pytest.mark.parametrize(
    ("projection", "message"),
    [
        (
            {"p": "Value", "zz": "Other"},
            r"projection of values/indices names unknown wire keys \['zz'\]",
        ),
        (
            {"p": "Value", "t": "Value"},
            r"projection of values/indices repeats columns \['Value'\]",
        ),
    ],
    ids=["unknown wire key", "repeated column"],
)
def test_an_inconsistent_projection_is_rejected(projection: dict[str, str], message: str) -> None:
    membership = Membership(kind="values", group="indices", position=0, projection=projection)
    with pytest.raises(ValueError, match=message):
        Source(
            key="indices/V",
            transport=Transport.WS,
            fields=(_price(), _time()),
            memberships=(membership,),
        )


# RestPath.

_TICKER = Param(name="stockTicker", kind=ParamKind.STRING, in_path=True)


def test_a_rest_path_is_a_rest_source() -> None:
    path = RestPath(key="/v3/trades/{stockTicker}", fields=(_price(),), params=(_TICKER,))
    assert isinstance(path, Source)
    assert path.transport is Transport.REST
    assert path.template == "/v3/trades/{stockTicker}"
    assert (path.result_key, path.params) == ("results", (_TICKER,))


def test_a_rest_path_transport_cannot_be_passed() -> None:
    kwargs: dict[str, Any] = {"key": "/x", "fields": (), "transport": Transport.WS}
    with pytest.raises(TypeError, match="transport"):
        RestPath(**kwargs)


def test_a_rest_path_checks_the_source_invariants() -> None:
    with pytest.raises(ValueError, match=r"repeated wire keys \[.p.\]"):
        RestPath(key="/x", fields=(_price(), _price()))


def test_a_rest_path_keeps_its_result_key_and_params() -> None:
    params = (
        Param(name="from", kind=ParamKind.STRING, in_path=True),
        Param(name="to", kind=ParamKind.STRING, in_path=True),
    )
    path = RestPath(key="/v1/last/crypto/{from}/{to}", fields=(), result_key="last", params=params)
    assert (path.result_key, path.params) == ("last", params)


def test_the_envelope_keys_are_the_ones_that_describe_a_response() -> None:
    assert frozenset({"status", "request_id", "next_url", "count"}) == ENVELOPE_KEYS


def test_a_rest_path_without_an_envelope_has_no_result_key() -> None:
    path = RestPath(key="/v1/marketstatus/now", fields=(), result_key=None)
    assert path.result_key is None


def test_query_parameters_need_no_placeholder() -> None:
    limit = Param(name="limit", kind=ParamKind.INTEGER)
    path = RestPath(key="/v3/trades/{stockTicker}", fields=(), params=(_TICKER, limit))
    assert path.params == (_TICKER, limit)


@pytest.mark.parametrize(
    ("key", "params", "message"),
    [
        (
            "/v3/trades/{stockTicker}",
            (_TICKER, Param(name="stockTicker", kind=ParamKind.STRING)),
            r"repeated parameters \['stockTicker'\]",
        ),
        (
            "/v1/last/crypto/{from}/{to}",
            (Param(name="from", kind=ParamKind.STRING, in_path=True),),
            r"placeholders without a path parameter \['to'\]",
        ),
        (
            "/v3/trades/{stockTicker}",
            (Param(name="stockTicker", kind=ParamKind.STRING),),  # a query parameter
            r"placeholders without a path parameter \['stockTicker'\]",
        ),
        (
            "/v3/trades",
            (_TICKER,),
            r"path parameters without a placeholder \['stockTicker'\]",
        ),
    ],
    ids=["repeated name", "missing path parameter", "placeholder as query", "extra path parameter"],
)
def test_an_inconsistent_rest_path_is_rejected(
    key: str, params: tuple[Param, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        RestPath(key=key, fields=(), params=params)


# DriftReport.


def test_a_drift_report_defaults_to_no_drift() -> None:
    assert DriftReport() == DriftReport(
        extra=frozenset(), missing=frozenset(), statuses=frozenset()
    )


# Every value type.


def _values() -> list[DataclassInstance]:
    """Return one value of each type; each call builds new, equal instances.

    Returns:
        The values.
    """
    return [
        _price(),
        Membership(kind="values", group="indices", position=0, projection={"p": "Value"}),
        Param(name="limit", kind=ParamKind.INTEGER),
        Source(key="stocks/T", transport=Transport.WS, fields=(_price(),)),
        RestPath(key="/v3/trades/{stockTicker}", fields=(), params=(_TICKER,)),
        DriftReport(extra=frozenset({"zz"})),
    ]


@pytest.mark.parametrize(
    ("value", "twin"),
    list(zip(_values(), _values(), strict=True)),
    ids=[type(value).__name__ for value in _values()],
)
def test_values_are_frozen_and_hash_by_value(
    value: DataclassInstance, twin: DataclassInstance
) -> None:
    attribute = dataclasses.fields(value)[0].name
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(value, attribute, None)
    assert value is not twin
    assert value == twin
    assert hash(value) == hash(twin)
