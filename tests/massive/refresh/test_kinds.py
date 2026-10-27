"""Tests for the dataset kinds: members, the type and transport agreement, and the column order.

Each function reads plain values (generated `Source`s and hand input), so each rule and gate is
shown on hand-built sources; `test_generate.py` shows them on the vendored snapshot. Every
gate has a test that fails when the gate is removed.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import kinds, overrides
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.spec.types import (
    Field,
    Membership,
    RestPath,
    Source,
    Transport,
    WireType,
    WireUnit,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from deephaven_massive._massive.refresh.gates import Problem

# Hand input with every table empty; each test replaces the tables it needs.
_EMPTY = HandInput(
    lexicons={},
    shapes={},
    identity={},
    names={},
    units={},
    tag_conflicts={},
    type_fixes={},
    result_keys={},
    param_units={},
    exclusions=(),
    kinds={},
    sample_errors=(),
    projections={},
    transport_only={},
    venues={},
)


def _field(wire_key: str, column: str, wire_type: WireType = WireType.DOUBLE) -> Field:
    unit = WireUnit.MS if wire_type is WireType.EPOCH else None
    return Field(
        wire_key=wire_key,
        column=column,
        wire_type=wire_type,
        unit=unit,
        description=f"The {wire_key}.",
    )


_PATH = RestPath(
    key="/v3/trades",
    fields=(_field("price", "Price"), _field("size", "Size"), _field("id", "Id", WireType.STRING)),
)
_CHANNEL = Source(
    key="stocks/T",
    transport=Transport.WS,
    fields=(_field("sym", "Ticker", WireType.STRING), _field("p", "Price"), _field("s", "Size")),
)
_FILE = Source(
    key="us_stocks_sip/trades_v1",
    transport=Transport.FLAT_FILES,
    fields=(
        _field("ticker", "Ticker", WireType.STRING),
        _field("size", "Size"),
        _field("price", "Price"),
    ),
)
_SOURCES = {s.key: s for s in (_PATH, _CHANNEL, _FILE)}
_TRADES = {"trades": {"stocks": ["/v3/trades", "stocks/T", "us_stocks_sip/trades_v1"]}}


def _members(hand: HandInput, sources: dict[str, Source] = _SOURCES) -> list[kinds.Member]:
    found, problems = kinds.members(hand, sources)
    assert problems == []
    return found


def _found(problems: Iterable[Problem]) -> list[tuple[str, str, str]]:
    return sorted((p.gate, p.source, p.key) for p in problems)


# --- Members --------------------------------------------------------------------------------


def test_members_follow_kinds_in_order_with_their_positions() -> None:
    found = _members(dataclasses.replace(_EMPTY, kinds=_TRADES))
    assert [(m.kind, m.group, m.position, m.source.key) for m in found] == [
        ("trades", "stocks", 0, "/v3/trades"),
        ("trades", "stocks", 1, "stocks/T"),
        ("trades", "stocks", 2, "us_stocks_sip/trades_v1"),
    ]
    assert found[0].columns() == [(f.column, f) for f in _PATH.fields]


def test_a_projected_member_contributes_its_projection_under_the_kind_s_names() -> None:
    projection = overrides.Projection(fields={"price": "last_price", "gone": "x"}, reason="t")
    hand = dataclasses.replace(
        _EMPTY,
        kinds={"last": {"stocks": ["/v3/trades"]}},
        projections={("last", "/v3/trades"): projection},
    )
    (member,), problems = kinds.members(hand, _SOURCES)
    assert member.projection == {"price": "LastPrice"}
    assert member.columns() == [("LastPrice", _PATH.fields[0])]
    assert _found(problems) == [("stale-entry", "/v3/trades", "gone")]


def test_gate_stale_entry_kinds_and_projections_naming_nothing() -> None:
    projection = overrides.Projection(fields={"price": "price"}, reason="t")
    hand = dataclasses.replace(
        _EMPTY,
        kinds={"trades": {"stocks": ["/v3/trades", "stocks/LV"]}},
        projections={("quotes", "/v3/trades"): projection},
    )
    found, problems = kinds.members(hand, _SOURCES)
    assert [m.source.key for m in found] == ["/v3/trades"]
    assert _found(problems) == [
        ("stale-entry", "/v3/trades", "quotes"),
        ("stale-entry", "stocks/LV", "trades"),
    ]


# --- The type lattice and type-agreement -----------------------------------------------------


@pytest.mark.parametrize(
    ("first", "second", "joined"),
    [
        (WireType.LONG, WireType.DOUBLE, WireType.DOUBLE),
        (WireType.DOUBLE, WireType.LONG, WireType.DOUBLE),
        (WireType.LONG, WireType.INT_ARRAY, WireType.INT_ARRAY),
        (WireType.EPOCH, WireType.EPOCH, WireType.EPOCH),
        (WireType.STRING, WireType.STRING, WireType.STRING),
        (WireType.STRING, WireType.LONG, None),
        (WireType.DOUBLE, WireType.INT_ARRAY, None),
        (WireType.EPOCH, WireType.LONG, None),
        (WireType.ISO_INSTANT, WireType.EPOCH, None),
    ],
)
def test_join_is_the_type_lattice(
    first: WireType, second: WireType, joined: WireType | None
) -> None:
    assert kinds.join(first, second) is joined


def test_type_agreement_passes_a_widening_and_epochs_in_two_units() -> None:
    file = dataclasses.replace(
        _FILE, fields=(*_FILE.fields[:2], _field("price", "Price", WireType.LONG))
    )
    channel = dataclasses.replace(
        _CHANNEL, fields=(*_CHANNEL.fields, _field("t", "Time", WireType.EPOCH))
    )
    nanos = Field(wire_key="t", column="Time", wire_type=WireType.EPOCH, unit=WireUnit.NS)
    path = dataclasses.replace(_PATH, fields=(*_PATH.fields, nanos))
    sources = {s.key: s for s in (path, channel, file)}
    found = _members(dataclasses.replace(_EMPTY, kinds=_TRADES), sources)
    assert list(kinds.type_agreement(found)) == []


def test_gate_type_agreement_one_column_with_two_types_prints_both_fields() -> None:
    # Within one kind, across groups: the stocks `dv` is text, the futures `dv` a number.
    stocks = Source(
        key="stocks/AM", transport=Transport.WS, fields=(_field("dv", "Dv", WireType.STRING),)
    )
    futures = Source(key="futures/AM", transport=Transport.WS, fields=(_field("dv", "Dv"),))
    more = Source(
        key="crypto/XA", transport=Transport.WS, fields=(_field("dv", "Dv", WireType.LONG),)
    )
    sources = {s.key: s for s in (stocks, futures, more)}
    hand = dataclasses.replace(
        _EMPTY,
        kinds={
            "bars": {"stocks": ["stocks/AM"], "futures": ["futures/AM"], "crypto": ["crypto/XA"]}
        },
    )
    (problem,) = kinds.type_agreement(_members(hand, sources))
    assert (problem.gate, problem.source, problem.key) == ("type-agreement", "bars", "Dv")
    assert "string stocks/AM 'dv' ('The dv.')" in problem.message
    assert "double futures/AM 'dv' ('The dv.')" in problem.message


def test_type_agreement_compares_columns_only_within_one_kind() -> None:
    other = Source(
        key="stocks/Q", transport=Transport.WS, fields=(_field("price", "Price", WireType.STRING),)
    )
    hand = dataclasses.replace(_EMPTY, kinds={**_TRADES, "quotes": {"stocks": ["stocks/Q"]}})
    assert list(kinds.type_agreement(_members(hand, {**_SOURCES, "stocks/Q": other}))) == []


# --- transport-agreement --------------------------------------------------------------------


def test_gate_transport_agreement_a_column_one_transport_alone_supplies() -> None:
    # REST's `Id` is on no other transport; `Ticker` is on two.
    found = _members(dataclasses.replace(_EMPTY, kinds=_TRADES))
    assert _found(kinds.transport_agreement(dataclasses.replace(_EMPTY, kinds=_TRADES), found)) == [
        ("transport-agreement", "trades", "Id")
    ]
    hand = dataclasses.replace(_EMPTY, kinds=_TRADES, transport_only={("trades", "Id"): "test"})
    assert list(kinds.transport_agreement(hand, found)) == []


def test_a_group_on_one_transport_needs_no_agreement() -> None:
    hand = dataclasses.replace(_EMPTY, kinds={"luld": {"stocks": ["stocks/T"]}})
    assert list(kinds.transport_agreement(hand, _members(hand))) == []


def test_gate_stale_entry_a_transport_only_column_two_transports_supply() -> None:
    hand = dataclasses.replace(
        _EMPTY,
        kinds=_TRADES,
        transport_only={
            ("trades", "Id"): "test",
            ("trades", "Price"): "test",
            ("quotes", "X"): "t",
        },
    )
    assert _found(kinds.transport_agreement(hand, _members(hand))) == [
        ("stale-entry", "quotes", "X"),
        ("stale-entry", "trades", "Price"),
    ]


# --- The column order -----------------------------------------------------------------------


def test_the_flat_file_then_the_stream_orders_every_member() -> None:
    found = _members(dataclasses.replace(_EMPTY, kinds=_TRADES))
    orders, problems = kinds.column_orders(found)
    assert problems == []
    assert orders == {
        "us_stocks_sip/trades_v1": ("Ticker", "Size", "Price"),
        "stocks/T": ("Ticker", "Size", "Price"),
        "/v3/trades": ("Size", "Price", "Id"),
    }


def test_the_stream_orders_a_group_without_a_flat_file() -> None:
    hand = dataclasses.replace(_EMPTY, kinds={"trades": {"stocks": ["/v3/trades", "stocks/T"]}})
    orders, _ = kinds.column_orders(_members(hand))
    assert orders["/v3/trades"] == ("Price", "Size", "Id")


def test_gate_column_order_a_group_with_no_stream_or_file() -> None:
    hand = dataclasses.replace(_EMPTY, kinds={"trades": {"stocks": ["/v3/trades"]}})
    orders, problems = kinds.column_orders(_members(hand))
    assert orders == {}
    assert _found(problems) == [("column-order", "/v3/trades", "trades")]


def test_gate_column_order_two_groups_that_order_a_source_differently() -> None:
    hand = dataclasses.replace(
        _EMPTY,
        kinds={
            "trades": {"stocks": ["/v3/trades", "us_stocks_sip/trades_v1"]},
            "ticks": {"stocks": ["/v3/trades", "stocks/T"]},
        },
    )
    _, problems = kinds.column_orders(_members(hand))
    assert _found(problems) == [("column-order", "/v3/trades", "ticks")]


def test_a_projected_member_is_neither_ordered_nor_an_order_reference() -> None:
    projection = overrides.Projection(fields={"size": "size"}, reason="t")
    hand = dataclasses.replace(
        _EMPTY,
        kinds={
            "trades": {"stocks": ["us_stocks_sip/trades_v1", "/v3/trades"]},
            "x": {"s": ["/v3/trades"]},
        },
        projections={("trades", "/v3/trades"): projection, ("x", "/v3/trades"): projection},
    )
    orders, problems = kinds.column_orders(_members(hand))
    assert "/v3/trades" not in orders
    assert problems == []


def test_apply_writes_memberships_and_the_order_and_keeps_the_type() -> None:
    hand = dataclasses.replace(_EMPTY, kinds=_TRADES)
    found = _members(hand)
    orders, _ = kinds.column_orders(found)
    (path,) = kinds.apply([_PATH], found, orders)
    assert isinstance(path, RestPath)
    assert [f.column for f in path.fields] == ["Size", "Price", "Id"]
    assert path.memberships == (Membership(kind="trades", group="stocks", position=0),)
    lone = Source(key="stocks/LULD", transport=Transport.WS, fields=_CHANNEL.fields)
    (unchanged,) = kinds.apply([lone], found, orders)
    assert unchanged == lone
