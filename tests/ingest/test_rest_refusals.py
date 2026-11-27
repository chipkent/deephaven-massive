"""Tests for dropping the symbols a REST refusal names and sending the rest of the request again.

The search of a refusal that names no symbol runs through `RestReader.read_each` against the
fake REST server (`tests/ingest/test_rest.py`); these tests drive `drop_named_refusals` with a
scripted `send`, which stands for each round of requests.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._errors import EntitlementError, MassiveError, SymbolRefusalError
from deephaven_massive._ingest import rest_requests
from deephaven_massive._ingest.rest_refusals import drop_named_refusals, search_refusals
from deephaven_massive._massive import RestPath, Transport

if TYPE_CHECKING:
    from collections.abc import Sequence

    from deephaven_massive._ingest.symbol_reads import RestUnit

PATH = RestPath(key="/v3/snapshot", fields=(), params=())
SNAPSHOT = DatasetSpec(
    summary="A dataset for the tests.",
    name="snapshots",
    fetch_path="/v3/snapshot",
    symbol_param="ticker.any_of",
    keys={(Transport.REST, "/v3/snapshot"): SourceKeys(symbol=KeySpec("Ticker"))},
)
# The options underlyings' list sends an index bare, so `DJT` and `I:DJT` are one wire symbol.
UNDERLYINGS = DatasetSpec(
    summary="A dataset for the tests.",
    name="options.open_interest",
    fetch_path="/v3/snapshot",
    symbol_param=rest_requests.BARE_INDEX_LIST_PARAM,
    keys={(Transport.REST, "/v3/snapshot"): SourceKeys(symbol=KeySpec("Ticker"))},
)

Answer = str | MassiveError
"""A scripted answer: rows, named by their text, or a failure."""


def listed(*symbols: str) -> RestUnit:
    return PATH, rest_requests.Call({}, {"ticker.any_of": symbols}, symbols=symbols)


def refused(*symbols: str) -> SymbolRefusalError:
    return SymbolRefusalError(f"delayed for {', '.join(symbols)}", symbols=symbols)


class Rounds:
    """A scripted `send`: each round's answers in turn, by the symbols of each request."""

    def __init__(self, *rounds: dict[tuple[str, ...], Answer]) -> None:
        self.rounds = list(rounds)
        self.sent: list[list[tuple[str, ...]]] = []

    def __call__(self, units: Sequence[RestUnit]) -> list[Answer]:
        named = [call.symbols for _, call in units]
        self.sent.append(named)
        answers = self.rounds.pop(0)
        return [answers[symbols] for symbols in named]


def named(units: Sequence[RestUnit], done: Sequence[Answer]) -> list[tuple[tuple[str, ...], str]]:
    # Each request's symbols, with its answer or its refusal's text.
    return [(call.symbols, str(answer)) for (_, call), answer in zip(units, done, strict=True)]


def test_a_refusal_naming_some_symbols_refuses_them_and_sends_the_others_again() -> None:
    error = refused("MSFT")
    send = Rounds({("AAPL", "NVDA"): "rows of AAPL and NVDA"})
    units, done = drop_named_refusals(
        SNAPSHOT, [listed("AAPL", "MSFT", "NVDA")], [error], send=send
    )
    assert send.sent == [[("AAPL", "NVDA")]]
    assert named(units, done) == [
        (("MSFT",), "delayed for MSFT"),
        (("AAPL", "NVDA"), "rows of AAPL and NVDA"),
    ]
    assert done[0] is error  # the refused part keeps Massive's answer
    assert units[1][1].query["ticker.any_of"] == ("AAPL", "NVDA")


def test_a_request_sent_again_and_refused_again_is_split_again_until_none_is_refused() -> None:
    send = Rounds({("AAPL", "NVDA"): refused("NVDA")}, {("AAPL",): "rows of AAPL"})
    first: list[Answer] = [refused("MSFT"), "rows of TSLA"]
    units, done = drop_named_refusals(
        SNAPSHOT, [listed("AAPL", "MSFT", "NVDA"), listed("TSLA")], first, send=send
    )
    assert send.sent == [[("AAPL", "NVDA")], [("AAPL",)]]
    assert named(units, done) == [
        (("MSFT",), "delayed for MSFT"),
        (("NVDA",), "delayed for NVDA"),
        (("AAPL",), "rows of AAPL"),
        (("TSLA",), "rows of TSLA"),
    ]


def test_symbols_of_one_wire_spelling_are_refused_together() -> None:
    # A request can never send `DJT` without `I:DJT`, so a refusal of one refuses both.
    send = Rounds({("AAPL",): "rows of AAPL"})
    units, done = drop_named_refusals(
        UNDERLYINGS, [listed("AAPL", "DJT", "I:DJT")], [refused("I:DJT")], send=send
    )
    assert send.sent == [[("AAPL",)]]
    assert named(units, done) == [
        (("DJT", "I:DJT"), "delayed for I:DJT"),
        (("AAPL",), "rows of AAPL"),
    ]


@pytest.mark.parametrize(
    ("unit", "answer"),
    [
        pytest.param(
            (PATH, rest_requests.Call({}, {}, "AAPL", symbols=("AAPL",))),
            refused("AAPL"),
            id="a request of one symbol",
        ),
        pytest.param(listed("AAPL", "MSFT"), refused("AAPL", "MSFT"), id="every symbol named"),
        pytest.param(listed("AAPL", "MSFT"), refused("NVDA"), id="none of its symbols named"),
        pytest.param(
            listed("AAPL", "MSFT"), EntitlementError("refused"), id="a refusal naming no symbol"
        ),
        pytest.param(listed("AAPL", "MSFT"), "rows", id="an answer"),
    ],
)
def test_a_request_with_nothing_left_to_send_again_is_kept_as_it_was(
    unit: RestUnit, answer: Answer
) -> None:
    send = Rounds()
    units, done = drop_named_refusals(SNAPSHOT, [unit], [answer], send=send)
    assert (units, done) == ([unit], [answer])
    assert send.sent == []


def test_a_part_of_a_search_refused_by_name_refuses_those_and_sends_the_rest_again() -> None:
    whole = EntitlementError("NOT_AUTHORIZED", status=403)
    send = Rounds(
        {("A", "B"): refused("A"), ("C", "D"): "rows of C and D"},
        {("B",): "rows of B"},
    )
    units, done = search_refusals(SNAPSHOT, [listed("A", "B", "C", "D")], [whole], send=send)
    assert send.sent == [[("A", "B"), ("C", "D")], [("B",)]]
    assert named(units, done) == [
        (("C", "D"), "rows of C and D"),
        (("B",), "rows of B"),
        (("A",), "delayed for A"),
    ]
