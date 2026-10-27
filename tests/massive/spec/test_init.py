from __future__ import annotations

import json
import subprocess
import sys
import threading
from importlib import resources
from types import MappingProxyType
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive import spec
from deephaven_massive._massive.spec.types import (
    Conversion,
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
    from collections.abc import Mapping


@pytest.fixture
def unloaded(monkeypatch: pytest.MonkeyPatch) -> dict[Transport, object]:
    """Give the test an empty cache, so the next call loads from the file again.

    Returns:
        The empty cache the module now uses.
    """
    cache: dict[Transport, object] = {}
    monkeypatch.setattr(spec, "_loaded", cache)
    return cache


def test_rest_sources_load_from_the_generated_file(unloaded: dict[Transport, object]) -> None:
    rest = spec.sources(Transport.REST)
    text = resources.files(spec).joinpath("rest.json").read_text(encoding="utf-8")
    assert list(rest) == [entry["key"] for entry in json.loads(text)["sources"]]
    assert all(isinstance(value, RestPath) for value in rest.values())
    assert isinstance(rest, MappingProxyType)
    assert unloaded[Transport.REST] is rest


def test_the_loaded_values_are_the_generated_ones() -> None:
    trades = spec.source(Transport.REST, "/v3/trades/{stockTicker}")
    assert trades.result_key == "results"
    assert trades.field("SipTimestamp") == Field(
        wire_key="sip_timestamp",
        column="SipTimestamp",
        wire_type=WireType.EPOCH,
        unit=WireUnit.NS,
        required=True,
    )
    assert trades.params[0] == Param(name="stockTicker", kind=ParamKind.STRING, in_path=True)
    gte = next(p for p in trades.params if p.name == "timestamp.gte")
    assert (gte.kind, gte.unit) == (ParamKind.TIMESTAMP, WireUnit.NS)
    status = spec.source(Transport.REST, "/v1/marketstatus/now")
    assert status.result_key is None


def test_every_call_returns_the_same_objects() -> None:
    assert spec.sources(Transport.REST) is spec.sources(Transport.REST)
    key = "/v3/quotes/{stockTicker}"
    assert spec.source(Transport.REST, key) is spec.sources(Transport.REST)[key]


def test_an_unknown_source_names_the_transport_and_key() -> None:
    with pytest.raises(KeyError, match=r"no generated REST source '/v3/reference/dividends'"):
        spec.source(Transport.REST, "/v3/reference/dividends")


@pytest.mark.parametrize("transport", [Transport.WS, Transport.FLAT_FILES])
def test_a_transport_without_a_generated_file(
    unloaded: dict[Transport, object], transport: Transport
) -> None:
    with pytest.raises(LookupError, match=f"no generated schema exists for transport {transport}"):
        spec.sources(transport)
    assert unloaded == {}


def test_sixteen_threads_share_one_first_load(
    unloaded: dict[Transport, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    parses: list[str] = []
    real = spec.parse

    def counting(transport: Transport, text: str) -> object:
        parses.append(transport)
        return real(transport, text)

    monkeypatch.setattr(spec, "parse", counting)
    barrier = threading.Barrier(16)
    results: list[object] = []
    lock = threading.Lock()

    def load() -> None:
        barrier.wait()
        loaded = spec.sources(Transport.REST)
        with lock:
            results.append(loaded)

    threads = [threading.Thread(target=load) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert parses == [Transport.REST]
    assert len(results) == 16
    assert all(result is results[0] for result in results)


def _file(*entries: Mapping[str, object]) -> str:
    return json.dumps({"sources": list(entries)})


_FIELD = {
    "wire_key": "x",
    "column": "X",
    "type": "string",
    "unit": None,
    "required": False,
    "conversion": "text-or-integer-id",
}


def test_parse_reads_memberships_and_other_transports() -> None:
    entry = {
        "key": "stocks/T",
        "fields": [_FIELD],
        "memberships": [
            {"kind": "trades", "group": "stocks", "position": 2, "projection": None},
            {"kind": "values", "group": "indices", "position": 0, "projection": {"x": "Value"}},
        ],
    }
    (value,) = spec.parse(Transport.WS, _file(entry)).values()
    assert type(value) is Source
    assert value.transport is Transport.WS
    assert value.fields[0].conversion is Conversion.TEXT_OR_INTEGER_ID
    assert value.memberships == (
        Membership(kind="trades", group="stocks", position=2),
        Membership(kind="values", group="indices", position=0, projection={"x": "Value"}),
    )


def test_parse_reads_parameter_values() -> None:
    entry = {
        "key": "/v1/x",
        "result_key": "results",
        "fields": [],
        "memberships": [],
        "params": [
            {"name": "order", "kind": "enum", "in_path": False, "values": ["asc"], "unit": None}
        ],
    }
    (value,) = spec.parse(Transport.REST, _file(entry)).values()
    assert isinstance(value, RestPath)
    assert value.params == (Param(name="order", kind=ParamKind.ENUM, values=("asc",)),)


def test_parse_rejects_two_sources_with_one_key() -> None:
    entry = {"key": "stocks/T", "fields": [], "memberships": []}
    with pytest.raises(ValueError, match="share the key 'stocks/T'"):
        spec.parse(Transport.WS, _file(entry, entry))


def test_parse_rejects_a_list_that_is_not_of_objects() -> None:
    with pytest.raises(ValueError, match="expected an array of objects"):
        spec.parse(Transport.WS, _file({"key": "stocks/T", "fields": ["x"], "memberships": []}))


def test_parse_rejects_a_position_that_is_not_an_integer() -> None:
    membership = {"kind": "k", "group": "g", "position": True, "projection": None}
    entry = {"key": "stocks/T", "fields": [], "memberships": [membership]}
    with pytest.raises(TypeError, match="position True is not an integer"):
        spec.parse(Transport.WS, _file(entry))


def test_parse_rejects_values_that_break_the_invariants() -> None:
    field = {**_FIELD, "type": "epoch", "conversion": None}  # an epoch without a unit
    with pytest.raises(ValueError, match="an epoch needs a unit"):
        spec.parse(Transport.WS, _file({"key": "s", "fields": [field], "memberships": []}))


def test_the_schemas_load_without_the_refresh_package() -> None:
    # An installed wheel has no `refresh/`: block it and load the schemas in a fresh process.
    # The script is a literal; `-P` keeps the working directory off `sys.path`.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import sys\n"
                "class Block:\n"
                "    def find_spec(self, name, path=None, target=None):\n"
                "        if name.startswith('deephaven_massive._massive.refresh'):\n"
                "            raise ImportError(name)\n"
                "sys.meta_path.insert(0, Block())\n"
                "from deephaven_massive._massive import spec\n"
                "from deephaven_massive._massive.spec.types import Transport\n"
                "print(len(spec.sources(Transport.REST)))\n"
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert int(result.stdout) == len(spec.sources(Transport.REST))


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"column": None}, "'column' is None, not a string"),
        ({"required": 1}, "'required' is 1, not a boolean"),
        ({"unit": 5}, "'unit' is 5, not a string"),
    ],
)
def test_parse_coerces_nothing_in_a_field(change: dict[str, object], message: str) -> None:
    entry = {"key": "s", "fields": [{**_FIELD, **change}], "memberships": []}
    with pytest.raises(TypeError, match=message):
        spec.parse(Transport.WS, _file(entry))


def test_parse_rejects_parameter_values_that_are_not_strings() -> None:
    param = {"name": "precision", "kind": "enum", "in_path": False, "values": [0], "unit": None}
    entry = {"key": "/v1/x", "result_key": None, "fields": [], "memberships": [], "params": [param]}
    with pytest.raises(TypeError, match="not an array of strings"):
        spec.parse(Transport.REST, _file(entry))


def test_parse_rejects_a_projection_that_is_not_of_strings() -> None:
    membership = {"kind": "k", "group": "g", "position": 0, "projection": {"x": 1}}
    entry = {"key": "stocks/T", "fields": [], "memberships": [membership]}
    with pytest.raises(TypeError, match="not an object of strings"):
        spec.parse(Transport.WS, _file(entry))


@pytest.mark.parametrize(
    ("conversion", "message"),
    [
        ("bool-as-text", "'bool-as-text' is not a valid Conversion"),
        ("quoted-integer", "the conversion quoted-integer gives long, not string"),
    ],
)
def test_parse_rejects_a_conversion_the_decoder_lacks_or_that_does_not_fit(
    conversion: str, message: str
) -> None:
    field = {**_FIELD, "type": "string", "conversion": conversion}
    with pytest.raises(ValueError, match=message):
        spec.parse(Transport.WS, _file({"key": "s", "fields": [field], "memberships": []}))
