from __future__ import annotations

import datetime as dt
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
    Provenance,
    RestPath,
    Source,
    Transport,
    WireType,
    WireUnit,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


_Loaded = dict[Transport, "Mapping[str, Source]"]


@pytest.fixture
def unloaded(monkeypatch: pytest.MonkeyPatch) -> _Loaded:
    """Give the test an empty cache, so the next call loads from the files again.

    Returns:
        The empty mapping of loaded sources the module now uses.
    """
    cache = spec._Cache(sources={})
    monkeypatch.setattr(spec, "_cache", cache)
    return cache.sources


def test_rest_sources_load_from_the_generated_file(unloaded: _Loaded) -> None:
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
        description="The nanosecond accuracy SIP Unix Timestamp. This is the timestamp of when "
        "the SIP received this trade from the exchange which produced it.",
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
def test_a_transport_without_a_generated_file(unloaded: _Loaded, transport: Transport) -> None:
    with pytest.raises(LookupError, match=f"no generated schema exists for transport {transport}"):
        spec.sources(transport)
    assert unloaded == {}
    assert spec._cache.descriptions is None


def test_sixteen_threads_share_one_first_load(
    unloaded: _Loaded, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Half the threads ask for the sources first and half for the provenance first, all at
    # once: each file is still read exactly once, and every thread gets the same objects.
    reads: list[str] = []
    real = spec._read

    def counting(name: str) -> str:
        reads.append(name)  # called only under the module's lock
        return real(name)

    monkeypatch.setattr(spec, "_read", counting)
    barrier = threading.Barrier(16)
    results: list[tuple[object, object]] = []
    lock = threading.Lock()

    def load(sources_first: bool) -> None:
        barrier.wait()
        if sources_first:
            rest = spec.sources(Transport.REST)
            loaded = (rest, spec.provenance())
        else:
            found = spec.provenance()
            loaded = (spec.sources(Transport.REST), found)
        with lock:
            results.append(loaded)

    threads = [threading.Thread(target=load, args=(i % 2 == 0,)) for i in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(reads) == ["descriptions.json", "rest.json"]
    assert len(results) == 16
    assert all(result[0] is results[0][0] for result in results)
    assert all(result[1] is results[0][1] for result in results)


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


# --- Descriptions and provenance -----------------------------------------------------------


def _committed_descriptions() -> dict[str, object]:
    text = resources.files(spec).joinpath("descriptions.json").read_text(encoding="utf-8")
    document: dict[str, object] = json.loads(text)
    return document


def test_every_description_is_joined_into_its_field() -> None:
    texts = _committed_descriptions()["descriptions"]
    assert isinstance(texts, dict)
    described = texts["REST"]
    for key, path in spec.sources(Transport.REST).items():
        for item in path.fields:
            assert item.description == described.get(key, {}).get(item.wire_key, ""), (key, item)
    trades = spec.source(Transport.REST, "/v3/trades/{stockTicker}")
    assert trades.field("Conditions").description == (
        "A list of condition codes. See Stocks Condition Codes "
        "(https://massive.com/docs/rest/stocks/market-operations/condition-codes) for what each "
        "code means."
    )


def test_a_field_the_vendor_does_not_describe_loads_as_empty() -> None:
    futures = spec.source(Transport.REST, "/futures/v1/snapshot")
    assert futures.field("DetailsTicker").description == ""
    assert futures.field("DetailsSettlementDate").description == (
        "The day that this contract is settled."
    )


def test_provenance_gives_the_fetch_date_and_every_source_url() -> None:
    found = spec.provenance()
    assert isinstance(found, Provenance)
    assert found is spec.provenance()
    assert found.fetched.isoformat() == _committed_descriptions()["fetched"]
    assert {key for _, key in found.urls} == set(spec.sources(Transport.REST))
    assert found.urls[Transport.REST, "/v3/trades/{stockTicker}"] == (
        "https://massive.com/docs/rest/stocks/trades-quotes/trades.md"
    )


_PRICE = {**_FIELD, "wire_key": "p", "column": "P", "conversion": None}
_DESCRIBED_ENTRY = {"key": "stocks/T", "fields": [_PRICE], "memberships": []}


def test_parse_joins_descriptions_and_leaves_the_rest_empty() -> None:
    entry = {**_DESCRIBED_ENTRY, "fields": [_PRICE, _FIELD]}
    (value,) = spec.parse(Transport.WS, _file(entry), {"stocks/T": {"p": "The price."}}).values()
    assert [(f.wire_key, f.description) for f in value.fields] == [("p", "The price."), ("x", "")]
    (bare,) = spec.parse(Transport.WS, _file(entry)).values()
    assert [f.description for f in bare.fields] == ["", ""]


@pytest.mark.parametrize(
    ("descriptions", "message"),
    [
        (
            {"stocks/Q": {"p": "The price."}},
            "descriptions name the WS source 'stocks/Q', which is absent",
        ),
        (
            {"stocks/T": {"p": "The price.", "z": "Gone."}},
            r"descriptions of WS 'stocks/T' name no field: \['z'\]",
        ),
    ],
    ids=["unknown source", "unknown wire key"],
)
def test_parse_rejects_a_description_of_something_the_schema_lacks(
    descriptions: Mapping[str, Mapping[str, str]], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        spec.parse(Transport.WS, _file(_DESCRIBED_ENTRY), descriptions)


def _descriptions(**change: object) -> str:
    document = {
        "generated_by": "python -m deephaven_massive._massive.refresh",
        "fetched": "2026-10-27",
        "urls": {"REST": {"/v1/x": "https://massive.com/docs/rest/x.md"}},
        "descriptions": {"REST": {"/v1/x": {"price": "The price."}}},
    }
    return json.dumps({**document, **change})


def test_parse_descriptions_reads_the_generated_format() -> None:
    found = spec.parse_descriptions(_descriptions())
    assert found.fetched == dt.date(2026, 10, 27)
    assert found.urls == {Transport.REST: {"/v1/x": "https://massive.com/docs/rest/x.md"}}
    assert found.texts == {Transport.REST: {"/v1/x": {"price": "The price."}}}
    assert isinstance(found.urls, MappingProxyType)
    assert isinstance(found.urls[Transport.REST], MappingProxyType)
    assert isinstance(found.texts[Transport.REST], MappingProxyType)
    assert isinstance(found.texts[Transport.REST]["/v1/x"], MappingProxyType)


@pytest.mark.parametrize(
    ("text", "error", "message"),
    [
        ("[]", TypeError, "the descriptions file is \\[\\], not an object"),
        (_descriptions(fetched="27 Oct 2026"), ValueError, "Invalid isoformat"),
        (_descriptions(fetched=None), TypeError, "'fetched' is None, not a string"),
        (_descriptions(urls={"SMS": {}}), ValueError, "'SMS' is not a valid Transport"),
        (_descriptions(urls=[]), TypeError, "urls is \\[\\], not an object"),
        (_descriptions(urls={"REST": {"/v1/x": 1}}), TypeError, "not an object of strings"),
        (
            _descriptions(descriptions={"REST": {"/v1/x": {"price": None}}}),
            TypeError,
            "descriptions of '/v1/x' is .* not an object of strings",
        ),
        (
            _descriptions(descriptions={"REST": ["/v1/x"]}),
            TypeError,
            "descriptions of REST is .* not an object",
        ),
    ],
    ids=[
        "not an object",
        "a date that is not ISO",
        "no date",
        "unknown transport",
        "urls not an object",
        "a url not text",
        "a description not text",
        "a transport's descriptions not an object",
    ],
)
def test_parse_descriptions_coerces_nothing(
    text: str, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        spec.parse_descriptions(text)


def _serve(monkeypatch: pytest.MonkeyPatch, **files: str) -> None:
    """Serve the given generated files in place of the committed ones, the rest unchanged."""
    real = spec._read
    monkeypatch.setattr(spec, "_read", lambda name: files.get(name.split(".")[0]) or real(name))


def test_a_descriptions_file_naming_a_transport_without_a_schema_raises(
    unloaded: _Loaded, monkeypatch: pytest.MonkeyPatch
) -> None:
    _serve(monkeypatch, descriptions=_descriptions(urls={"WS": {}}))
    with pytest.raises(ValueError, match=r"names transports without a schema: \['WS'\]"):
        spec.sources(Transport.REST)
    assert unloaded == {}
    assert spec._cache.descriptions is None


def test_a_description_of_an_unknown_source_raises_at_load(
    unloaded: _Loaded, monkeypatch: pytest.MonkeyPatch
) -> None:
    _serve(monkeypatch, descriptions=_descriptions(urls={}))
    with pytest.raises(ValueError, match="descriptions name the REST source '/v1/x'"):
        spec.sources(Transport.REST)
    assert unloaded == {}


def test_a_url_of_an_unknown_source_raises_at_load(
    unloaded: _Loaded, monkeypatch: pytest.MonkeyPatch
) -> None:
    _serve(monkeypatch, descriptions=_descriptions(descriptions={}))
    with pytest.raises(ValueError, match=r"URLs of REST sources the schema lacks: \['/v1/x'\]"):
        spec.provenance()
    assert spec._cache.provenance is None
