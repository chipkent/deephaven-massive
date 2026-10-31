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


@pytest.mark.parametrize(
    ("transport", "name"), [(Transport.WS, "ws.json"), (Transport.FLAT_FILES, "flatfiles.json")]
)
def test_every_transport_loads_its_generated_file(
    unloaded: _Loaded, transport: Transport, name: str
) -> None:
    loaded = spec.sources(transport)
    text = resources.files(spec).joinpath(name).read_text(encoding="utf-8")
    assert list(loaded) == [entry["key"] for entry in json.loads(text)["sources"]]
    assert all(type(value) is Source and value.transport is transport for value in loaded.values())
    assert unloaded[transport] is loaded


def test_channels_and_prefixes_carry_their_memberships_and_descriptions() -> None:
    trades = spec.source(Transport.WS, "stocks/T")
    assert trades.memberships == (Membership(kind="trades", group="stocks", position=2),)
    assert trades.field("SipTimestamp").unit is WireUnit.MS
    assert trades.field("Price").description == "The price."
    futures = spec.source(Transport.FLAT_FILES, "us_futures_{venue}/trades_v1")
    assert {"ticker", "timestamp"} <= {f.wire_key for f in futures.fields}
    assert futures.field("Timestamp").unit is WireUnit.NS
    (quotes,) = spec.source(Transport.REST, "/v1/last_quote/currencies/{from}/{to}").memberships
    assert quotes.projection == {
        "ask": "AskPrice",
        "bid": "BidPrice",
        "exchange": "Exchange",
        "timestamp": "Timestamp",
    }
    (values,) = spec.source(Transport.FLAT_FILES, "us_indices/values_v1").memberships
    assert values == Membership(kind="values", group="indices", position=1)


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
    # The provenance loads every transport the descriptions file gives URLs for.
    assert sorted(reads) == ["descriptions.json", "flatfiles.json", "rest.json", "ws.json"]
    assert len(results) == 16
    assert all(result[0] is results[0][0] for result in results)
    assert all(result[1] is results[0][1] for result in results)


_FIELD = {
    "wire_key": "x",
    "column": "X",
    "type": "string",
    "unit": None,
    "required": False,
    "conversion": "text-or-integer-id",
}


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
                "for transport in Transport:\n"
                "    found = spec.sources(transport).values()\n"
                "    print(len(found), sum(len(s.memberships) for s in found))\n"
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    expected = [
        f"{len(found)} {sum(len(s.memberships) for s in found)}"
        for found in (spec.sources(transport).values() for transport in Transport)
    ]
    assert result.stdout.splitlines() == expected
    # Every channel and prefix belongs to a kind, so memberships are read where they matter.
    assert all(count.split()[1] != "0" for count in expected)


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
    by_class = _committed_descriptions()["class_descriptions"]
    assert isinstance(by_class, dict)
    fields = {f.wire_key: f for f in spec.source(Transport.REST, "/v3/snapshot").fields}
    for wire_key, texts in by_class["REST"]["/v3/snapshot"].items():
        assert dict(fields[wire_key].class_descriptions) == texts
    trades = spec.source(Transport.REST, "/v3/trades/{stockTicker}")
    assert trades.field("Conditions").description == (
        "A list of condition codes. See Stocks Condition Codes "
        "(https://massive.com/docs/rest/stocks/market-operations/condition-codes) for what each "
        "code means."
    )


def test_a_field_the_vendor_does_not_describe_loads_as_empty() -> None:
    futures = spec.source(Transport.REST, "/futures/v1/snapshot")
    assert futures.field("Ticker").description == ""
    assert futures.field("DetailsSettlementDate").description == (
        "The day that this contract is settled."
    )


def test_provenance_gives_the_fetch_date_and_every_source_url() -> None:
    found = spec.provenance()
    assert isinstance(found, Provenance)
    assert found is spec.provenance()
    assert found.fetched.isoformat() == _committed_descriptions()["fetched"]
    assert set(found.urls) == {
        (transport, key) for transport in Transport for key in spec.sources(transport)
    }
    assert found.urls[Transport.REST, "/v3/trades/{stockTicker}"] == (
        "https://massive.com/docs/rest/stocks/trades-quotes/trades.md",
    )
    # A path several asset classes share cites every page that documents it.
    assert found.urls[Transport.REST, "/v3/reference/tickers"] == tuple(
        f"https://massive.com/docs/rest/{asset}/tickers/all-tickers.md"
        for asset in ("crypto", "forex", "indices", "stocks")
    )


_URL = "https://massive.com/docs/rest/x.md"


def _descriptions(**change: object) -> str:
    document = {
        "generated_by": "python -m deephaven_massive._massive.refresh",
        "fetched": "2026-10-27",
        "urls": {"REST": {"/v1/x": [_URL]}},
        "descriptions": {"REST": {"/v1/x": {"price": "The price."}}},
        "class_descriptions": {"REST": {"/v1/x": {"price": {"options": "Per share."}}}},
        "plans": {
            _URL: {
                "title": "X",
                "access": "Included in all Stocks plans",
                "recency": "Real-time",
                "history": "",
                "plans": [
                    {"plan": "Stocks Basic", "access": "Included", "recency": None, "history": None}
                ],
            }
        },
    }
    return json.dumps({**document, **change})


# The loader against generated files of the test's own, so each mismatch shows alone.

_SCHEMA = json.dumps(
    {
        "sources": [
            {
                "key": "/v1/x",
                "result_key": "results",
                "params": [],
                "fields": [{**_FIELD, "wire_key": "price", "column": "Price"}],
                "memberships": [],
            }
        ]
    }
)


def _serve(monkeypatch: pytest.MonkeyPatch, files: Mapping[str, str]) -> None:
    """Make the loader read `files`, by file name, instead of the committed generated files.

    Args:
        monkeypatch: The test's monkeypatch fixture.
        files: The text of each generated file, such as `{"rest.json": ...}`; a file not
            named is read from the package as usual.
    """
    real = spec._read
    monkeypatch.setattr(spec, "_read", lambda name: files[name] if name in files else real(name))


def test_the_loader_joins_and_checks_files_of_its_own(
    unloaded: _Loaded, monkeypatch: pytest.MonkeyPatch
) -> None:
    _serve(monkeypatch, {"rest.json": _SCHEMA, "descriptions.json": _descriptions()})
    assert spec.source(Transport.REST, "/v1/x").field("Price").description == "The price."
    found = spec.provenance()
    assert found == Provenance(
        fetched=dt.date(2026, 10, 27),
        urls={(Transport.REST, "/v1/x"): (_URL,)},
        plans={_URL: found.plans[_URL]},
    )
    assert found.plans[_URL].plans[0].plan == "Stocks Basic"


@pytest.mark.parametrize(
    ("descriptions", "message"),
    [
        pytest.param(
            _descriptions(descriptions={"REST": {"/v1/y": {"price": "P."}}}),
            "descriptions name the REST source '/v1/y', which is absent",
            id="a description of an unknown source",
        ),
        pytest.param(
            _descriptions(urls={"REST": {}}),
            r"has REST sources without a URL: \['/v1/x'\]",
            id="a source without a URL",
        ),
        pytest.param(
            _descriptions(urls={"REST": {"/v1/x": [_URL], "/v1/y": [_URL]}}),
            r"has REST URLs of sources the schema lacks: \['/v1/y'\]",
            id="a URL of an unknown source",
        ),
    ],
)
@pytest.mark.parametrize("first", ["sources", "provenance"])
def test_a_mismatched_file_raises_at_load_whichever_call_comes_first(
    unloaded: _Loaded,
    monkeypatch: pytest.MonkeyPatch,
    *,
    descriptions: str,
    message: str,
    first: str,
) -> None:
    # `sources` and `provenance` reject the same files, and the transport stays unloaded so
    # that the next call raises again rather than returning what failed.
    _serve(monkeypatch, {"rest.json": _SCHEMA, "descriptions.json": descriptions})
    call = spec.provenance if first == "provenance" else lambda: spec.sources(Transport.REST)
    for _ in range(2):
        with pytest.raises(ValueError, match=message):
            call()
    assert unloaded == {}
    assert spec._cache.provenance is None
