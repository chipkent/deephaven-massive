"""Tests for the vendored snapshot: its location, storing with a manifest, and verified loading.

Synthetic snapshots are written under `tmp_path`; the tests of the real snapshot skip where
there is none (an sdist leaves it out). The last of them check the real snapshot for the
documentation problems Massive has confirmed and not yet fixed, each an expected failure until
a refresh brings the fix.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path

import pytest

from deephaven_massive._massive.refresh import docs, knowledge_base, snapshot
from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.openapi import JsonObject, OpenApi, properties, text
from deephaven_massive._massive.refresh.snapshot import Snapshot, VendorFile
from tests._live.massive_problems import expect_fixed


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --- The location ---------------------------------------------------------------------------


def test_the_snapshot_lives_outside_the_package() -> None:
    # `vendor/massive/` at the top of the checkout, beside `src/`: no distribution reaches it.
    root = Path(__file__).resolve().parents[3]  # this file is tests/massive/refresh/...
    assert snapshot.DIRECTORY.resolve() == root / "vendor" / "massive"


@pytest.mark.skipif(
    not snapshot.DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)
def test_the_real_snapshot_matches_its_manifest() -> None:
    manifest = json.loads((snapshot.DIRECTORY / "manifest.json").read_text("utf-8"))
    loaded = Snapshot.load()
    assert loaded.fetched.isoformat() == manifest["fetched"]
    assert loaded.index_url == manifest["files"]["llms.txt"]["url"]
    assert loaded.origin == "https://massive.com"
    assert loaded.websocket["openapi"]  # the WebSocket specification was read
    for area, pages, urls in (
        ("rest", loaded.pages, loaded.page_urls),
        ("websocket", loaded.websocket_pages, loaded.websocket_page_urls),
        ("flat-files", loaded.flat_file_pages, loaded.flat_file_page_urls),
    ):
        assert pages
        assert urls.keys() == pages.keys()
        for name, url in urls.items():
            assert manifest["files"][f"{area}/{name}"]["url"] == url
    assert loaded.articles.keys() == knowledge_base.ARTICLES.keys()
    for slug, url in loaded.article_urls.items():
        assert url == knowledge_base.article_url(slug)
        assert manifest["files"][f"knowledge-base/{slug}.md"]["url"] == url


def test_a_missing_snapshot_says_to_run_from_a_source_checkout(tmp_path: Path) -> None:
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "absent")
    (problem,) = caught.value.problems
    assert (problem.gate, problem.key) == ("snapshot", str(tmp_path / "absent"))
    assert "source checkout" in problem.message


# --- Storing --------------------------------------------------------------------------------


def _file(name: str, stored: bytes, raw: bytes | None = None) -> VendorFile:
    return VendorFile(
        name=name, url=f"https://example.com/{name}", raw=raw or stored, stored=stored
    )


def test_store_writes_every_file_and_a_manifest_of_both_hashes(tmp_path: Path) -> None:
    target = tmp_path / "massive"
    files = [_file("openapi.json", b"{}\n", raw=b"{}"), _file("rest/a/b.md", b"page\n")]
    names = snapshot.store(target, files, fetched=dt.date(2026, 10, 26))
    assert names == ["openapi.json", "rest/a/b.md", "manifest.json"]
    assert (target / "rest" / "a" / "b.md").read_bytes() == b"page\n"
    text = (target / "manifest.json").read_text(encoding="utf-8")
    assert text.endswith("}\n")
    assert json.loads(text) == {
        "fetched": "2026-10-26",
        "files": {
            "openapi.json": {
                "url": "https://example.com/openapi.json",
                "fetched_sha256": _sha(b"{}"),
                "stored_sha256": _sha(b"{}\n"),
            },
            "rest/a/b.md": {
                "url": "https://example.com/rest/a/b.md",
                "fetched_sha256": _sha(b"page\n"),
                "stored_sha256": _sha(b"page\n"),
            },
        },
    }


def test_store_replaces_the_old_snapshot_entirely(tmp_path: Path) -> None:
    target = tmp_path / "massive"
    (target / "rest" / "gone").mkdir(parents=True)
    (target / "rest" / "gone" / "page.md").write_text("removed by Massive\n", encoding="utf-8")
    (tmp_path / "massive-incoming").mkdir()  # a staging directory left by an interrupted run
    (tmp_path / "massive-previous").mkdir()  # a backup left by an interrupted swap
    snapshot.store(target, [_file("openapi.json", b"{}\n")], fetched=dt.date(2026, 1, 1))
    assert sorted(p.name for p in target.iterdir()) == ["manifest.json", "openapi.json"]
    assert not (tmp_path / "massive-incoming").exists()
    assert not (tmp_path / "massive-previous").exists()


def test_store_restores_a_snapshot_an_interrupted_swap_left_aside(tmp_path: Path) -> None:
    # A run stopped between moving the old snapshot aside and moving the new one in: the
    # backup is the only copy, and a failing next run must not delete it.
    target = tmp_path / "massive"
    (tmp_path / "massive-previous").mkdir()
    (tmp_path / "massive-previous" / "openapi.json").write_text("old\n", encoding="utf-8")
    clash = [_file("x", b"1"), _file("x/y", b"2")]  # a file where a directory must go
    with pytest.raises(FileExistsError):
        snapshot.store(target, clash, fetched=dt.date(2026, 1, 1))
    assert (target / "openapi.json").read_text(encoding="utf-8") == "old\n"


@pytest.mark.parametrize(
    ("url", "area", "stored"),
    [
        (
            "https://massive.com/docs/rest/stocks/trades-quotes/trades.md",
            docs.REST,
            "rest/stocks/trades-quotes/trades.md",
        ),
        (
            "https://massive.com/docs/websocket/stocks/trades.md",
            docs.WEBSOCKET,
            "websocket/stocks/trades.md",
        ),
        (
            "https://massive.com/docs/flat-files/futures/trades/cme.md",
            docs.FLAT_FILES,
            "flat-files/futures/trades/cme.md",
        ),
    ],
)
def test_each_area_s_pages_are_stored_below_its_directory(
    url: str, area: docs.Area, stored: str
) -> None:
    assert snapshot.page_file(url, area) == stored


def test_an_article_is_stored_by_its_slug_below_the_knowledge_base_directory() -> None:
    assert snapshot.article_file("a-question") == "knowledge-base/a-question.md"


# --- Loading --------------------------------------------------------------------------------


_EXTRA_PAGES = {
    "websocket/stocks/trades.md": "feed",
    "flat-files/stocks/trades.md": "columns",
    "knowledge-base/a-question.md": "# A question?\n",
}


def _write_snapshot(directory: Path, *, llms: str, pages: dict[str, str]) -> None:
    """Write a snapshot whose manifest lists every file with its true hash.

    `pages` are REST pages; the WebSocket and flat-file pages of `_INDEX` are always written.
    """
    files = [
        _file("openapi.json", json.dumps({"paths": {}}).encode()),
        _file("websocket.json", json.dumps({"paths": {"/stocks/T": {}}}).encode()),
        _file("llms.txt", llms.encode()),
    ]
    files += [_file(f"rest/{name}", text.encode()) for name, text in pages.items()]
    files += [_file(name, text.encode()) for name, text in _EXTRA_PAGES.items()]
    snapshot.store(directory, files, fetched=dt.date(2026, 1, 1))


_INDEX = (
    "## REST API\n- [Trades](https://massive.com/docs/rest/stocks/trades.md): Trades\n"
    "## WebSocket\n- [Trades](https://massive.com/docs/websocket/stocks/trades.md): Trades\n"
    "## Flat Files\n- [Trades](https://massive.com/docs/flat-files/stocks/trades.md): Trades\n"
)


def test_a_stored_snapshot_loads(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    loaded = Snapshot.load(tmp_path / "s")
    assert loaded.pages == {"stocks/trades.md": "page"}
    assert loaded.openapi == {"paths": {}}
    assert loaded.llms == _INDEX
    assert loaded.fetched == dt.date(2026, 1, 1)
    assert loaded.index_url == "https://example.com/llms.txt"
    assert loaded.origin == "https://example.com"
    assert loaded.page_urls == {"stocks/trades.md": "https://example.com/rest/stocks/trades.md"}
    assert loaded.websocket == {"paths": {"/stocks/T": {}}}
    assert loaded.websocket_pages == {"stocks/trades.md": "feed"}
    assert loaded.websocket_page_urls == {
        "stocks/trades.md": "https://example.com/websocket/stocks/trades.md"
    }
    assert loaded.flat_file_pages == {"stocks/trades.md": "columns"}
    assert loaded.flat_file_page_urls == {
        "stocks/trades.md": "https://example.com/flat-files/stocks/trades.md"
    }
    # The articles are whichever the manifest lists, by slug.
    assert loaded.articles == {"a-question": "# A question?\n"}
    assert loaded.article_urls == {"a-question": "https://example.com/knowledge-base/a-question.md"}


def _edit_manifest(directory: Path, change: dict[str, object]) -> None:
    """Merge `change` into a stored snapshot's manifest."""
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    (directory / "manifest.json").write_text(json.dumps({**manifest, **change}), encoding="utf-8")


@pytest.mark.parametrize("fetched", ["27 October 2026", None])
def test_gate_snapshot_a_manifest_without_a_fetch_date(tmp_path: Path, fetched: object) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    _edit_manifest(tmp_path / "s", {"fetched": fetched})
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key, p.message) for p in caught.value.problems] == [
        ("snapshot", "manifest.json", f"the fetch date {fetched!r} is not an ISO date")
    ]


def test_gate_snapshot_a_manifest_entry_without_a_url(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    manifest = json.loads((tmp_path / "s" / "manifest.json").read_text(encoding="utf-8"))
    del manifest["files"]["llms.txt"]["url"]
    _edit_manifest(tmp_path / "s", {"files": manifest["files"]})
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key, p.message) for p in caught.value.problems] == [
        ("snapshot", "llms.txt", "the manifest records no URL")
    ]


def test_gate_snapshot_an_edited_vendor_file(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    page = tmp_path / "s" / "rest" / "stocks" / "trades.md"
    page.write_text("pagf", encoding="utf-8")  # one byte changed
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key) for p in caught.value.problems] == [
        ("snapshot", "rest/stocks/trades.md")
    ]


def test_gate_snapshot_files_must_match_the_manifest_exactly(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    (tmp_path / "s" / "stray.md").write_text("added by hand", encoding="utf-8")
    (tmp_path / "s" / "rest" / "stocks" / "trades.md").rename(tmp_path / "trades.md")
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key, p.message) for p in caught.value.problems if p.gate == "snapshot"] == [
        ("snapshot", "rest/stocks/trades.md", "listed but missing"),
        ("snapshot", "stray.md", "not in the manifest"),
    ]


def test_file_browser_litter_is_ignored_but_any_other_stray_file_is_not(tmp_path: Path) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    pages = tmp_path / "s" / "rest" / "stocks"
    for litter in (tmp_path / "s" / ".DS_Store", pages / "._trades.md", pages / "Thumbs.db"):
        litter.write_bytes(b"\x00")
    (tmp_path / "s" / "desktop.ini").write_text("[.ShellClassInfo]", encoding="utf-8")
    assert Snapshot.load(tmp_path / "s").pages == {"stocks/trades.md": "page"}
    (pages / "notes.txt").write_text("a note", encoding="utf-8")
    (tmp_path / "s" / ".hidden").write_text("a dot-file", encoding="utf-8")
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert sorted((p.gate, p.key, p.message) for p in caught.value.problems) == [
        ("snapshot", ".hidden", "not in the manifest"),
        ("snapshot", "rest/stocks/notes.txt", "not in the manifest"),
    ]


def test_gate_docs_an_index_without_an_area_s_links(tmp_path: Path) -> None:
    llms = _INDEX.replace("## Flat Files", "## Elsewhere")
    _write_snapshot(tmp_path / "s", llms=llms, pages={"stocks/trades.md": "page"})
    (tmp_path / "s" / "flat-files" / "stocks" / "trades.md").unlink()
    manifest = json.loads((tmp_path / "s" / "manifest.json").read_text(encoding="utf-8"))
    del manifest["files"]["flat-files/stocks/trades.md"]
    _edit_manifest(tmp_path / "s", {"files": manifest["files"]})
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key, p.message) for p in caught.value.problems] == [
        ("docs", "llms.txt", "the documentation index has no 'Flat Files' links")
    ]


def test_gate_docs_a_linked_page_missing_from_the_snapshot(tmp_path: Path) -> None:
    llms = _INDEX + "- [Quotes](https://massive.com/docs/flat-files/stocks/quotes.md): Quotes\n"
    _write_snapshot(tmp_path / "s", llms=llms, pages={})
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [(p.gate, p.key) for p in caught.value.problems] == [
        ("docs", "stocks/quotes.md"),
        ("docs", "stocks/trades.md"),
    ]


@pytest.mark.parametrize("name", ["openapi.json", "websocket.json"])
def test_a_snapshot_that_is_not_json_objects_is_reported(tmp_path: Path, name: str) -> None:
    _write_snapshot(tmp_path / "s", llms=_INDEX, pages={"stocks/trades.md": "page"})
    (tmp_path / "s" / name).write_text("[]", encoding="utf-8")
    with pytest.raises(RefreshError) as caught:
        Snapshot.load(tmp_path / "s")
    assert [p.gate for p in caught.value.problems] == ["snapshot"]


# --- Massive's documentation problems -------------------------------------------------------
#
# Each check reads the vendored snapshot and asserts what Massive's corrected documentation
# says, and its `massive_issue` marker names the GitHub issue that tracks the problem. It is an
# expected failure while the vendored text keeps the problem; once a refresh brings the fix,
# it passes, and the strict expected failure stops CI with a message naming the issue to close.

_SNAPSHOT_HERE = pytest.mark.skipif(
    not snapshot.DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)

_STOCK_PRICE_EXAMPLE = "with a price of $2.00 would be worth a total dollar value of $200.00"
"""The end of Massive's stock trade price text, which counts shares or contracts at $2.00."""


@pytest.fixture(scope="module")
def vendored() -> Snapshot:
    """Return the vendored snapshot, read once for the module's checks."""
    return Snapshot.load()


def _documents(vendored: Snapshot, *asset_classes: str) -> dict[str, str]:
    """Return the text of every vendored page and specification path of the asset classes.

    Args:
        vendored: The vendored snapshot.
        asset_classes: Such as `"options"`: the first part of a page's name below `rest/`,
            `websocket/` or `flat-files/`, and a word of a REST path or a WebSocket channel.

    Returns:
        Each document's text, by its name: a page as `rest/options/...md`, a REST path as
        `openapi.json <path>`, a channel as `websocket.json <channel>`. The unified snapshot
        (`/v3/snapshot`), which serves every asset class, is left out.
    """
    found: dict[str, str] = {}
    pages = {"rest": vendored.pages, "websocket": vendored.websocket_pages}
    pages["flat-files"] = vendored.flat_file_pages
    for area, texts in pages.items():
        for name, page in texts.items():
            if name.split("/", 1)[0] in asset_classes:
                found[f"{area}/{name}"] = page
    specifications = {"openapi.json": vendored.openapi, "websocket.json": vendored.websocket}
    for file, document in specifications.items():
        paths = document["paths"]
        assert isinstance(paths, dict)
        for path, item in paths.items():
            words = path.lower().replace("{", "/").replace("}", "/").split("/")
            if any(word.startswith(asset_classes) for word in words if word):
                found[f"{file} {path}"] = json.dumps(item)
    return found


def _saying(documents: dict[str, str], text: str) -> list[str]:
    """Return the names of the documents that hold the text, sorted."""
    return sorted(name for name, document in documents.items() if text in document)


def _channel_fields(vendored: Snapshot, channel: str) -> dict[str, JsonObject]:
    """Return the fields of a WebSocket channel's message, each with its resolved schema."""
    spec = OpenApi(vendored.websocket)
    paths = vendored.websocket["paths"]
    assert isinstance(paths, dict)
    operation = spec.deref(spec.deref(paths[channel], channel).get("get"), channel)
    ok = spec.deref(spec.deref(operation.get("responses"), channel).get("200"), channel)
    media = spec.deref(spec.deref(ok.get("content"), channel).get("application/json"), channel)
    found = properties(spec.schema(media.get("schema"), channel))
    assert found, f"{channel}: no message fields"
    return {key: spec.schema(value, f"{channel} {key}") for key, value in found.items()}


@_SNAPSHOT_HERE
def test_documents_of_an_asset_class_are_its_pages_and_paths(vendored: Snapshot) -> None:
    documents = _documents(vendored, "options")
    assert "rest/options/unusual-options-activity/unusual-trades.md" in documents
    assert "openapi.json /options/v1/unusual-trades" in documents
    assert "openapi.json /v3/trades/{optionsTicker}" in documents
    assert "websocket.json /options/T" in documents
    assert "flat-files/options/trades.md" in documents
    assert "openapi.json /v3/snapshot" not in documents
    assert not any("stocks" in name for name in documents)


@_SNAPSHOT_HERE
@pytest.mark.massive_issue(779)
def test_massive_documents_the_day_its_index_bars_cover(vendored: Snapshot) -> None:
    """Whether Massive's index custom-bars text no longer puts its bars in Eastern Time.

    Massive's index day bars and minute files cover the Chicago day, though its custom-bars
    text says the aggregates are in Eastern Time. A fix of either the text or the data
    settles the item; the data's side is the live check of the same item.
    """
    shown = _saying(_documents(vendored, "indices"), "time interval in Eastern Time (ET)")
    expect_fixed(fixed=not shown, shown=f"'in Eastern Time (ET)' in {shown}")


@_SNAPSHOT_HERE
@pytest.mark.massive_issue(791)
def test_massive_describes_an_option_trade_price_per_share(vendored: Snapshot) -> None:
    """Whether no options text calls a trade's price the value per whole contract.

    The unusual-trades and options trades `price` is per share of the underlying, as
    Massive confirmed, though its description says "per whole contract".
    """
    shown = _saying(_documents(vendored, "options"), "per whole contract")
    expect_fixed(fixed=not shown, shown=f"'per whole contract' in {shown}")


@_SNAPSHOT_HERE
@pytest.mark.massive_issue(793)
def test_massive_types_futures_stream_prices_as_it_sends_them(vendored: Snapshot) -> None:
    """Whether the specification types the futures stream's prices as the text Massive sends.

    The futures stream sends its prices and dollar volume as JSON strings while the
    specification types them as numbers. Either side's fix settles the item; the stream's
    side is the live check of the same item.
    """
    priced = {
        "/futures/T": ("p",),
        "/futures/Q": ("bp", "ap"),
        "/futures/A": ("o", "h", "l", "c", "dv"),
        "/futures/AM": ("o", "h", "l", "c", "dv"),
    }
    typed = {
        f"{channel} {key}": text(_channel_fields(vendored, channel)[key], "type")
        for channel, keys in priced.items()
        for key in keys
    }
    numbers = sorted(name for name, kind in typed.items() if kind != "string")
    expect_fixed(fixed=not numbers, shown=f"typed other than string: {numbers}")


@_SNAPSHOT_HERE
@pytest.mark.massive_issue(794)
def test_massive_describes_trade_prices_without_the_stock_example(vendored: Snapshot) -> None:
    """Whether no options, crypto, or futures text prices a trade with the stock example.

    The example counts 100 shares (or contracts) at $2.00 as $200.00, which leaves out an
    option's and a futures contract's multiplier and gives a crypto pair shares.
    """
    documents = _documents(vendored, "options", "crypto", "futures")
    shown = _saying(documents, _STOCK_PRICE_EXAMPLE)
    expect_fixed(fixed=not shown, shown=f"the stock price example in {shown}")


@_SNAPSHOT_HERE
@pytest.mark.massive_issue(794)
def test_massive_describes_futures_dollar_volume_without_us_dollars(vendored: Snapshot) -> None:
    """Whether no futures text calls a bar's dollar volume a value in US dollars.

    Futures dollar volume is price times size with no contract multiplier, as Massive's REST
    futures bars say, while its stream and flat-file texts call it a US dollar value.
    """
    documents = _documents(vendored, "futures")
    shown = [
        *_saying(documents, "The total US dollar value of shares traded"),
        *_saying(documents, "The total dollar volume of the transactions"),
    ]
    expect_fixed(fixed=not shown, shown=f"a US dollar volume text in {shown}")


@_SNAPSHOT_HERE
@pytest.mark.massive_issue(802)
def test_massive_documents_the_forex_quote_s_i(vendored: Snapshot) -> None:
    """Whether the specification declares the `i` that forex quotes (`C`) carry."""
    declared = sorted(_channel_fields(vendored, "/forex/C"))
    expect_fixed(fixed="i" in declared, shown=f"/forex/C declares only {declared}")


@_SNAPSHOT_HERE
@pytest.mark.massive_issue(802)
def test_massive_documents_the_crypto_quote_s_lp_and_ls(vendored: Snapshot) -> None:
    """Whether the specification declares the `lp` and `ls` that crypto quotes (`XQ`) carry."""
    declared = sorted(_channel_fields(vendored, "/crypto/XQ"))
    fixed = {"lp", "ls"} <= set(declared)
    expect_fixed(fixed=fixed, shown=f"/crypto/XQ declares only {declared}")
