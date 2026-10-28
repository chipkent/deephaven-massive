"""Tests for the column reference: parity, docstring links, `render`, and `sync`.

The parity test renders the pages from the process's catalog and the committed provenance, in
the test JVM, and fails on any page under `docs/datasets/` that is stale, missing, or extra,
naming the command that fixes it. The docstring rule checks that every dataset method's
docstring links its own section. Both are functions over their inputs, so the tests also run
them on a copy of the directory and on synthetic namespaces that break them.
"""

from __future__ import annotations

import datetime as dt
import os
import re
import shutil
import typing
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import deephaven_massive as dm
from deephaven_massive import _massive
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.column_reference import (
    _TRANSPORT_ORDER,
    COMMAND,
    SyncResult,
    anchor,
    call_path,
    render,
    sync,
)
from deephaven_massive._enums import AssetClass
from deephaven_massive._massive import Field, Provenance, Source, Transport, WireType, source
from deephaven_massive._namespaces.base import Namespace
from deephaven_massive._session import CATALOG

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven_massive._query import Query

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES
DOCS = Path(__file__).resolve().parents[2] / "docs" / "datasets"
SITE = "https://github.com/chipkent/deephaven-massive/blob/main/"
# The wire interface names `Source.memberships` but exports no constructor for an element.
MEMBERSHIP = type(source(R, "/v3/snapshot/indices").memberships[0])


# --- The parity of docs/datasets ---------------------------------------------------------------


def parity_message(directory: Path, pages: Mapping[str, str]) -> str | None:
    """Say how `directory` differs from the rendered pages, or return None when it does not.

    Args:
        directory: The directory of the committed pages.
        pages: The rendered pages by file name.

    Returns:
        One message naming every stale, missing, and extra entry and the command that fixes
        them; None when the directory holds exactly the pages, byte for byte.
    """
    if not directory.is_dir() or directory.is_symlink():
        return f"docs/datasets/ is not a directory; run `{COMMAND}`"
    present = {entry.name: entry for entry in directory.iterdir()}
    problems = [f"extra: {name}" for name in sorted(present.keys() - pages.keys())]
    for name, text in pages.items():
        entry = present.get(name)
        if entry is None:
            problems.append(f"missing: {name}")
        elif not entry.is_file() or entry.is_symlink() or entry.read_bytes() != text.encode():
            problems.append(f"stale: {name}")
    if not problems:
        return None
    return f"docs/datasets/ is out of date ({'; '.join(problems)}); run `{COMMAND}`"


def rendered() -> dict[str, str]:
    return render(CATALOG, _massive.provenance())


def test_the_committed_pages_are_the_rendered_pages() -> None:
    message = parity_message(DOCS, rendered())
    assert message is None, message


def test_every_namespace_with_datasets_has_a_page() -> None:
    assert list(rendered()) == [
        "stocks.md",
        "options.md",
        "futures.md",
        "indices.md",
        "forex.md",
        "crypto.md",
        "reference.md",
        "indicators.md",
        "economy.md",
        "partners.md",
    ]


def test_the_session_method_s_section_says_why_it_is_on_the_reference_page() -> None:
    page = rendered()["reference.md"]
    section = page.partition('<a id="snapshots"></a>\n\n## `m.snapshots`\n\n')[2]
    note = (
        "Called as `m.snapshots`, a method of the session itself rather than of "
        "`m.reference`; its catalog record is `reference.snapshots`, so its columns are "
        "documented on this page.\n\n| Column |"
    )
    assert section.split("\n\n", 1)[1].startswith(note)
    # Only the session method has the note.
    assert page.count("a method of the session itself") == 1


def test_the_wire_columns_follow_the_order_of_every_supplied_by() -> None:
    # The page orders the wire-field columns by transport; every column of the real catalog
    # lists its transports in that same order.
    rank = {transport: index for index, transport in enumerate(_TRANSPORT_ORDER)}
    for spec in CATALOG.by_name.values():
        for column in CATALOG.dataset_schema(spec).columns:
            ranks = [rank[t] for t in column.supplied_by]
            assert ranks == sorted(ranks), (spec.name, column.name)


def _plant_stray(directory: Path) -> None:
    (directory / "stray.md").write_text("# Stray\n")


def _plant_directory(directory: Path) -> None:
    (directory / "old").mkdir()
    (directory / "old" / "trades.md").write_text("# Old\n")


def _delete_page(directory: Path) -> None:
    (directory / "stocks.md").unlink()


def _edit_page(directory: Path) -> None:
    page = directory / "crypto.md"
    page.write_text(page.read_text() + "An edit.\n")


@pytest.mark.parametrize(
    ("damage", "found"),
    [
        (_plant_stray, "extra: stray.md"),
        (_plant_directory, "extra: old"),
        (_delete_page, "missing: stocks.md"),
        (_edit_page, "stale: crypto.md"),
    ],
)
def test_the_parity_test_fails_on_damage_that_the_command_repairs(
    tmp_path: Path, damage: Callable[[Path], None], found: str
) -> None:
    copy = tmp_path / "datasets"
    shutil.copytree(DOCS, copy)
    pages = rendered()
    assert parity_message(copy, pages) is None
    damage(copy)
    message = parity_message(copy, pages)
    assert message is not None
    assert found in message
    assert f"run `{COMMAND}`" in message
    sync(copy, pages)
    assert parity_message(copy, pages) is None


def test_the_parity_test_fails_on_a_linked_page_or_a_missing_directory(tmp_path: Path) -> None:
    copy = tmp_path / "datasets"
    shutil.copytree(DOCS, copy)
    pages = rendered()
    # A link to the right bytes is still no committed page: sync would replace it.
    (tmp_path / "stocks.md").write_bytes((copy / "stocks.md").read_bytes())
    (copy / "stocks.md").unlink()
    (copy / "stocks.md").symlink_to(tmp_path / "stocks.md")
    assert "stale: stocks.md" in str(parity_message(copy, pages))
    gone = f"docs/datasets/ is not a directory; run `{COMMAND}`"
    assert parity_message(tmp_path / "absent", pages) == gone
    assert parity_message(tmp_path / "stocks.md", pages) == gone


# --- The docstring rule: every dataset method links its own section ----------------------------


def factory(session: type, path: str) -> Callable[..., object] | None:
    """Return the method a dataset's call path names, walking the namespace properties.

    Args:
        session: The session class, the `m` of the path.
        path: The call path, such as `"m.stocks.financials.ratios"` or `"m.snapshots"`.

    Returns:
        The method, or None when some part of the path names nothing.
    """
    owner: type = session
    *namespaces, method = path.split(".")[1:]
    for name in namespaces:
        member = vars(owner).get(name)
        if not isinstance(member, property) or member.fget is None:
            return None
        owner = typing.get_type_hints(member.fget)["return"]
    found = vars(owner).get(method)
    return found if callable(found) else None


def link_problems(session: type, specs: Iterable[DatasetSpec]) -> list[str]:
    """Return every dataset whose method's docstring does not link its own section.

    Args:
        session: The session class whose namespaces hold the dataset methods.
        specs: The datasets.

    Returns:
        One message per dataset whose call path names no method, or whose method's docstring
        lacks the full GitHub URL of `docs/datasets/<namespace>.md#<anchor>`, ending at the
        anchor (a longer anchor that merely starts with it does not count).
    """
    problems = []
    for spec in specs:
        method = factory(session, call_path(spec))
        link = f"docs/datasets/{spec.name.split('.')[0]}.md#{anchor(spec)}"
        if method is None:
            problems.append(f"{spec.name}: no method at {call_path(spec)}")
        elif not re.search(re.escape(SITE + link) + r"(?![\w-])", method.__doc__ or ""):
            problems.append(f"{spec.name}: the docstring must link {SITE}{link}")
    return problems


def test_every_dataset_docstring_links_its_section() -> None:
    assert link_problems(dm.MassiveSession, CATALOG.by_name.values()) == []


class _Linked(Namespace):
    __slots__ = ()

    def dividends(self) -> Query:
        """Cash dividends.

        Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#dividends.
        """
        raise NotImplementedError

    def splits(self) -> Query:
        """Splits; the link names another dataset.

        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#dividends
        """
        raise NotImplementedError

    def ipos(self) -> Query:
        raise NotImplementedError

    def financials(self) -> Query:
        """Financials; the link names a longer anchor that starts with this one.

        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#financials-ratios
        """
        raise NotImplementedError

    def news(self) -> Query:
        """News; the link lacks the GitHub URL: docs/datasets/stocks.md#news."""
        raise NotImplementedError


class _LinkedSession:
    @property
    def stocks(self) -> _Linked:
        raise NotImplementedError

    def news(self) -> Query:
        """Any news: docs/datasets/reference.md#news."""
        raise NotImplementedError


def _linked(name: str) -> DatasetSpec:
    return DatasetSpec(name=name, summary="A dataset for the tests.", fetch_path="/x")


def test_the_docstring_rule_reports_every_missing_link() -> None:
    specs = [
        _linked(name)
        for name in (
            "stocks.dividends",
            "stocks.splits",
            "stocks.ipos",
            "stocks.financials",
            "stocks.news",
            "stocks.missing",
            "reference.news",
            "stocks.dividends.more",  # `dividends` is a method, not a namespace
        )
    ]
    assert link_problems(_LinkedSession, specs) == [
        f"stocks.splits: the docstring must link {SITE}docs/datasets/stocks.md#splits",
        f"stocks.ipos: the docstring must link {SITE}docs/datasets/stocks.md#ipos",
        f"stocks.financials: the docstring must link {SITE}docs/datasets/stocks.md#financials",
        f"stocks.news: the docstring must link {SITE}docs/datasets/stocks.md#news",
        "stocks.missing: no method at m.stocks.missing",
        "reference.news: no method at m.reference.news",
        "stocks.dividends.more: no method at m.stocks.dividends.more",
    ]


def test_a_session_method_is_reached_directly() -> None:
    # The path a dataset of the session itself has, such as `m.snapshots`.
    assert factory(_LinkedSession, "m.news") is _LinkedSession.news
    assert factory(_LinkedSession, "m.stocks.dividends") is _Linked.dividends


# --- render on a synthetic catalog -------------------------------------------------------------


def _field(wire_key: str, column: str, wire_type: WireType, *, text: str = "") -> Field:
    return Field(wire_key=wire_key, column=column, wire_type=wire_type, description=text)


_SOURCES: dict[tuple[Transport, str], Source] = {
    (R, "/last/{ticker}"): Source(
        key="/last/{ticker}",
        transport=R,
        fields=(
            _field("ticker", "Ticker", WireType.STRING),
            _field("t", "SipTimestamp", WireType.ISO_INSTANT, text="When the trade printed."),
            _field("price", "Price", WireType.DOUBLE, text="The price, bid | ask."),
        ),
        memberships=(MEMBERSHIP(kind="trades", group="stocks", position=0),),
    ),
    # The kind's projection renames `p` and drops `x`; the page names the wire key `p`.
    (R, "/trades/{ticker}"): Source(
        key="/trades/{ticker}",
        transport=R,
        fields=(
            _field("t", "SipTimestamp", WireType.ISO_INSTANT),
            _field("p", "TradePrice", WireType.DOUBLE),
            _field("x", "Extra", WireType.LONG),
        ),
        memberships=(
            MEMBERSHIP(
                kind="trades",
                group="stocks",
                position=1,
                projection={"t": "SipTimestamp", "p": "Price"},
            ),
        ),
    ),
    (F, "us_stocks/trades"): Source(
        key="us_stocks/trades",
        transport=F,
        fields=(
            _field("ticker", "Ticker", WireType.STRING, text="The ticker."),
            _field("sip_timestamp", "SipTimestamp", WireType.ISO_INSTANT),
            _field("price", "Price", WireType.DOUBLE),
        ),
        memberships=(MEMBERSHIP(kind="trades", group="stocks", position=0),),
    ),
    (W, "stocks/T"): Source(
        key="stocks/T",
        transport=W,
        fields=(
            _field("sym", "Ticker", WireType.STRING),
            _field("t", "SipTimestamp", WireType.ISO_INSTANT),
            _field("p", "Price", WireType.DOUBLE),
        ),
        memberships=(MEMBERSHIP(kind="trades", group="stocks", position=0),),
    ),
    (W, "crypto/XT"): Source(
        key="crypto/XT",
        transport=W,
        fields=(
            _field("pair", "Pair", WireType.STRING),
            _field("t", "SipTimestamp", WireType.ISO_INSTANT),
            _field("p", "Price", WireType.DOUBLE),
            _field("r", "Received", WireType.ISO_INSTANT, text="When Massive received it."),
        ),
        memberships=(MEMBERSHIP(kind="trades", group="crypto", position=0),),
    ),
    (R, "/ratios"): Source(
        key="/ratios",
        transport=R,
        fields=(_field("pe", "PriceEarnings", WireType.DOUBLE, text="Price | earnings."),),
    ),
}

_STOCK_TRADES = DatasetSpec(
    name="stocks.trades",
    summary="Trades of stocks, tick by tick.",
    asset_class=AssetClass.STOCKS,
    kind="trades",
    fetch_path="/last/{ticker}",
    history_path="/trades/{ticker}",
    channels={None: "T"},
    flat_file_prefixes={None: "us_stocks/trades"},
    keys={
        (R, "/last/{ticker}"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp")),
        (R, "/trades/{ticker}"): SourceKeys(
            KeySpec("ticker", from_request=True), KeySpec("SipTimestamp")
        ),
        (F, "us_stocks/trades"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp")),
        (W, "stocks/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp")),
    },
)
_RATIOS = DatasetSpec(
    name="stocks.financials.ratios", summary="Financial ratios.", fetch_path="/ratios"
)
_CRYPTO_TRADES = DatasetSpec(
    name="crypto.trades",
    summary="Trades of crypto pairs.",
    asset_class=AssetClass.CRYPTO,
    kind="trades",
    channels={None: "XT"},
    keys={(W, "crypto/XT"): SourceKeys(KeySpec("Pair"), KeySpec("SipTimestamp"))},
)
_CATALOG = Catalog(
    (_STOCK_TRADES, _RATIOS, _CRYPTO_TRADES),
    source_tables=lambda transport: {k: s for (t, k), s in _SOURCES.items() if t is transport},
)
_PROVENANCE = Provenance(
    fetched=dt.date(2026, 10, 27),
    urls={
        # A source of no asset class: its dataset, which has none either, keeps every page.
        (R, "/ratios"): [
            "https://docs.example/rest/crypto/ratios.md",
            "https://docs.example/rest/stocks/ratios.md",
        ],
        # A shared source: `stocks.trades` keeps only the page whose path names `stocks`.
        (W, "stocks/T"): [
            "https://docs.example/ws/crypto/trades.md",
            "https://docs.example/ws/stocks/trades.md?v=1#top",
        ],
        # No page names the class, so the dataset keeps them all; the duplicate is listed once.
        (R, "/trades/{ticker}"): ["https://docs.example/a.md", "https://docs.example/a.md"],
        (W, "crypto/XT"): ["https://docs.example/ws/crypto.md"],
    },
)

_GUIDE = (
    "Each section lists one dataset's columns in the order every verb returns them. "
    "**Supplied by** names the transports that fill a column; a column the serving transport "
    "does not fill is null. The wire-field columns name the Massive fields behind each column "
    "(`request` for a value the library fills from the request). The same descriptions appear "
    "in the web UI's column tooltips and in a query's `columns()`."
)
_FOOTER = (
    "Generated by `uv run python scripts/render_reference.py` from Massive's documentation as "
    "fetched on 2026-10-27. The descriptions are Massive's, corrected where they are clearly "
    "wrong; the library writes its own for the key columns and for columns that only other "
    "datasets of the same kind fill."
)


def test_render_gives_one_page_per_namespace_in_catalog_order() -> None:
    pages = render(_CATALOG, _PROVENANCE)
    assert list(pages) == ["stocks.md", "crypto.md"]
    assert render(Catalog(()), _PROVENANCE) == {}


def test_the_rendered_stocks_page() -> None:
    page = render(_CATALOG, _PROVENANCE)["stocks.md"]
    schema = {c.name: c for c in _CATALOG.dataset_schema(_STOCK_TRADES).columns}
    symbol = schema["Symbol"].description.replace("|", "\\|")
    stamp = schema["Timestamp"].description.replace("|", "\\|")
    received = schema["Received"].description
    assert received == "Always null for `stocks.trades`; supplied by `crypto.trades`."
    every = "REST, flat files, WebSocket"
    head = (
        "| Column | Deephaven type | Description | Supplied by | REST field | Flat-file field "
        "| WebSocket field |"
    )
    price = (
        "| `Price` | double | The price, bid \\| ask. | REST, flat files, WebSocket "
        "| `price`, `p` | `price` | `p` |"
    )
    expected = f"""# Columns of the `m.stocks` datasets

<!-- Generated by `uv run python scripts/render_reference.py`; do not edit by hand. -->

{_GUIDE}

<a id="trades"></a>

## `m.stocks.trades`

Trades of stocks, tick by tick.

{head}
| --- | --- | --- | --- | --- | --- | --- |
| `Symbol` | String | {symbol} | {every} | `ticker`, request | `ticker` | `sym` |
| `Timestamp` | Instant | {stamp} | {every} | `t` | `sip_timestamp` | `t` |
{price}
| `Received` | Instant | {received} |  |  |  |  |

Massive's documentation of this dataset:

- <https://docs.example/a.md>
- <https://docs.example/ws/stocks/trades.md?v=1#top>

<a id="financials-ratios"></a>

## `m.stocks.financials.ratios`

Financial ratios.

| Column | Deephaven type | Description | Supplied by | REST field |
| --- | --- | --- | --- | --- |
| `PriceEarnings` | double | Price \\| earnings. | REST | `pe` |

Massive's documentation of this dataset:

- <https://docs.example/rest/crypto/ratios.md>
- <https://docs.example/rest/stocks/ratios.md>

---

{_FOOTER}
"""
    assert page == expected


def test_the_rendered_crypto_page() -> None:
    page = render(_CATALOG, _PROVENANCE)["crypto.md"]
    # One wire-field column for the one transport the dataset reads, and its own URLs only.
    assert "| Column | Deephaven type | Description | Supplied by | WebSocket field |\n" in page
    received = "| `Received` | Instant | When Massive received it. | WebSocket | `r` |\n"
    links = "\nMassive's documentation of this dataset:\n\n- <https://docs.example/ws/crypto.md>\n"
    assert page.endswith(f"{received}{links}\n---\n\n{_FOOTER}\n")


def test_a_dataset_whose_sources_have_no_urls_has_no_url_list() -> None:
    bare = Provenance(fetched=dt.date(2026, 10, 27), urls={})
    page = render(_CATALOG, bare)["crypto.md"]
    assert page.endswith(f"| WebSocket | `r` |\n\n---\n\n{_FOOTER}\n")
    assert "documentation of this dataset" not in page


def test_the_real_pages_link_only_the_datasets_own_class() -> None:
    options = rendered()["options.md"]
    snapshot = "https://massive.com/docs/rest/{}/snapshots/unified-snapshot.md"
    assert f"- <{snapshot.format('options')}>" in options
    for other in ("stocks", "forex", "crypto", "indices"):
        assert snapshot.format(other) not in options
    # The session-wide snapshot spans classes, so it links every class's page.
    reference = rendered()["reference.md"]
    for every in ("stocks", "options", "forex", "crypto", "indices"):
        assert f"- <{snapshot.format(every)}>" in reference


def test_every_page_line_is_clean() -> None:
    for page in (*render(_CATALOG, _PROVENANCE).values(), *rendered().values()):
        assert page.endswith("\n")
        assert not page.endswith("\n\n")
        assert all(line == line.rstrip() for line in page.split("\n"))


def test_anchors_and_call_paths() -> None:
    assert anchor(_STOCK_TRADES) == "trades"
    assert anchor(_RATIOS) == "financials-ratios"
    assert call_path(_RATIOS) == "m.stocks.financials.ratios"
    snapshots = CATALOG.by_name["reference.snapshots"]
    assert (anchor(snapshots), call_path(snapshots)) == ("snapshots", "m.snapshots")
    news = CATALOG.by_name["partners.benzinga.news"]
    assert (anchor(news), call_path(news)) == ("benzinga-news", "m.partners.benzinga.news")


# --- sync --------------------------------------------------------------------------------------


def test_sync_writes_the_pages_into_a_missing_directory(tmp_path: Path) -> None:
    target = tmp_path / "docs" / "datasets"
    result = sync(target, {"b.md": "B\n", "a.md": "A\n"})
    assert result == SyncResult(written=("a.md", "b.md"), deleted=())
    assert sorted(p.name for p in target.iterdir()) == ["a.md", "b.md"]
    assert (target / "a.md").read_bytes() == b"A\n"


def test_sync_with_no_pages_on_a_missing_directory_creates_nothing(tmp_path: Path) -> None:
    target = tmp_path / "datasets"
    assert sync(target, {}) == SyncResult(written=(), deleted=())
    assert not target.exists()


def test_sync_leaves_an_unchanged_page_untouched(tmp_path: Path) -> None:
    page = tmp_path / "a.md"
    page.write_bytes(b"A\n")
    os.utime(page, ns=(1_000_000_000, 1_000_000_000))
    (tmp_path / "b.md").write_bytes(b"old\n")
    result = sync(tmp_path, {"a.md": "A\n", "b.md": "new\n"})
    assert result == SyncResult(written=("b.md",), deleted=())
    assert page.stat().st_mtime_ns == 1_000_000_000
    assert (tmp_path / "b.md").read_bytes() == b"new\n"


def test_sync_deletes_everything_it_does_not_render(tmp_path: Path) -> None:
    target = tmp_path / "datasets"
    target.mkdir()
    (target / "stray.md").write_text("x")
    (target / "nested" / "deeper").mkdir(parents=True)
    (target / "nested" / "deeper" / "page.md").write_text("x")
    # A page's name taken by a directory: the directory goes, the page is written.
    (target / "a.md").mkdir()
    (target / "a.md" / "inner.md").write_text("x")
    result = sync(target, {"a.md": "A\n"})
    assert result == SyncResult(written=("a.md",), deleted=("a.md", "nested", "stray.md"))
    assert [p.name for p in target.iterdir()] == ["a.md"]
    assert (target / "a.md").read_bytes() == b"A\n"


def test_sync_never_follows_a_symbolic_link_out_of_its_directory(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.md").write_text("keep\n")
    (outside / "file.md").write_text("outside\n")
    target = tmp_path / "datasets"
    target.mkdir()
    (target / "linked").symlink_to(outside, target_is_directory=True)
    (target / "a.md").symlink_to(outside / "file.md")
    result = sync(target, {"a.md": "A\n"})
    assert result == SyncResult(written=("a.md",), deleted=("a.md", "linked"))
    # The links are gone, a real file replaces one, and nothing outside changed.
    assert not (target / "a.md").is_symlink()
    assert (target / "a.md").read_bytes() == b"A\n"
    assert sorted(p.name for p in target.iterdir()) == ["a.md"]
    assert (outside / "keep.md").read_text() == "keep\n"
    assert (outside / "file.md").read_text() == "outside\n"


def test_sync_refuses_a_directory_that_is_a_symbolic_link(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    (target / "important.txt").write_text("keep\n")
    link = tmp_path / "datasets"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="is a symbolic link"):
        sync(link, {"a.md": "A\n"})
    assert [p.name for p in target.iterdir()] == ["important.txt"]


@pytest.mark.parametrize("name", ["", ".", "..", "../escape.md", "sub/page.md", "/abs.md"])
def test_sync_refuses_a_page_name_that_is_not_a_plain_file_name(tmp_path: Path, name: str) -> None:
    (tmp_path / "stray.md").write_text("x")
    with pytest.raises(ValueError, match="is not a plain file name"):
        sync(tmp_path, {"a.md": "A\n", name: "x"})
    # Checked before anything is touched.
    assert [p.name for p in tmp_path.iterdir()] == ["stray.md"]
