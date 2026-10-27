"""Tests for the flat-file generator: prefixes, venues, columns, order, and its gates.

The first group runs against the vendored snapshot, and skips where there is none (an sdist
leaves it out). The rest build small synthetic overview and dataset pages, so each rule and
gate is shown in isolation; every gate has a test that fails when the gate is removed.
"""

from __future__ import annotations

import dataclasses
import os
from datetime import date
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import pytest

from deephaven_massive._massive.refresh import exclusions, flatfiles, overrides
from deephaven_massive._massive.refresh.flatfiles import (
    FlatFileCoverage,
    generate_flat_files,
    venue_template,
)
from deephaven_massive._massive.refresh.gates import RefreshError
from deephaven_massive._massive.refresh.hand_input import HandInput
from deephaven_massive._massive.refresh.snapshot import DIRECTORY, Snapshot
from deephaven_massive._massive.spec.types import Conversion, Source, Transport, WireType, WireUnit

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# --- The vendored snapshot -------------------------------------------------------------------

_requires_snapshot = pytest.mark.skipif(
    not DIRECTORY.is_dir() and "CI" not in os.environ,
    reason="no vendored snapshot (vendor/massive/) outside a source checkout; CI never skips",
)


@pytest.fixture(scope="module")
def real() -> tuple[tuple[Source, ...], FlatFileCoverage]:
    """Generate the flat-file sources of the vendored snapshot once for the module.

    Returns:
        The generated prefixes and the coverage.
    """
    return generate_flat_files(Snapshot.load())


@_requires_snapshot
def test_the_futures_venues_fold_into_one_template(
    real: tuple[tuple[Source, ...], FlatFileCoverage],
) -> None:
    files, coverage = real
    assert all(f.transport is Transport.FLAT_FILES for f in files)
    assert coverage.pages["us_futures_{venue}/trades_v1"] == (
        "futures/trades/cbot.md",
        "futures/trades/cme.md",
        "futures/trades/comex.md",
        "futures/trades/nymex.md",
    )
    assert coverage.pages["us_stocks_sip/trades_v1"] == ("stocks/trades.md",)


@_requires_snapshot
def test_the_four_venue_pages_of_each_futures_dataset_are_identical(
    real: tuple[tuple[Source, ...], FlatFileCoverage],
) -> None:
    # The venue-agreement gate itself, over each futures dataset's four real pages.
    snapshot = Snapshot.load()
    templates = {key: names for key, names in real[1].pages.items() if "{venue}" in key}
    assert len(templates) == 4
    for template, names in templates.items():
        by_venue = {PurePosixPath(name).stem: name for name in names}
        assert set(by_venue) == set(overrides.VENUES)
        found = flatfiles._check_venues(
            template, by_venue, snapshot=snapshot, venues=list(overrides.VENUES)
        )
        assert found == [], template


@_requires_snapshot
def test_the_columns_take_their_documented_types_units_and_conversions(
    real: tuple[tuple[Source, ...], FlatFileCoverage],
) -> None:
    files = {f.key: f for f in real[0]}
    trades = files["us_stocks_sip/trades_v1"]
    assert trades.field("Conditions").conversion is Conversion.CODE_LIST_TEXT
    assert trades.field("Conditions").wire_type is WireType.INT_ARRAY
    assert trades.field("SipTimestamp").unit is WireUnit.NS
    assert (
        trades.field("Ticker").description == "The exchange symbol that this item is traded under."
    )
    futures = files["us_futures_{venue}/trades_v1"]
    assert futures.field("SessionEndDate").wire_type is WireType.DATE
    assert futures.field("Timestamp").unit is WireUnit.NS
    # A capitalized example header (`Ticker`) still names the table's `ticker`.
    assert files["us_stocks_sip/quotes_v1"].fields[0].wire_key == "ticker"
    assert not any(f.required for source in files.values() for f in source.fields)


@_requires_snapshot
def test_the_flat_file_type_fixes_cite_the_documented_list(
    real: tuple[tuple[Source, ...], FlatFileCoverage],
) -> None:
    files = {f.key for f in real[0]}
    fixed = {key for key in overrides.TYPE_FIXES if key[0] in files}
    assert fixed == {
        ("us_stocks_sip/trades_v1", "conditions"),
        ("us_stocks_sip/quotes_v1", "conditions"),
        ("us_stocks_sip/quotes_v1", "indicators"),
        ("us_options_opra/trades_v1", "conditions"),
        ("global_crypto/trades_v1", "conditions"),
    }
    pages = Snapshot.load().flat_file_pages
    assert "| MSFT | 12,37 |" in pages["stocks/trades.md"]  # the list the conversion cites


# --- Synthetic pages ------------------------------------------------------------------------

_FF = "https://massive.com/docs/flat-files"


def _overview(*rows: tuple[str, str]) -> str:
    lines = [
        "## Available Datasets",
        "",
        "| Datasets | Description | Path |",
        "| --- | --- | --- |",
    ]
    lines += [f"| [D]({_FF}/{page}) | A dataset. | `S3 {prefix}` |" for page, prefix in rows]
    return "\n".join(lines) + "\n"


def _dataset(
    columns: Sequence[tuple[str, str, str]],
    header: Sequence[str] | None = None,
    rows: Sequence[Sequence[str]] = (),
) -> str:
    lines = ["## Columns", "", "| Column | Type | Description |", "|--------|------|-------------|"]
    lines += [f"| `{name}` | {kind} | {text} |" for name, kind, text in columns]
    names = list(header if header is not None else [c[0] for c in columns])
    lines += ["", "## Example Data", "", "| " + " | ".join(names) + " |"]
    lines += ["| " + " | ".join("---" for _ in names) + " |"]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([*lines, ""])


_TRADES = [
    ("ticker", "string", "The symbol."),
    ("price", "number", "The price."),
    ("size", "integer", "The size."),
]


def _snapshot(pages: Mapping[str, str]) -> Snapshot:
    return Snapshot(
        openapi={"paths": {}},
        llms="",
        pages={},
        fetched=date(2026, 1, 2),
        page_urls={},
        index_url="https://massive.com/docs/llms.txt",
        flat_file_pages=dict(pages),
        flat_file_page_urls={name: f"{_FF}/{name}" for name in pages},
    )


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
_VENUES = dataclasses.replace(_EMPTY, venues={"cme": "test", "cbot": "test"})


def _problems(snapshot: Snapshot, hand: HandInput = _EMPTY) -> list[tuple[str, str, str]]:
    with pytest.raises(RefreshError) as caught:
        generate_flat_files(snapshot, hand)
    return sorted((p.gate, p.source, p.key) for p in caught.value.problems)


def test_a_dataset_takes_its_overview_prefix_and_its_example_header_s_order() -> None:
    snapshot = _snapshot(
        {
            "stocks/overview.md": _overview(("stocks/trades.md", "us_stocks_sip/trades_v1")),
            "stocks/trades.md": _dataset(_TRADES, header=["Ticker", "size", "price"]),
            "stocks/unlisted.md": _dataset(_TRADES),  # no overview row: not generated
        }
    )
    (trades,), coverage = generate_flat_files(snapshot, _EMPTY)
    assert trades.key == "us_stocks_sip/trades_v1"
    assert [(f.wire_key, f.column, f.wire_type) for f in trades.fields] == [
        ("ticker", "Ticker", WireType.STRING),
        ("size", "Size", WireType.LONG),
        ("price", "Price", WireType.DOUBLE),
    ]
    assert trades.field("Price").description == "The price."
    assert coverage.specified == coverage.generated == {"us_stocks_sip/trades_v1"}
    assert coverage.pages == {"us_stocks_sip/trades_v1": ("stocks/trades.md",)}


def test_the_table_and_the_header_match_whatever_their_case() -> None:
    # The table's spelling is the wire key, however the header capitalizes it, and the reverse.
    columns = [("Ticker", "string", "The symbol."), ("price", "number", "The price.")]
    snapshot = _snapshot(
        {
            "stocks/overview.md": _overview(("stocks/trades.md", "us_stocks_sip/trades_v1")),
            "stocks/trades.md": _dataset(columns, header=["ticker", "PRICE"]),
        }
    )
    (trades,), _ = generate_flat_files(snapshot, _EMPTY)
    assert [f.wire_key for f in trades.fields] == ["Ticker", "price"]


def test_a_documented_timestamp_takes_its_units_entry() -> None:
    columns = [("ticker", "string", "The symbol."), ("start", "timestamp - integer", "The start.")]
    snapshot = _snapshot(
        {
            "overview.md": _overview(("bars.md", "us_x/bars_v1")),
            "bars.md": _dataset(columns),
        }
    )
    # "start" is not a timestamp-like name: the documented type alone makes it one.
    assert _problems(snapshot) == [("timestamp-unit", "us_x/bars_v1", "start")]
    ns = overrides.Unit(unit=WireUnit.NS, reason="test")
    (bars,), _ = generate_flat_files(
        snapshot, dataclasses.replace(_EMPTY, units={("us_x/bars_v1", "start"): ns})
    )
    assert (bars.field("Start").wire_type, bars.field("Start").unit) == (
        WireType.EPOCH,
        WireUnit.NS,
    )


def test_venues_fold_into_a_template_whose_pages_must_agree() -> None:
    pages = {
        "futures/overview.md": _overview(
            ("futures/trades/cme.md", "us_futures_cme/trades_v1"),
            ("futures/trades/cbot.md", "us_futures_cbot/trades_v1"),
        ),
        "futures/trades/cme.md": _dataset(_TRADES),
        # The tables' own order may differ between venues; their files' order may not.
        "futures/trades/cbot.md": _dataset(
            list(reversed(_TRADES)), header=["ticker", "price", "size"]
        ),
    }
    (trades,), coverage = generate_flat_files(_snapshot(pages), _VENUES)
    assert trades.key == "us_futures_{venue}/trades_v1"
    assert coverage.pages[trades.key] == ("futures/trades/cbot.md", "futures/trades/cme.md")


def test_gate_venue_agreement_a_venue_page_with_other_columns() -> None:
    pages = {
        "futures/overview.md": _overview(
            ("futures/trades/cme.md", "us_futures_cme/trades_v1"),
            ("futures/trades/cbot.md", "us_futures_cbot/trades_v1"),
        ),
        "futures/trades/cme.md": _dataset(_TRADES),
        "futures/trades/cbot.md": _dataset([*_TRADES, ("exchange", "integer", "The exchange.")]),
    }
    assert _problems(_snapshot(pages), _VENUES) == [
        ("venue-agreement", "us_futures_{venue}/trades_v1", "cbot")
    ]


@pytest.mark.parametrize(
    "other",
    [
        _dataset(_TRADES, header=["ticker", "size", "price"]),
        _dataset(
            [(name, "number" if name == "size" else kind, text) for name, kind, text in _TRADES]
        ),
    ],
    ids=["file order", "column type"],
)
def test_gate_venue_agreement_a_venue_page_with_another_order_or_type(other: str) -> None:
    pages = {
        "futures/overview.md": _overview(
            ("futures/trades/cme.md", "us_futures_cme/trades_v1"),
            ("futures/trades/cbot.md", "us_futures_cbot/trades_v1"),
        ),
        "futures/trades/cme.md": _dataset(_TRADES),
        "futures/trades/cbot.md": other,
    }
    assert _problems(_snapshot(pages), _VENUES) == [
        ("venue-agreement", "us_futures_{venue}/trades_v1", "cbot")
    ]


def test_gate_example_type_reads_every_venue_page_s_rows() -> None:
    pages = {
        "futures/overview.md": _overview(
            ("futures/trades/cme.md", "us_futures_cme/trades_v1"),
            ("futures/trades/cbot.md", "us_futures_cbot/trades_v1"),
        ),
        "futures/trades/cme.md": _dataset(_TRADES, rows=[["ESZ4", "1.5", "2"]]),
        "futures/trades/cbot.md": _dataset(_TRADES, rows=[["ZBZ4", "1.5", "2.5"]]),
    }
    assert _problems(_snapshot(pages), _VENUES) == [
        ("example-type", "us_futures_{venue}/trades_v1", "size")
    ]


def test_gate_docs_another_venue_s_example_rows_that_cannot_be_read() -> None:
    pages = {
        "futures/overview.md": _overview(
            ("futures/trades/cme.md", "us_futures_cme/trades_v1"),
            ("futures/trades/cbot.md", "us_futures_cbot/trades_v1"),
        ),
        "futures/trades/cme.md": _dataset(_TRADES, rows=[["ESZ4", "1.5", "2"]]),
        "futures/trades/cbot.md": _dataset(_TRADES, rows=[["ZBZ4", "1.5"]]),
    }
    assert _problems(_snapshot(pages), _VENUES) == [
        ("docs", "us_futures_{venue}/trades_v1", "futures/trades/cbot.md")
    ]


def test_gate_venue_agreement_a_venue_without_a_page() -> None:
    pages = {
        "futures/overview.md": _overview(
            ("futures/trades/cme.md", "us_futures_cme/trades_v1"),
            ("futures/quotes/cme.md", "us_futures_cme/quotes_v1"),
            ("futures/quotes/cbot.md", "us_futures_cbot/quotes_v1"),
        ),
        "futures/trades/cme.md": _dataset(_TRADES),
        "futures/quotes/cme.md": _dataset(_TRADES),
        "futures/quotes/cbot.md": _dataset(_TRADES),
    }
    assert _problems(_snapshot(pages), _VENUES) == [
        ("venue-agreement", "us_futures_{venue}/trades_v1", "cbot")
    ]


def test_gate_docs_another_venue_s_page_that_cannot_be_read() -> None:
    pages = {
        "futures/overview.md": _overview(
            ("futures/trades/cme.md", "us_futures_cme/trades_v1"),
            ("futures/trades/cbot.md", "us_futures_cbot/trades_v1"),
        ),
        "futures/trades/cme.md": _dataset(_TRADES),
        "futures/trades/cbot.md": "## Columns\n\n| a | b |\n|---|---|\n| junk |\n",
    }
    assert _problems(_snapshot(pages), _VENUES) == [
        ("docs", "us_futures_{venue}/trades_v1", "futures/trades/cbot.md")
    ]


def test_an_unreadable_first_venue_page_is_reported_once() -> None:
    pages = {
        "futures/overview.md": _overview(
            ("futures/trades/cme.md", "us_futures_cme/trades_v1"),
            ("futures/trades/cbot.md", "us_futures_cbot/trades_v1"),
        ),
        "futures/trades/cme.md": "## Columns\n\n| a | b |\n|---|---|\n| junk |\n",
        "futures/trades/cbot.md": _dataset(_TRADES),
    }
    assert _problems(_snapshot(pages), _VENUES) == [("docs", "us_futures_{venue}/trades_v1", "")]


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        ("us_futures_cme/trades_v1", ("us_futures_{venue}/trades_v1", "cme")),
        ("us_stocks_sip/trades_v1", ("us_stocks_sip/trades_v1", None)),
        ("us_cmex/trades_v1", ("us_cmex/trades_v1", None)),  # a suffix, not a venue word
    ],
)
def test_venue_template(prefix: str, expected: tuple[str, str | None]) -> None:
    assert venue_template(prefix, ["cme", "cbot"]) == expected


def test_gate_stale_entry_a_venue_no_prefix_names() -> None:
    pages = {
        "stocks/overview.md": _overview(("stocks/trades.md", "us_stocks_sip/trades_v1")),
        "stocks/trades.md": _dataset(_TRADES),
    }
    assert _problems(_snapshot(pages), _VENUES) == [
        ("stale-entry", "", "cbot"),
        ("stale-entry", "", "cme"),
    ]


def test_an_excluded_prefix_is_not_generated_and_a_stale_one_is_reported() -> None:
    pages = {
        "stocks/overview.md": _overview(
            ("stocks/trades.md", "us_stocks_sip/trades_v1"),
            ("stocks/quotes.md", "us_stocks_sip/quotes_v1"),
        ),
        "stocks/trades.md": _dataset(_TRADES),
        "stocks/quotes.md": "no columns at all",
    }
    entry = exclusions.Exclusion(
        kind=Transport.FLAT_FILES, item="us_stocks_sip/quotes_v1", reason="t"
    )
    other = exclusions.Exclusion(kind=Transport.WS, item="/stocks/LV", reason="t")
    hand = dataclasses.replace(_EMPTY, exclusions=(entry, other))
    (trades,), coverage = generate_flat_files(_snapshot(pages), hand)
    assert trades.key == "us_stocks_sip/trades_v1"
    assert coverage.excluded == {"us_stocks_sip/quotes_v1"}
    gone = exclusions.Exclusion(kind=Transport.FLAT_FILES, item="us_gone/x_v1", reason="t")
    hand = dataclasses.replace(_EMPTY, exclusions=(entry, gone))
    assert _problems(_snapshot(pages), hand) == [("stale-entry", "us_gone/x_v1", "")]


@pytest.mark.parametrize(
    "page",
    [
        _dataset(_TRADES, header=["ticker", "price"]),  # a column the files lack
        _dataset(_TRADES, header=["ticker", "price", "size", "extra"]),  # one the table lacks
        "## Example Data\n\n| ticker |\n| --- |\n",  # no Columns table
    ],
    ids=["missing", "extra", "no table"],
)
def test_gate_docs_an_example_header_that_differs_from_the_columns(page: str) -> None:
    pages = {
        "stocks/overview.md": _overview(("stocks/trades.md", "us_stocks_sip/trades_v1")),
        "stocks/trades.md": page,
    }
    assert _problems(_snapshot(pages)) == [("docs", "us_stocks_sip/trades_v1", "")]


def test_gate_docs_an_unreadable_dataset_page() -> None:
    pages = {
        "stocks/overview.md": _overview(("stocks/trades.md", "us_stocks_sip/trades_v1")),
        "stocks/trades.md": _dataset(_TRADES).replace("| `price` |", "| price |"),
    }
    assert _problems(_snapshot(pages)) == [("docs", "us_stocks_sip/trades_v1", "")]


def test_gate_shape_a_column_type_the_pages_do_not_use() -> None:
    pages = {
        "stocks/overview.md": _overview(("stocks/trades.md", "us_stocks_sip/trades_v1")),
        "stocks/trades.md": _dataset([*_TRADES, ("flag", "boolean", "A flag.")]),
    }
    assert _problems(_snapshot(pages)) == [("shape", "us_stocks_sip/trades_v1", "flag")]


def test_gate_docs_two_overview_rows_naming_one_prefix() -> None:
    pages = {
        "stocks/overview.md": _overview(
            ("stocks/trades.md", "us_stocks_sip/trades_v1"),
            ("stocks/trades.md", "us_stocks_sip/trades_v1"),  # the same row twice is harmless
            ("stocks/other.md", "us_stocks_sip/trades_v1"),
        ),
        "stocks/trades.md": _dataset(_TRADES),
        "stocks/other.md": _dataset(_TRADES),
    }
    assert _problems(_snapshot(pages)) == [
        ("docs", "us_stocks_sip/trades_v1", "stocks/overview.md")
    ]


def test_gate_docs_an_overview_row_naming_a_page_outside_the_snapshot() -> None:
    pages = {
        "stocks/overview.md": _overview(
            ("stocks/trades.md", "us_stocks_sip/trades_v1"),
            ("stocks/gone.md", "us_stocks_sip/gone_v1"),
        )
        + "| [D](https://example.com/x.md) | Elsewhere. | `S3 us_stocks_sip/x_v1` |\n",
        "stocks/trades.md": _dataset(_TRADES),
    }
    assert _problems(_snapshot(pages)) == [
        ("docs", "us_stocks_sip/gone_v1", "stocks/overview.md"),
        ("docs", "us_stocks_sip/x_v1", "stocks/overview.md"),
    ]


def test_a_type_fix_turns_a_documented_code_into_a_list() -> None:
    columns = [("ticker", "string", "The symbol."), ("conditions", "integer", "The codes.")]
    pages = {
        "stocks/overview.md": _overview(("stocks/trades.md", "us_stocks_sip/trades_v1")),
        "stocks/trades.md": _dataset(columns),
    }
    fix = overrides.TypeFix(conversion=Conversion.CODE_LIST_TEXT, reason="test")
    hand = dataclasses.replace(_EMPTY, type_fixes={("us_stocks_sip/trades_v1", "conditions"): fix})
    (trades,), _ = generate_flat_files(_snapshot(pages), hand)
    assert trades.field("Conditions").wire_type is WireType.INT_ARRAY


def test_gate_example_type_reads_every_example_row() -> None:
    # The cells are read as the file's reader reads them: a list of codes, an empty cell,
    # numeric text in a text column, and a number in exponent form all convert.
    columns = [
        ("id", "string", "The ID."),
        ("conditions", "integer", "The codes."),
        ("volume", "number", "The volume."),
    ]
    rows = [["1", "12,37", "1.44E+06"], ["2", "", "55"], ["x", "12", "-3.5"]]
    pages = {
        "stocks/overview.md": _overview(("stocks/trades.md", "us_stocks_sip/trades_v1")),
        "stocks/trades.md": _dataset(columns, rows=rows),
    }
    named = dataclasses.replace(_EMPTY, identity={"id": "test"})
    assert _problems(_snapshot(pages), named) == [
        ("example-type", "us_stocks_sip/trades_v1", "conditions")
    ]
    fix = overrides.TypeFix(conversion=Conversion.CODE_LIST_TEXT, reason="test")
    hand = dataclasses.replace(named, type_fixes={("us_stocks_sip/trades_v1", "conditions"): fix})
    _, coverage = generate_flat_files(_snapshot(pages), hand)
    assert coverage.uses["us_stocks_sip/trades_v1"].needed_conversions == {"conditions"}
    # A number column's cell that is no number is a wrong type or a wrong sample.
    pages["stocks/trades.md"] = _dataset(columns, rows=[["1", "12", "n/a"]])
    assert _problems(_snapshot(pages), hand) == [
        ("example-type", "us_stocks_sip/trades_v1", "volume")
    ]


def test_gate_docs_an_example_row_of_another_width() -> None:
    pages = {
        "stocks/overview.md": _overview(("stocks/trades.md", "us_stocks_sip/trades_v1")),
        "stocks/trades.md": _dataset(_TRADES, rows=[["BCC", "1.5"]]),
    }
    assert _problems(_snapshot(pages)) == [("docs", "us_stocks_sip/trades_v1", "")]
