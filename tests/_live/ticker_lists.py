"""What Massive's ticker lists hold: the forms of futures spread tickers and crypto pair currencies.

Two live checks read a whole list of tickers from Massive and record what the library's symbol
rules make of it, so a person can read where the rules and Massive's tickers part:

- **Futures spreads.** Massive documents no grammar for spread (combo) tickers; the library
  accepts the forms its samples show (`dm.canonical_symbol_future_spread`). The spread check
  reads the combo products of every venue, then the combo contracts of each venue
  (`spread_reads` chooses which product codes each read names), and writes every ticker form
  with examples, each example's legs, and whether the library accepts it (`spread_report`).
  A form is a ticker with each run of letters written `A` and each run of digits `9`
  (`ticker_form`): `GCQ6-GCZ6` is `A9-A9`, `BOB 02-01 U6` is `A 9-9 A9`.
- **Crypto pairs.** The library splits a crypto ticker into its two currencies only from
  Massive's list (`/v3/reference/tickers?market=crypto`), and refuses a pair whose
  `base_currency_symbol` and `currency_symbol` do not join to its ticker. The crypto check reads
  the list of listed and of delisted pairs, and `crypto_report` writes every row the library
  would refuse.

Every function is pure: the live tests pass in the rows and the library's checks, and
`tests/conftest.py` turns the functions into fixtures.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

VENUE_ANCHORS: Final = {
    "XCME": ("ES", "6A"),
    "XCBT": ("ZN", "ZC"),
    "XNYM": ("CL", "NG"),
    "XCEC": ("GC", "SI"),
}
"""Two single products of each venue (CME, CBOT, NYMEX, COMEX, by MIC) whose calendar spreads a
combo read of the venue includes: a calendar spread such as `GCQ6-GCZ6` may carry the product
code of its legs rather than a combo product's code."""

PRODUCTS_PER_READ: Final = 20
"""The most combo product codes one contracts read names beside its venue's anchors."""

EXAMPLES: Final = 3
"""The examples written for each ticker form."""

REFUSED_SHOWN: Final = 30
"""The most tickers the library refuses that a report writes per venue."""

SHARED_SHOWN: Final = 30
"""The most tickers in both crypto reads that a report writes."""

UNJOINED_SHOWN: Final = 100
"""The most crypto rows whose currencies do not join that a report writes, one line each."""

_RUNS: Final = re.compile(r"[A-Za-z]+|[0-9]+")
"""A run of letters or of digits, which a ticker form writes as one `A` or `9`."""

_LEG_SEPARATORS: Final = re.compile(r"[-: ]")
"""What separates the parts of a spread ticker that may be its legs."""

_CONTRACT: Final = re.compile(r"[A-Z0-9]+?[FGHJKMNQUVXZ]\d{1,2}")
"""A single contract ticker: a product code, a month letter, and a one- or two-digit year."""


@dataclass(frozen=True)
class SpreadRead:
    """One read of the combo contracts of a venue.

    Attributes:
        venue: The venue's MIC, such as `"XNYM"`.
        product_codes: The product codes the read names (`product_code.any_of`): the venue's
            anchors (`VENUE_ANCHORS`), then its combo products'.
    """

    venue: str
    product_codes: tuple[str, ...]


def ticker_form(ticker: str) -> str:
    """Return the form of a ticker: each run of letters as `A`, each run of digits as `9`.

    Args:
        ticker: A ticker, such as `"CL:BF Q6-U6-V6"`.

    Returns:
        Its form, such as `"A:A A9-A9-A9"`; every other character is kept.
    """
    return _RUNS.sub(lambda run: "A" if run.group().isalpha() else "9", ticker)


def accepts(check: Callable[[str], object], text: str) -> bool:
    """Tell whether one of the library's symbol checks accepts a ticker.

    Args:
        check: A check that raises `ValueError` for a ticker it refuses, such as
            `dm.canonical_symbol_future_spread`.
        text: The ticker.

    Returns:
        True unless the check raised `ValueError`.
    """
    try:
        check(text)
    except ValueError:
        return False
    return True


def ticker_legs(ticker: str) -> list[str]:
    """Return the parts of a spread ticker that are single contract tickers by themselves.

    Args:
        ticker: A spread ticker, such as `"GCQ6-GCZ6"`.

    Returns:
        The parts, split at `-`, `:`, and spaces, that have a contract's shape (a product code,
        a month letter, and a one- or two-digit year), such as `["GCQ6", "GCZ6"]`; empty when
        the legs are not written out whole (`CL:BF Q6-U6-V6` writes only their months).
    """
    return [part for part in _LEG_SEPARATORS.split(ticker) if _CONTRACT.fullmatch(part)]


def _code(row: Mapping[str, Any]) -> str:
    """Return a row's product code as text.

    Args:
        row: A product or contract row.

    Returns:
        Its `product_code`, or `""` without one.
    """
    return str(row.get("product_code") or "")


def _venues(products: Iterable[Mapping[str, Any]]) -> list[str]:
    """Return the venues to read: the anchor venues, then any other, most products first.

    Args:
        products: The combo products.

    Returns:
        The venues' MICs.
    """
    counts = Counter(str(row.get("trading_venue")) for row in products)
    others = sorted((v for v in counts if v not in VENUE_ANCHORS), key=lambda v: (-counts[v], v))
    return [*VENUE_ANCHORS, *others]


def _spread_over_types(products: Sequence[Mapping[str, Any]], count: int) -> list[str]:
    """Choose product codes in turn from each strategy type, so every type is represented.

    Args:
        products: One venue's combo products.
        count: The most codes to choose.

    Returns:
        Distinct product codes: the first of each strategy type, then the second of each, and
        so on, the types in order of their first product.
    """
    by_type: dict[str, list[str]] = {}
    for row in products:
        code = _code(row)
        codes = by_type.setdefault(str(row.get("strategy_type")), [])
        if code and code not in codes:
            codes.append(code)
    chosen: list[str] = []
    depth = 0
    while len(chosen) < count and any(depth < len(codes) for codes in by_type.values()):
        for codes in by_type.values():
            if depth < len(codes) and codes[depth] not in chosen and len(chosen) < count:
                chosen.append(codes[depth])
        depth += 1
    return chosen


def spread_reads(products: Sequence[Mapping[str, Any]]) -> list[SpreadRead]:
    """Choose the reads of combo contracts: one per venue, naming its anchors and combo products.

    The futures contracts endpoint filters by product code, not by venue, so each venue's read
    names the venue's anchors (`VENUE_ANCHORS`), whose calendar spreads may carry the legs'
    product code, and up to `PRODUCTS_PER_READ` of the venue's combo products, taken in turn
    from each strategy type.

    Args:
        products: The combo products of every venue (`/futures/v1/products?type=combo`).

    Returns:
        The reads, one request each: the four anchor venues first, then any other venue with
        the most products first.
    """
    chosen = []
    for venue in _venues(products):
        own = [row for row in products if str(row.get("trading_venue")) == venue]
        anchors = VENUE_ANCHORS.get(venue, ())
        codes = [c for c in _spread_over_types(own, PRODUCTS_PER_READ) if c not in anchors]
        chosen.append(SpreadRead(venue, (*anchors, *codes)))
    return chosen


def _product_lines(products: Sequence[Mapping[str, Any]]) -> list[str]:
    """Describe the combo products of each venue: how many, of which strategy types.

    Args:
        products: The combo products.

    Returns:
        One line for the whole list, then one per venue.
    """
    by_venue = Counter(str(row.get("trading_venue")) for row in products)
    lines = [f"combo products: {len(products)}, by venue {dict(sorted(by_venue.items()))}"]
    for venue in sorted(by_venue):
        own = [row for row in products if str(row.get("trading_venue")) == venue]
        types = Counter(str(row.get("strategy_type")) for row in own)
        examples = [f"{_code(row)} {row.get('name')!r}" for row in own[:EXAMPLES]]
        lines.append(f"    {venue}: strategy types {dict(sorted(types.items()))}; e.g. {examples}")
    return lines


def spread_report(
    products: Sequence[Mapping[str, Any]],
    contracts: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    spread: Callable[[str], object],
) -> list[str]:
    """Describe the combo products and, per venue read, every form its combo tickers take.

    Args:
        products: The combo products of every venue.
        contracts: The combo contracts each venue's read returned, by the venue's MIC.
        spread: The library's spread check, `dm.canonical_symbol_future_spread`.

    Returns:
        Evidence lines: the products by venue and strategy type; for each venue read, its
        count of tickers and forms, then one line per form with its count, how many of its
        tickers the library accepts, and examples with their contract name, product code, and
        legs; and the tickers the library refuses, at most `REFUSED_SHOWN`.
    """
    lines = _product_lines(products)
    for venue, rows in contracts.items():
        by_ticker = {str(row.get("ticker")): row for row in rows if row.get("ticker")}
        forms: dict[str, list[str]] = {}
        for ticker in sorted(by_ticker):
            forms.setdefault(ticker_form(ticker), []).append(ticker)
        lines.append(f"{venue} combo contracts: {len(by_ticker)} tickers in {len(forms)} forms")
        refused: list[str] = []
        for form, tickers in sorted(forms.items(), key=lambda item: (-len(item[1]), item[0])):
            accepted = [t for t in tickers if accepts(spread, t)]
            refused.extend(t for t in tickers if t not in accepted)
            examples = [
                f"{t} ({by_ticker[t].get('name')!r}, product {_code(by_ticker[t])}, "
                f"legs {ticker_legs(t)})"
                for t in tickers[:EXAMPLES]
            ]
            lines.append(
                f"    form {form!r}: {len(tickers)} tickers, the library accepts "
                f"{len(accepted)}; e.g. {examples}"
            )
        lines.append(f"    refused by the library: {refused[:REFUSED_SHOWN]}")
    return lines


def unjoined(rows: Iterable[Mapping[str, Any]], read: str) -> list[str]:
    """Describe the rows whose two currencies do not join to their ticker.

    The library's rule: a crypto ticker splits only when `X:` + `base_currency_symbol` +
    `currency_symbol` is the ticker, both currencies being text.

    Args:
        rows: Rows of the crypto ticker list.
        read: Which read they came from, `"listed"` or `"delisted"`.

    Returns:
        One description per such row, such as `"X:ETHWBTC (listed): base 'ETH', quote 'BTC'"`.
    """
    found = []
    for row in rows:
        ticker = row.get("ticker")
        base, quote = row.get("base_currency_symbol"), row.get("currency_symbol")
        joined = isinstance(base, str) and isinstance(quote, str) and f"X:{base}{quote}" == ticker
        if not joined:
            found.append(f"{ticker} ({read}): base {base!r}, quote {quote!r}")
    return found


def crypto_report(
    listed: Sequence[Mapping[str, Any]],
    delisted: Sequence[Mapping[str, Any]],
    *,
    listed_limit: int | None,
    delisted_limit: int | None,
) -> list[str]:
    """Describe the crypto ticker list: its size and every row the library cannot split.

    Args:
        listed: The rows read with `active=true`.
        delisted: The rows read with `active=false`.
        listed_limit: The most rows the listed read was allowed, or None when it read every
            page: a read that returned exactly that many may have been cut short.
        delisted_limit: The same for the delisted read.

    Returns:
        Evidence lines: each read's count of rows (marked when it may be cut short), the
        tickers in both reads (at most `SHARED_SHOWN`), the tickers without the `X:` prefix,
        and how many rows have currencies that do not join to their ticker, then one line
        for each of the first `UNJOINED_SHOWN`.
    """
    lines = []
    reads = (("listed", listed, listed_limit), ("delisted", delisted, delisted_limit))
    for read, rows, limit in reads:
        cut = " (may be cut short at the read's limit)" if len(rows) == limit else ""
        lines.append(f"{read} crypto tickers: {len(rows)} rows{cut}")
    tickers = [str(row.get("ticker")) for row in (*listed, *delisted)]
    both = sorted({str(r.get("ticker")) for r in listed} & {str(r.get("ticker")) for r in delisted})
    lines.append(
        f"tickers in both reads (the library keeps the listed row): {len(both)}, "
        f"e.g. {both[:SHARED_SHOWN]}"
    )
    lines.append(f"tickers without the X: prefix: {sorted(t for t in tickers if t[:2] != 'X:')}")
    split_refused = [*unjoined(listed, "listed"), *unjoined(delisted, "delisted")]
    lines.append(f"rows whose currencies do not join to the ticker: {len(split_refused)}")
    lines.extend(f"    {line}" for line in split_refused[:UNJOINED_SHOWN])
    return lines
