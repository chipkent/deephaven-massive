"""What Massive's crypto ticker list holds: the currencies of each pair.

The library splits a crypto ticker into its two currencies only from Massive's list
(`/v3/reference/tickers?market=crypto`), and cannot split a pair whose `base_currency_symbol` and
`currency_symbol` do not join to its ticker. The crypto check reads the list of listed and of
delisted pairs, and `crypto_report` writes every row the library could not split.

Every function is pure: the live test passes in the rows, and `tests/conftest.py` turns the
report into a fixture.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

SHARED_SHOWN: Final = 30
"""The most tickers in both crypto reads that a report writes."""

UNJOINED_SHOWN: Final = 100
"""The most crypto rows whose currencies do not join that a report writes, one line each."""


def _unjoined(rows: Iterable[Mapping[str, Any]], read: str) -> list[str]:
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
    unjoined = [*_unjoined(listed, "listed"), *_unjoined(delisted, "delisted")]
    lines.append(f"rows whose currencies do not join to the ticker: {len(unjoined)}")
    lines.extend(f"    {line}" for line in unjoined[:UNJOINED_SHOWN])
    return lines
