"""The vendor items the refresh deliberately does not generate, each with its reason.

`EXCLUSIONS` lists the REST paths that are in Massive's OpenAPI specification but get no
generated source. Each entry names an item present in the pinned snapshot; one that names an
item the snapshot lacks fails the refresh as stale, so the list can never silently outlive
the vendor's own sources. Endpoints that are absent from every vendor source are not listed
here; the design documents them so nobody re-adds them.

A path the specification marks deprecated (`x-polygon-deprecation`) or draft
(`x-polygon-draft`) is never generated and never listed here: the specification already
accounts for it. So every path of the specification is exactly one of generated, excluded,
or deprecated-or-draft, and the refresh checks that it is.

`undocumented=True` marks a path the documentation never mentions: no REST page that
`llms.txt` links has it on its endpoint line. The refresh checks that claim against the
snapshot in both directions, so an exclusion whose reason is "not documented" fails once
Massive documents the path, and one whose reason relies on documentation fails if the page
disappears.
"""

from __future__ import annotations

from dataclasses import dataclass

from deephaven_massive._massive.spec.types import Transport

__all__ = ["EXCLUSIONS", "Exclusion"]


@dataclass(frozen=True, kw_only=True)
class Exclusion:
    """One vendor item the refresh does not generate.

    Attributes:
        kind: The transport of the item.
        item: The REST path template exactly as the specification spells it.
        reason: Why it is excluded, for the reviewer.
        undocumented: True when the reason is that no documentation page names the item.
    """

    kind: Transport
    item: str
    reason: str
    undocumented: bool = False


_UNDOCUMENTED = (
    "Present only in the OpenAPI specification; no page of the documentation index names it."
)


def _rest(item: str, reason: str, *, undocumented: bool = False) -> Exclusion:
    """Return a REST exclusion.

    Args:
        item: The path template.
        reason: Why it is excluded.
        undocumented: Whether the reason is that no documentation page names it.

    Returns:
        The entry.
    """
    return Exclusion(kind=Transport.REST, item=item, reason=reason, undocumented=undocumented)


def _indicator_variants() -> tuple[Exclusion, ...]:
    """Return the exclusions of the indicator paths that repeat the stocks path exactly.

    Massive documents each indicator once per asset class, as five templates of one URL
    (`{stockTicker}`, `{optionsTicker}`, `{indicesTicker}`, `{fxTicker}`, `{cryptoTicker}`);
    the ticker's prefix selects the asset class. The options, indices, and forex templates
    document exactly the fields and parameters of the stocks template, so they are excluded
    as repeats; a refresh test checks that they still are. The crypto templates document no
    `adjusted` parameter, so they are a different interface and are generated.

    Returns:
        One exclusion per indicator and repeating placeholder.
    """
    return tuple(
        _rest(
            f"/v1/indicators/{indicator}/{{{placeholder}}}",
            f"The same URL, fields, and parameters as /v1/indicators/{indicator}/{{stockTicker}}, "
            "which serves every asset class by the ticker's prefix.",
        )
        for indicator in ("ema", "macd", "rsi", "sma")
        for placeholder in ("optionsTicker", "indicesTicker", "fxTicker")
    )


EXCLUSIONS: tuple[Exclusion, ...] = (
    _rest(
        "/v3/reference/dividends",
        "Deprecated by Massive (its page is titled 'Dividends (Deprecated)'); replaced by "
        "/stocks/v1/dividends.",
    ),
    _rest(
        "/v3/reference/splits",
        "Deprecated by Massive (its page is titled 'Splits (Deprecated)'); replaced by "
        "/stocks/v1/splits.",
    ),
    _rest("/v1/summaries", _UNDOCUMENTED, undocumented=True),
    _rest("/stocks/filings/vX/form-5", _UNDOCUMENTED, undocumented=True),
    _rest("/stocks/v1/exchanges", _UNDOCUMENTED, undocumented=True),
    _rest("/options/v1/exchanges", _UNDOCUMENTED, undocumented=True),
    _rest("/forex/v1/exchanges", _UNDOCUMENTED, undocumented=True),
    _rest("/crypto/v1/exchanges", _UNDOCUMENTED, undocumented=True),
    _rest("/options/v3/trades/{ticker}", _UNDOCUMENTED, undocumented=True),
    _rest("/options/v3/quotes/{ticker}", _UNDOCUMENTED, undocumented=True),
    _rest("/v1/reference/ipos", _UNDOCUMENTED, undocumented=True),
    _rest("/stocks/dev/trades/{ticker}", _UNDOCUMENTED, undocumented=True),
    _rest("/stocks/filings/10-K/vX_0/sections", _UNDOCUMENTED, undocumented=True),
    _rest(
        "/v2/snapshot/locale/us/markets/stocks/tickers/{stocksTicker}",
        "Covered by the stocks snapshots dataset, which sends the `tickers` filter to the "
        "full-market snapshot.",
    ),
    _rest(
        "/v2/snapshot/locale/global/markets/forex/tickers/{ticker}",
        "Covered by the forex snapshots dataset, which sends the `tickers` filter to the "
        "full-market snapshot.",
    ),
    _rest(
        "/v2/snapshot/locale/global/markets/crypto/tickers/{ticker}",
        "Covered by the crypto snapshots dataset, which sends the `tickers` filter to the "
        "full-market snapshot.",
    ),
    _rest(
        "/v3/snapshot/options/{underlyingAsset}/{optionContract}",
        "Covered by the options snapshots dataset through the unified snapshot, which returns "
        "the same fields for a contract.",
    ),
    *_indicator_variants(),
)
"""Every excluded vendor item."""
