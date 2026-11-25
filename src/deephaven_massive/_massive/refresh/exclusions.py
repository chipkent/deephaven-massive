"""The vendor items the refresh deliberately does not generate, each with its reason.

`EXCLUSIONS` lists the items that are in a vendor source but get no generated source: REST
paths of the OpenAPI specification, WebSocket channels of the WebSocket specification, and
flat-file prefixes of the flat-file overview pages. Each entry names an item present in the
pinned snapshot; one that names an item the snapshot lacks fails the refresh as stale, so the
list can never silently outlive the vendor's own sources. Endpoints that are absent from every
vendor source are not listed here; they are recorded below so nobody re-adds them:

- `/benzinga/v1/transcripts`: never existed.
- `/v1/summaries`: removed from the OpenAPI specification on 2026-10-28; never in the
  documentation index.
- `/benzinga/v1/news`: sunset, replaced by `/benzinga/v2/news`.
- `/vX/reference/financials`: sunset, replaced by `/stocks/financials/v1/*`.
- `/options/vX/open-interest`: absent from the OpenAPI specification of the 2026-11-16
  snapshot, which documents `/options/v1/open-interest`.
- `/options/vX/open-interest/underlying`: absent from the OpenAPI specification of the
  2026-11-25 snapshot, which documents `/options/v1/open-interest/underlying`.
- WebSocket `/futures/{venue}`: not documented; the futures WebSocket pages list `/futures` only.

Every generated source must be read by a dataset; the coverage test in
`tests/datasets/test_catalog.py` fails on one that is not. So when a refresh brings a new
endpoint, channel, or flat-file prefix, the same pull request does one of three things:

- adds a dataset that reads it;
- excludes it here, only when the library deliberately never supports it; or
- defers it: opens an issue, then adds a `DEFERRED` entry next to the coverage test with the
  reason and the issue number. The refresh keeps generating a deferred item.

An exclusion never means "not yet". Its reason is permanent: deprecated in practice, retired,
undocumented, or a duplicate of another source.

A path or channel the specification marks deprecated (`x-polygon-deprecation`) or draft
(`x-polygon-draft`) is never generated and never listed here: the specification already
accounts for it. So every item of the vendor sources is exactly one of generated, excluded,
or deprecated-or-draft, and the refresh checks that it is.

`undocumented=True` marks a path the documentation never mentions: no REST page that
`llms.txt` links has it on its endpoint line. The refresh checks that claim against the
snapshot in both directions, so an exclusion whose reason is "not documented" fails once
Massive documents the path, and one whose reason relies on documentation fails if the page
disappears.

`SAMPLE_ERRORS` lists the vendor samples the refresh deliberately does not believe: a
documented example value of a field that is wrong while the field's type is right (a CUSIP
that lost its leading zero by being written as a number). The `example-type` gate skips
exactly that value of that field; a `TYPE_FIXES` entry would instead loosen the type to
accept the wrong form. It also excuses a key a WebSocket example sends that the message
schema does not declare (the futures trade's `z`). An entry whose sample the snapshot no
longer shows, or no longer refuses, fails the refresh as stale.

`IGNORED_KEYS` lists the keys Massive sends on a source that the library deliberately does
not read: undocumented keys that carry nothing a user needs (a constant, or a value another
column already holds). The refresh writes them onto the source (`Source.ignored_keys`), and
the decoders drop them without reporting drift, so the drift report keeps showing only what
needs a decision. An entry for a key the source declares, or for a source the snapshot does not
generate, fails the refresh as stale.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from deephaven_massive._massive.spec.types import Transport

if TYPE_CHECKING:
    from deephaven_massive._massive.refresh.openapi import Json

__all__ = ["EXCLUSIONS", "IGNORED_KEYS", "SAMPLE_ERRORS", "Exclusion", "IgnoredKey", "SampleError"]


@dataclass(frozen=True, kw_only=True)
class Exclusion:
    """One vendor item the refresh does not generate.

    Attributes:
        kind: The transport of the item.
        item: The item exactly as its vendor source spells it: a REST path template, a
            WebSocket specification path (`"/launchpad/stocks/AM"`), or a flat-file prefix.
        reason: Why it is excluded, for the reviewer.
        undocumented: True when the reason is that no documentation page names the item; only
            REST exclusions use it.
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


def _launchpad() -> tuple[Exclusion, ...]:
    """Return the exclusions of the retired Launchpad channels.

    The WebSocket specification still lists Massive's Launchpad feeds (an aggregate `AM` and a
    value `LV` channel per asset class, under `/launchpad/`), but Massive retired the product
    on 2025-06-30, and no documentation page describes them.

    Returns:
        One exclusion per Launchpad channel.
    """
    return tuple(
        Exclusion(
            kind=Transport.WS,
            item=f"/launchpad/{cluster}/{channel}",
            reason="Launchpad, a retired product; no WebSocket documentation page describes it.",
        )
        for cluster in ("stocks", "options", "forex", "crypto")
        for channel in ("AM", "LV")
    )


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
    _rest("/options/v1/analytics/expiration", _UNDOCUMENTED, undocumented=True),
    _rest("/options/v1/analytics/strike", _UNDOCUMENTED, undocumented=True),
    _rest("/options/v1/analytics/underlying", _UNDOCUMENTED, undocumented=True),
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
    *_launchpad(),
)
"""Every excluded vendor item."""


@dataclass(frozen=True, kw_only=True)
class SampleError:
    """One documented example value the refresh does not believe, because the sample is wrong.

    Attributes:
        path: The source key: a REST path template exactly as the specification spells it, or
            a WebSocket `"<cluster>/<channel>"`.
        wire_key: The field's dotted wire key, or a key the example sends that the source does
            not declare.
        sample: The wrong example value, exactly as the specification gives it.
        reason: Why the sample, not the field's type, is wrong.
    """

    path: str
    wire_key: str
    sample: Json
    reason: str


SAMPLE_ERRORS: tuple[SampleError, ...] = (
    SampleError(
        path="futures/T",
        wire_key="z",
        sample=3,
        reason="The futures trade example sends `z`, which the message schema does not declare "
        "and no futures source carries (the stocks trade's `z` is its tape, which futures have "
        "not). Taken as a leftover of the stocks example; a live `z` is dropped and reported "
        "as drift.",
    ),
    SampleError(
        path="/vX/reference/ipos",
        wire_key="us_code",
        sample=37833100,
        reason="The field is a CUSIP, nine characters of text, and the page's own sample is "
        '"75383L102". The property example 37833100 is Apple\'s CUSIP 037833100 written as a '
        "number, which lost its leading zero; the type stays text, so a real CUSIP keeps "
        "every character.",
    ),
    SampleError(
        path="/futures/v1/products",
        wire_key="last_updated",
        sample="2025-02-22",
        reason="The specification declares format date-time, and Massive sends one: a live "
        'response on 2026-10-30 sent "2017-01-01T17:11:39-06:00" for the first product, a '
        "time of day with an offset, which a date cannot hold. The example's bare date is "
        "taken as wrong; the field stays an instant, and a live bare date would fail loudly.",
    ),
)
"""Every documented example value the `example-type` gate skips."""


@dataclass(frozen=True, kw_only=True)
class IgnoredKey:
    """One undeclared key a source sends that the library deliberately does not read.

    Attributes:
        source: The source key: a REST path template exactly as the specification spells it,
            or a WebSocket `"<cluster>/<channel>"`.
        wire_key: The key as Massive sends it, at the top of the row or message.
        reason: The live evidence, and why the key carries nothing a user needs.
    """

    source: str
    wire_key: str
    reason: str


_FINANCIALS_TICKERS = (
    "The live run of 2026-11-02 received `max_ticker` and `min_ticker` on the rows of a "
    'request for AAPL, both "AAPL". The financials pages document neither, and the row\'s '
    "own `tickers` column already lists every ticker of the company, each share class "
    "included, so the two keys add nothing it lacks."
)

IGNORED_KEYS: tuple[IgnoredKey, ...] = (
    *(
        IgnoredKey(source=path, wire_key=key, reason=_FINANCIALS_TICKERS)
        for path in (
            "/stocks/financials/v1/balance-sheets",
            "/stocks/financials/v1/cash-flow-statements",
        )
        for key in ("max_ticker", "min_ticker")
    ),
)
"""Every undeclared key the decoders drop without reporting drift."""
