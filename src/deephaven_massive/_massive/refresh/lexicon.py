"""The lexicons that translate Massive's short wire keys into its own long names.

Some REST objects use one- and two-letter keys (`{"p": 189.5, "s": 100, "t": ...}`), and one
letter means different things in different objects: `c` is the close of an aggregate and the
conditions of a trade, `t` is the start of an aggregate window and the SIP time of a trade. A
flat key table cannot work, so the translation goes by **shape**:

- a `Lexicon` maps the short keys of one kind of object to the long names Massive itself uses
  for the same data where it spells them out (the trade lexicon takes `sip_timestamp` and
  `participant_timestamp` from `/v3/trades`, the quote lexicon `bid_price` from `/v3/quotes`,
  and so on); its `reason` names that source;
- `SHAPES` says which lexicon applies to which object: `(path, object prefix)`, where the
  prefix is the dotted wire key of the object within a row (`""` for the row itself,
  `"lastTrade"` for a snapshot's last trade);
- `IDENTITY` lists the short keys that are already Massive's long form (`id`, `url`, `low`),
  each with its reason.

The generator translates every segment of a wire key through the lexicon of the object it
sits in, else keeps an identity key or a long key as it is; a short key (three characters or
fewer, no underscore) that neither covers stops the refresh (gate G1). A lexicon or identity
entry is never derived from a description. A `SHAPES` entry naming an object the snapshot no
longer generates is stale and stops the refresh too (gate G4).

The tables are plain read-only data, built once at import.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = ["IDENTITY", "LEXICONS", "SHAPES", "Lexicon"]


@dataclass(frozen=True, kw_only=True)
class Lexicon:
    """The long names of the short keys of one kind of object.

    Attributes:
        name: The lexicon's name, which `SHAPES` refers to.
        reason: Where Massive spells these keys out, which is where the long names come from.
        entries: Short key to long name, such as `{"p": "price"}`. Copied into a read-only
            view.

    Example:
        ::

            from deephaven_massive._massive.refresh.lexicon import LEXICONS

            assert LEXICONS["trade"].entries["t"] == "sip_timestamp"
    """

    name: str
    reason: str
    entries: Mapping[str, str]

    def __post_init__(self) -> None:
        """Freeze a copy of the entries."""
        object.__setattr__(self, "entries", MappingProxyType(dict(self.entries)))


_AGGREGATE = {
    "T": "ticker",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
    "vw": "vwap",
    "n": "transactions",
    "t": "window_start",
}

_LEXICONS = (
    Lexicon(
        name="aggregate",
        reason="An aggregate bar. The long names are those of the flat-file aggregates and "
        "/futures/v1/aggs (`window_start`, `transactions`) and of the unified snapshot's "
        "bars (`vwap`).",
        entries=_AGGREGATE,
    ),
    Lexicon(
        name="stocks_bar",
        reason="A stocks snapshot bar: an aggregate bar plus the fractional volume, which the "
        "unified snapshot spells `decimal_volume`.",
        entries={**_AGGREGATE, "dv": "decimal_volume"},
    ),
    Lexicon(
        name="trade",
        reason="A stocks or options trade. The long names are those of /v3/trades/{stockTicker}.",
        entries={
            "T": "ticker",
            "c": "conditions",
            "ds": "decimal_size",
            "e": "correction",
            "f": "trf_timestamp",
            "i": "id",
            "p": "price",
            "q": "sequence_number",
            "r": "trf_id",
            "s": "size",
            "t": "sip_timestamp",
            "x": "exchange",
            "y": "participant_timestamp",
            "z": "tape",
        },
    ),
    Lexicon(
        name="quote",
        reason="A stocks NBBO quote. The long names are those of /v3/quotes/{stockTicker}.",
        entries={
            "T": "ticker",
            "P": "ask_price",
            "S": "ask_size",
            "X": "ask_exchange",
            "p": "bid_price",
            "s": "bid_size",
            "x": "bid_exchange",
            "c": "conditions",
            "i": "indicators",
            "f": "trf_timestamp",
            "q": "sequence_number",
            "t": "sip_timestamp",
            "y": "participant_timestamp",
            "z": "tape",
        },
    ),
    Lexicon(
        name="forex_quote",
        reason="A forex quote. The long names are those of the `last` object of "
        "/v1/last_quote/currencies/{from}/{to}.",
        entries={"a": "ask", "b": "bid", "t": "timestamp", "x": "exchange"},
    ),
    Lexicon(
        name="crypto_trade",
        reason="A crypto trade. The long names are those of the `last` object of "
        "/v1/last/crypto/{from}/{to}, and `id` of /v3/trades/{cryptoTicker}.",
        entries={
            "c": "conditions",
            "i": "id",
            "p": "price",
            "s": "size",
            "t": "timestamp",
            "x": "exchange",
        },
    ),
    Lexicon(
        name="snapshot",
        reason="A full-market or movers snapshot row, whose minute bar is `min`; the unified "
        "snapshot calls the same bar `last_minute`, and `minute` is its plain long form.",
        entries={"min": "minute"},
    ),
)

LEXICONS: Mapping[str, Lexicon] = MappingProxyType({lex.name: lex for lex in _LEXICONS})
"""Every lexicon, by name."""

IDENTITY: Mapping[str, str] = MappingProxyType(
    {
        "id": "An English word: the record's identifier.",
        "url": "An established abbreviation, spelled out nowhere by Massive.",
        "cik": "The SEC's Central Index Key, spelled `cik` throughout Massive's long names.",
        "mic": "The ISO 10383 Market Identifier Code, as in Massive's `operating_mic`.",
        "cfi": "The ISO 10962 CFI code of a contract.",
        "low": "An English word: the lowest price or value.",
        "ask": "An English word: the ask price, as in the forex `last` object.",
        "bid": "An English word: the bid price, as in the forex `last` object.",
        "day": "An English word: the session day's bar, or the requested date.",
        "fmv": "Fair market value, spelled `fmv` in Massive's `fmv_last_updated`.",
        "otc": "Over the counter, spelled `otc` throughout Massive's documentation.",
        "nav": "Net asset value, ETF Global's own name for the field.",
        "aum": "Assets under management, ETF Global's own name for the field.",
        "esg": "Environmental, social, and governance, ETF Global's own name for the field.",
        "cpi": "The Consumer Price Index, the Federal Reserve series' own name.",
        "pce": "Personal Consumption Expenditures, the Federal Reserve series' own name.",
        "to": "An English word: the currency converted to.",
        "fx": "Massive's own name for the forex market (`fx` in its URLs).",
        "cgi": "Cboe Global Indices, the index group's own acronym.",
        "CTA": "The Consolidated Tape Association, the SIP's own acronym.",
        "UTP": "The UTP Plan, the SIP's own acronym.",
    }
)
"""Short keys that are already long names, each with its reason."""


def _shapes(paths: Iterable[str], objects: Mapping[str, str]) -> dict[tuple[str, str], str]:
    """Return the `SHAPES` entries for several paths whose objects share lexicons.

    Args:
        paths: The path templates.
        objects: Object prefix to lexicon name.

    Returns:
        `(path, prefix)` to lexicon name, for every path and prefix.
    """
    return {(path, prefix): name for path in paths for prefix, name in objects.items()}


_BARS = (
    "/v2/aggs/grouped/locale/global/market/crypto/{date}",
    "/v2/aggs/grouped/locale/global/market/fx/{date}",
    "/v2/aggs/grouped/locale/us/market/stocks/{date}",
    *(
        f"/v2/aggs/ticker/{{{ticker}}}/{leaf}"
        for ticker in (
            "stocksTicker",
            "optionsTicker",
            "indicesTicker",
            "forexTicker",
            "cryptoTicker",
        )
        for leaf in ("prev", "range/{multiplier}/{timespan}/{from}/{to}")
    ),
)

SHAPES: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        **_shapes(_BARS, {"": "aggregate"}),
        **_shapes(
            ("/v2/last/trade/{stocksTicker}", "/v2/last/trade/{optionsTicker}"), {"": "trade"}
        ),
        **_shapes(("/v2/last/nbbo/{stocksTicker}",), {"": "quote"}),
        **_shapes(
            (
                "/v2/snapshot/locale/us/markets/stocks/tickers",
                "/v2/snapshot/locale/us/markets/stocks/{direction}",
            ),
            {
                "": "snapshot",
                "day": "stocks_bar",
                "min": "stocks_bar",
                "prevDay": "aggregate",
                "lastTrade": "trade",
                "lastQuote": "quote",
            },
        ),
        **_shapes(
            (
                "/v2/snapshot/locale/global/markets/forex/tickers",
                "/v2/snapshot/locale/global/markets/forex/{direction}",
            ),
            {
                "": "snapshot",
                "day": "aggregate",
                "min": "aggregate",
                "prevDay": "aggregate",
                "lastQuote": "forex_quote",
            },
        ),
        **_shapes(
            (
                "/v2/snapshot/locale/global/markets/crypto/tickers",
                "/v2/snapshot/locale/global/markets/crypto/{direction}",
            ),
            {
                "": "snapshot",
                "day": "aggregate",
                "min": "aggregate",
                "prevDay": "aggregate",
                "lastTrade": "crypto_trade",
            },
        ),
    }
)
"""`(path, object prefix)` to the lexicon of that object's keys."""
