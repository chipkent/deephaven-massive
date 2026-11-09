"""The lexicons that translate Massive's short wire keys into its own long names.

Some REST objects and every WebSocket message use one- and two-letter keys
(`{"p": 189.5, "s": 100, "t": ...}`), and one letter means different things in different
objects: `c` is the close of an aggregate and the conditions of a trade, `t` is the start of an
aggregate window and the SIP time of a trade, `z` is a trade's tape and an aggregate's average
trade size, `dv` is a stock bar's fractional volume and a futures bar's dollar volume. A flat
key table cannot work, so the translation goes by **shape**:

- a `Lexicon` maps the short keys of one kind of object to the long names Massive itself uses
  for the same fields elsewhere in that asset class's REST or flat-file data, and every entry
  cites where (the trade lexicon takes `sip_timestamp` from `/v3/trades/{stockTicker}`, the
  forex quote lexicon `ask_price` from `/v3/quotes/{fxTicker}`, and so on). So one field gets
  one column on every endpoint and transport;
- `SHAPES` says which lexicon applies to which object: `(source, object prefix)`, where the
  source is a REST path, a WebSocket `"<cluster>/<channel>"`, or a flat-file prefix, and the
  prefix is the dotted wire key of the object within a row (`""` for the row itself,
  `"lastTrade"` for a snapshot's last trade);
- `IDENTITY` lists the short keys that are already Massive's long form (`id`, `url`, `low`),
  each with its reason.

A lexicon's keys are Massive's abbreviations: mostly short keys (three characters or fewer,
no underscore), and a few longer ones the WebSocket trade uses (`trfi`, `trft`), which would
otherwise pass for long names. The generator translates every segment of a wire key through
the lexicon of the object it sits in, else keeps an identity key or a long key as it is; a
short key that neither covers stops the refresh (the `short-key` gate). A lexicon or
identity entry never takes a name from a description. A `SHAPES` or `IDENTITY` entry that
no generated object uses is stale and stops the refresh too (the `stale-entry` gate).

The REST path tuples at the end (`BAR_PATHS` and the snapshot paths) are shared with
`overrides`.

The tables are plain read-only data, built once at import.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = [
    "BAR_PATHS",
    "CRYPTO_SNAPSHOTS",
    "FOREX_SNAPSHOTS",
    "IDENTITY",
    "LEXICONS",
    "RANGE_PATHS",
    "SHAPES",
    "STOCKS_SNAPSHOTS",
    "Lexicon",
    "LongName",
]


@dataclass(frozen=True, kw_only=True)
class LongName:
    """The long name of one short key, and where Massive spells it out.

    Attributes:
        long_name: The long name in snake case, such as `"sip_timestamp"`.
        source: The vendor endpoint or file whose field carries that name for the same data,
            such as `"/v3/trades/{stockTicker}"`.
    """

    long_name: str
    source: str


@dataclass(frozen=True, kw_only=True)
class Lexicon:
    """The long names of the short keys of one kind of object.

    Attributes:
        name: The lexicon's name, which `SHAPES` refers to.
        reason: What kind of object the lexicon covers.
        entries: Short key to its long name and source. Copied into a read-only view.

    Example:
        ::

            from deephaven_massive._massive.refresh.lexicon import LEXICONS

            assert LEXICONS["trade"].entries["t"].long_name == "sip_timestamp"
    """

    name: str
    reason: str
    entries: Mapping[str, LongName]

    def __post_init__(self) -> None:
        """Freeze a copy of the entries."""
        object.__setattr__(self, "entries", MappingProxyType(dict(self.entries)))


def _from(source: str, **names: str) -> dict[str, LongName]:
    """Return lexicon entries that all cite one source.

    Args:
        source: The vendor endpoint or file that spells the names out.
        **names: Short key to long name. A key that is a Python keyword is written with a
            trailing underscore (`as_` for the WebSocket ask size `as`), which is removed.

    Returns:
        Short key to `LongName`.
    """
    return {
        short.removesuffix("_"): LongName(long_name=name, source=source)
        for short, name in names.items()
    }


_UNIFIED_BAR = "/v3/snapshot `last_minute` and `session` (the unified snapshot)"
# Crypto's one REST quote: the unified snapshot's last quote, documented for crypto.
_CRYPTO_LAST_QUOTE = "/v3/snapshot `last_quote` (the crypto unified snapshot)"
# Every flat file names its instrument `ticker`, as does the unified snapshot.
_TICKER = "/v3/snapshot `ticker`, and the `ticker` column of every flat file"
_AGGREGATE = {
    **_from(_TICKER, T="ticker", sym="ticker"),
    **_from(_UNIFIED_BAR, o="open", h="high", l="low", c="close", v="volume", vw="vwap"),
    **_from("/v3/snapshot `last_minute.transactions`", n="transactions"),
    # The day's volume-weighted average price: the `vwap` of the snapshots' `day` bar
    # (`/v3/snapshot/options/{underlyingAsset}` `day.vwap`, the v2 snapshots' `day.vw`).
    **_from("/v3/snapshot/options/{underlyingAsset} `day.vwap`", a="day_vwap"),
    **_from(
        "/futures/v1/aggs and every `*_aggs_v1` flat file `window_start`",
        t="window_start",
        s="window_start",
    ),
}

_LEXICONS = (
    Lexicon(name="aggregate", reason="An aggregate bar of any asset class.", entries=_AGGREGATE),
    Lexicon(
        name="stocks_bar",
        reason="A stocks snapshot bar: an aggregate bar plus its fractional volume.",
        entries={
            **_AGGREGATE,
            **_from("/v3/snapshot `last_minute.decimal_volume`", dv="decimal_volume"),
        },
    ),
    Lexicon(
        name="futures_bar",
        reason="A futures aggregate bar: an aggregate bar plus its dollar volume.",
        entries={
            **_AGGREGATE,
            **_from(
                "/futures/v1/aggs/{ticker} and us_futures_{venue}/minute_aggs_v1 `dollar_volume`",
                dv="dollar_volume",
            ),
        },
    ),
    Lexicon(
        name="trade",
        reason="A stocks or options trade, from the REST last trade or the WebSocket.",
        entries={
            **_from(_TICKER, T="ticker", sym="ticker"),
            **_from(
                "/v3/trades/{stockTicker}",
                c="conditions",
                ds="decimal_size",
                e="correction",
                f="trf_timestamp",
                i="id",
                p="price",
                q="sequence_number",
                r="trf_id",
                s="size",
                t="sip_timestamp",
                x="exchange",
                y="participant_timestamp",
                z="tape",
                pt="participant_timestamp",
                trfi="trf_id",
                trft="trf_timestamp",
            ),
        },
    ),
    Lexicon(
        name="quote",
        reason="A stocks or options quote, from the REST last NBBO or the WebSocket.",
        entries={
            **_from(_TICKER, T="ticker", sym="ticker"),
            **_from(
                "/v3/quotes/{stockTicker}",
                P="ask_price",
                S="ask_size",
                X="ask_exchange",
                p="bid_price",
                s="bid_size",
                x="bid_exchange",
                c="conditions",
                i="indicators",
                f="trf_timestamp",
                q="sequence_number",
                t="sip_timestamp",
                y="participant_timestamp",
                z="tape",
                ap="ask_price",
                as_="ask_size",
                ax="ask_exchange",
                bp="bid_price",
                bs="bid_size",
                bx="bid_exchange",
                pt="participant_timestamp",
            ),
        },
    ),
    Lexicon(
        name="forex_quote",
        reason="A forex quote, from a REST snapshot or the WebSocket.",
        entries={
            **_from(
                "/v3/quotes/{fxTicker}",
                a="ask_price",
                b="bid_price",
                t="participant_timestamp",
            ),
            **_from("/v1/last_quote/currencies/{from}/{to} `last.exchange`", x="exchange"),
            **_from("global_forex/quotes_v1 `ticker`", p="ticker"),
        },
    ),
    Lexicon(
        name="crypto_trade",
        reason="A crypto trade, from a REST snapshot or the WebSocket.",
        entries=_from(
            "/v3/trades/{cryptoTicker}",
            c="conditions",
            i="id",
            p="price",
            s="size",
            t="participant_timestamp",
            x="exchange",
            r="received_timestamp",
        ),
    ),
    Lexicon(
        name="crypto_quote",
        reason="A crypto quote from the WebSocket. Crypto's own REST data has one quote, the "
        "unified snapshot's `last_quote`, which names the prices and sizes; the times and "
        "the exchange take the crypto trade's names.",
        entries={
            **_from(
                "/v3/trades/{cryptoTicker}",
                t="participant_timestamp",
                x="exchange",
                r="received_timestamp",
            ),
            **_from(_CRYPTO_LAST_QUOTE, ap="ask", bp="bid", as_="ask_size", bs="bid_size"),
        },
    ),
    Lexicon(
        name="futures_trade",
        reason="A futures trade from the WebSocket.",
        entries={
            **_from(_TICKER, sym="ticker"),
            **_from(
                "/futures/v1/trades/{ticker}",
                p="price",
                s="size",
                t="timestamp",
                q="sequence_number",
            ),
        },
    ),
    Lexicon(
        name="futures_quote",
        reason="A futures quote from the WebSocket.",
        entries={
            **_from(_TICKER, sym="ticker"),
            **_from(
                "/futures/v1/quotes/{ticker}",
                ap="ask_price",
                as_="ask_size",
                at="ask_timestamp",
                bp="bid_price",
                bs="bid_size",
                bt="bid_timestamp",
                t="timestamp",
            ),
        },
    ),
    Lexicon(
        name="index_value",
        reason="An index value from the WebSocket.",
        entries=_from("us_indices/values_v1", T="ticker", val="value", t="timestamp"),
    ),
    Lexicon(
        name="fair_market_value",
        reason="A fair-market-value tick from the WebSocket.",
        entries={
            **_from(_TICKER, sym="ticker"),
            **_from(
                "/v3/snapshot `fmv_last_updated`, the time of the fair market value",
                t="fmv_last_updated",
            ),
        },
    ),
    Lexicon(
        name="luld",
        reason="A limit up-limit down band from the WebSocket.",
        entries={
            **_from(_TICKER, T="ticker"),
            **_from("/v3/quotes/{stockTicker}", i="indicators", q="sequence_number", z="tape"),
        },
    ),
    Lexicon(
        name="imbalance",
        reason="A net order imbalance from the WebSocket.",
        entries={
            **_from(_TICKER, T="ticker"),
            **_from("/v3/trades/{stockTicker}", x="exchange"),
        },
    ),
    Lexicon(
        name="options_admin",
        reason="An options administrative message from the WebSocket, such as a FLEX trade notice.",
        entries=_from(
            "/v3/trades/{optionsTicker}",
            q="sequence_number",
            t="sip_timestamp",
            x="exchange",
        ),
    ),
    Lexicon(
        name="snapshot",
        reason="A full-market or movers snapshot row.",
        entries=_from(
            "/v3/snapshot and /futures/v1/snapshot `last_minute`, the same most-recent minute bar",
            min="last_minute",
        ),
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
        "ask": "An English word: the ask price, as the forex `last` object spells it.",
        "bid": "An English word: the bid price, as the forex `last` object spells it.",
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


BAR_PATHS: tuple[str, ...] = (
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
"""The aggregate paths, whose rows are aggregate bars."""

RANGE_PATHS: tuple[str, ...] = tuple(path for path in BAR_PATHS if "/range/" in path)
"""The aggregate range paths, whose `{from}` and `{to}` bound the bars."""

STOCKS_SNAPSHOTS: tuple[str, ...] = (
    "/v2/snapshot/locale/us/markets/stocks/tickers",
    "/v2/snapshot/locale/us/markets/stocks/{direction}",
)
"""The stocks full-market and movers snapshots."""

FOREX_SNAPSHOTS: tuple[str, ...] = (
    "/v2/snapshot/locale/global/markets/forex/tickers",
    "/v2/snapshot/locale/global/markets/forex/{direction}",
)
"""The forex full-market and movers snapshots."""

CRYPTO_SNAPSHOTS: tuple[str, ...] = (
    "/v2/snapshot/locale/global/markets/crypto/tickers",
    "/v2/snapshot/locale/global/markets/crypto/{direction}",
)
"""The crypto full-market and movers snapshots."""

SHAPES: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        **_shapes(BAR_PATHS, {"": "aggregate"}),
        **_shapes(
            ("/v2/last/trade/{stocksTicker}", "/v2/last/trade/{optionsTicker}"), {"": "trade"}
        ),
        **_shapes(("/v2/last/nbbo/{stocksTicker}",), {"": "quote"}),
        **_shapes(
            STOCKS_SNAPSHOTS,
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
            FOREX_SNAPSHOTS,
            {
                "": "snapshot",
                "day": "aggregate",
                "min": "aggregate",
                "prevDay": "aggregate",
                "lastQuote": "forex_quote",
            },
        ),
        **_shapes(
            CRYPTO_SNAPSHOTS,
            {
                "": "snapshot",
                "day": "aggregate",
                "min": "aggregate",
                "prevDay": "aggregate",
                "lastTrade": "crypto_trade",
            },
        ),
        **_shapes(("stocks/AM", "stocks/A"), {"": "stocks_bar"}),
        **_shapes(("futures/AM", "futures/A"), {"": "futures_bar"}),
        **_shapes(
            (
                "options/AM",
                "options/A",
                "indices/AM",
                "indices/A",
                "forex/CA",
                "forex/CAS",
                "crypto/XA",
                "crypto/XAS",
            ),
            {"": "aggregate"},
        ),
        **_shapes(("stocks/T", "options/T"), {"": "trade"}),
        **_shapes(("stocks/Q", "options/Q"), {"": "quote"}),
        **_shapes(("forex/C",), {"": "forex_quote"}),
        **_shapes(("crypto/XT",), {"": "crypto_trade"}),
        **_shapes(("crypto/XQ",), {"": "crypto_quote"}),
        **_shapes(("futures/T",), {"": "futures_trade"}),
        **_shapes(("futures/Q",), {"": "futures_quote"}),
        **_shapes(("indices/V",), {"": "index_value"}),
        **_shapes(
            ("stocks/FMV", "options/FMV", "forex/FMV", "crypto/FMV"), {"": "fair_market_value"}
        ),
        **_shapes(("stocks/LULD",), {"": "luld"}),
        **_shapes(("stocks/NOI",), {"": "imbalance"}),
        **_shapes(("options/AD",), {"": "options_admin"}),
    }
)
"""`(source, object prefix)` to the lexicon of that object's keys."""
