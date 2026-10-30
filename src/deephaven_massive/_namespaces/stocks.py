"""The `m.stocks` namespace: US equities.

Datasets: trades, quotes, bars, LULD bands, net order imbalances, fair market value,
snapshots, movers, daily open/close, dividends, splits, IPOs, short interest and volume, share
float, and the nested `m.stocks.financials` (`_StockFinancials`) and `m.stocks.filings`
(`_StockFilings`) namespaces.

Each dataset is one factory method that validates its arguments and returns a query; nothing
touches the network until a verb is called on the query. This module owns the namespace's
`DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog (`CATALOG` in
`_session.py`). A spec names the dataset's generated sources, its key columns per source, the
wire parameters behind `start`, `end`, and `symbols`, its `poll()` defaults, and the REST page
size (the largest `limit` the endpoint documents); columns, types, and units come from the
generated schemas.

The REST list datasets (dividends through the filings) share one shape: `symbols` is optional
and only filters the request, `dm.ALL` is refused in favour of `symbols=None`, `start` and
`end` bound the dataset's date axis where it has one, and the `sort`, `order`, `limit`, and
`filters` block works as described in the package documentation. The rows of the three financial
statements, Forms 3 and 4, the insider activity and holdings, and the 8-K disclosures each list
several tickers, so those datasets have no `Symbol` column and keep `Tickers`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, Literal, cast, overload

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.dates import calendar_day
from deephaven_massive._datasets.row_times import RowTimes
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    Direction,
    NotBars,
    Order,
    PollMode,
    Timeframe,
    Timespan,
    _require_enum,
)
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import (
    Namespace,
    adjustment_notes,
    build_query,
    list_params,
    one_or_any_of,
    optional_text,
    require_bool,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._query import (
        HistoryLatestQuery,
        HistoryQuery,
        Query,
        StreamHistoryLatestQuery,
        StreamHistoryQuery,
        StreamQuery,
    )
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike, DaySpan, FilterValue, StreamSpan, Symbols

__all__ = ["DATASETS", "_StockFilings", "_StockFinancials", "_Stocks"]

logger = logging.getLogger(__name__)

_R: Final = Transport.REST
_W: Final = Transport.WS
_F: Final = Transport.FLAT_FILES
_STOCKS: Final = AssetClass.STOCKS

_EVERY_TICKER: Final = "symbols=None, which returns every ticker"
"""The alternative to `dm.ALL` that the REST list datasets name in their `ValueError`."""


def _row(column: str) -> KeySpec:
    """Return a key the source's rows carry in `column`."""
    return KeySpec(column)


def _request(parameter: str) -> KeySpec:
    """Return a key filled from the request parameter `parameter`."""
    return KeySpec(parameter, from_request=True)


def _ticker_keys(time: str | None = None) -> SourceKeys:
    """Return the keys of a source whose rows name their stock in `Ticker`."""
    return SourceKeys(symbol=_row("Ticker"), time=None if time is None else _row(time))


def _stream_bar_keys() -> SourceKeys:
    """Return the keys of a WebSocket bars channel: its ticker, window start, and end `e`."""
    return SourceKeys(symbol=_row("Ticker"), time=_row("WindowStart"), end=_row("EndTimestamp"))


# --- Market data ------------------------------------------------------------------------------

_LAST_TRADE: Final = "/v2/last/trade/{stocksTicker}"
_TRADES: Final = "/v3/trades/{stockTicker}"
_LAST_QUOTE: Final = "/v2/last/nbbo/{stocksTicker}"
_QUOTES: Final = "/v3/quotes/{stockTicker}"
_PREV: Final = "/v2/aggs/ticker/{stocksTicker}/prev"
_RANGE: Final = "/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}"
_GROUPED: Final = "/v2/aggs/grouped/locale/us/market/stocks/{date}"
_MINUTE_FILES: Final = "us_stocks_sip/minute_aggs_v1"
_DAY_FILES: Final = "us_stocks_sip/day_aggs_v1"

TRADES: Final = DatasetSpec(
    name="stocks.trades",
    summary="Trades of US-listed stocks, tick by tick.",
    asset_class=_STOCKS,
    kind="trades",
    fetch_path=_LAST_TRADE,
    history_path=_TRADES,
    channels={NotBars.ANY: "T"},
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/trades_v1"},
    keys={
        (_R, _LAST_TRADE): _ticker_keys("SipTimestamp"),
        (_R, _TRADES): SourceKeys(symbol=_request("stockTicker"), time=_row("SipTimestamp")),
        (_W, "stocks/T"): _ticker_keys("SipTimestamp"),
        (_F, "us_stocks_sip/trades_v1"): _ticker_keys("SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    page_size=50_000,
)
QUOTES: Final = DatasetSpec(
    name="stocks.quotes",
    summary="National best bid and offer quotes of US-listed stocks, tick by tick.",
    asset_class=_STOCKS,
    kind="quotes",
    fetch_path=_LAST_QUOTE,
    history_path=_QUOTES,
    channels={NotBars.ANY: "Q"},
    flat_file_prefixes={NotBars.ANY: "us_stocks_sip/quotes_v1"},
    keys={
        (_R, _LAST_QUOTE): _ticker_keys("SipTimestamp"),
        (_R, _QUOTES): SourceKeys(symbol=_request("stockTicker"), time=_row("SipTimestamp")),
        (_W, "stocks/Q"): _ticker_keys("SipTimestamp"),
        (_F, "us_stocks_sip/quotes_v1"): _ticker_keys("SipTimestamp"),
    },
    time_axis=("timestamp.gte", "timestamp.lt"),
    page_size=50_000,
)
BARS: Final = DatasetSpec(
    name="stocks.bars",
    summary="Aggregate bars (open, high, low, close, volume) of US-listed stocks.",
    asset_class=_STOCKS,
    is_bars=True,
    kind="bars",
    fetch_path=_PREV,
    history_path=_RANGE,
    grouped_daily_path=_GROUPED,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    flat_file_prefixes={Timespan.MINUTE: _MINUTE_FILES, Timespan.DAY: _DAY_FILES},
    keys={
        (_R, _PREV): _ticker_keys("WindowStart"),
        (_R, _RANGE): SourceKeys(symbol=_request("stocksTicker"), time=_row("WindowStart")),
        (_R, _GROUPED): _ticker_keys("WindowStart"),
        (_W, "stocks/A"): _stream_bar_keys(),
        (_W, "stocks/AM"): _stream_bar_keys(),
        (_F, _MINUTE_FILES): _ticker_keys("WindowStart"),
        (_F, _DAY_FILES): _ticker_keys("WindowStart"),
    },
    page_size=50_000,
    column_notes=adjustment_notes(("Open", "High", "Low", "Close", "Vwap"), ("Volume",)),
)
LULD: Final = DatasetSpec(
    name="stocks.luld",
    summary="Limit up-limit down price bands of US-listed stocks, as they change.",
    asset_class=_STOCKS,
    kind="luld",
    channels={NotBars.ANY: "LULD"},
    keys={(_W, "stocks/LULD"): _ticker_keys("UnixTimestamp")},
)
IMBALANCES: Final = DatasetSpec(
    name="stocks.imbalances",
    summary="Net order imbalances of US-listed stocks ahead of opening and closing auctions.",
    asset_class=_STOCKS,
    kind="imbalances",
    channels={NotBars.ANY: "NOI"},
    keys={(_W, "stocks/NOI"): _ticker_keys("UnixTimestamp")},
)
FMV: Final = DatasetSpec(
    name="stocks.fmv",
    summary="Massive's fair market value estimate of US-listed stocks, as it updates.",
    asset_class=_STOCKS,
    kind="fmv",
    channels={NotBars.ANY: "FMV"},
    family=ChannelFamily.FMV,
    keys={(_W, "stocks/FMV"): _ticker_keys("FmvLastUpdated")},
)

# --- Snapshots and daily summaries -------------------------------------------------------------

_SNAPSHOTS: Final = "/v2/snapshot/locale/us/markets/stocks/tickers"
_MOVERS: Final = "/v2/snapshot/locale/us/markets/stocks/{direction}"
_OPEN_CLOSE: Final = "/v1/open-close/{stocksTicker}/{date}"

# The snapshots declare no time key: a ticker that has not traded can carry `updated` 0,
# which as the declared time column would fail the whole response instead of reading as null.
# The tickers travel comma-separated in the URL, so a long watchlist is split into requests of
# 250 symbols, the size Massive's unified snapshot documents as its limit, to keep each URL
# well under the lengths servers refuse.
SNAPSHOTS: Final = DatasetSpec(
    name="stocks.snapshots",
    summary="Current market snapshots of US-listed stocks, one row per ticker.",
    asset_class=_STOCKS,
    fetch_path=_SNAPSHOTS,
    keys={(_R, _SNAPSHOTS): _ticker_keys()},
    symbol_param="tickers",
    symbols_per_request=250,
    revision_keys=("Updated",),
)
MOVERS: Final = DatasetSpec(
    name="stocks.movers",
    summary="The twenty US stocks with the largest percentage move today, up or down.",
    asset_class=_STOCKS,
    fetch_path=_MOVERS,
    keys={(_R, _MOVERS): _ticker_keys()},
    revision_keys=("Updated",),
)
DAILY_OPEN_CLOSE: Final = DatasetSpec(
    name="stocks.daily_open_close",
    summary="The open, close, and pre- and after-market prices of US stocks on one day.",
    asset_class=_STOCKS,
    fetch_path=_OPEN_CLOSE,
    keys={(_R, _OPEN_CLOSE): SourceKeys(symbol=_row("Symbol"), date=_row("From"))},
    per_symbol=True,
    row_times=RowTimes.DAY,
    column_notes=adjustment_notes(
        ("Open", "High", "Low", "Close", "PreMarket", "AfterHours"), ("Volume",)
    ),
)

# --- Corporate actions and short data ----------------------------------------------------------

_DIVIDENDS: Final = "/stocks/v1/dividends"
_SPLITS: Final = "/stocks/v1/splits"
_IPOS: Final = "/vX/reference/ipos"
_SHORT_INTEREST: Final = "/stocks/v1/short-interest"
_SHORT_VOLUME: Final = "/stocks/v1/short-volume"
_FLOAT: Final = "/stocks/vX/float"

DIVIDENDS: Final = DatasetSpec(
    name="stocks.dividends",
    summary="Cash dividends of US-listed stocks, one row per distribution.",
    asset_class=_STOCKS,
    fetch_path=_DIVIDENDS,
    keys={(_R, _DIVIDENDS): _ticker_keys()},
    time_axis=("ex_dividend_date.gte", "ex_dividend_date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Id",),
    page_size=5_000,
)
SPLITS: Final = DatasetSpec(
    name="stocks.splits",
    summary="Stock splits, reverse splits, and stock dividends of US-listed stocks.",
    asset_class=_STOCKS,
    fetch_path=_SPLITS,
    keys={(_R, _SPLITS): _ticker_keys()},
    time_axis=("execution_date.gte", "execution_date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Id",),
    page_size=5_000,
)
# The IPO path filters one `ticker` per request, so the REST layer sends one request per symbol.
IPOS: Final = DatasetSpec(
    name="stocks.ipos",
    summary="Initial public offerings on US exchanges, upcoming and past, one row per offering.",
    asset_class=_STOCKS,
    fetch_path=_IPOS,
    keys={(_R, _IPOS): _ticker_keys()},
    time_axis=("listing_date.gte", "listing_date.lte"),
    symbol_param="ticker",
    poll_mode=PollMode.UPSERT,
    # One offering is one issuer listing under one ticker; both are required on every row, and
    # a ticker an earlier, different issuer used (a recycled SPAC ticker) stays apart. The key
    # assumes Massive keeps an offering's issuer name unchanged through its statuses, which no
    # vendor source states: a renamed issuer would leave its old row behind. The listing date
    # and ISIN are no key: an offering can be postponed to a new date, and either can be
    # missing before the listing.
    poll_keys=("Symbol", "IssuerName"),
    page_size=1_000,
)
SHORT_INTEREST: Final = DatasetSpec(
    name="stocks.short_interest",
    summary="Short interest of US-listed stocks, one row per ticker and FINRA settlement date.",
    asset_class=_STOCKS,
    fetch_path=_SHORT_INTEREST,
    keys={(_R, _SHORT_INTEREST): _ticker_keys()},
    time_axis=("settlement_date.gte", "settlement_date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Symbol", "SettlementDate"),
    page_size=50_000,
)
SHORT_VOLUME: Final = DatasetSpec(
    name="stocks.short_volume",
    summary="Daily short sale volume of US-listed stocks, one row per ticker and trading day.",
    asset_class=_STOCKS,
    fetch_path=_SHORT_VOLUME,
    keys={(_R, _SHORT_VOLUME): _ticker_keys()},
    time_axis=("date.gte", "date.lte"),
    symbol_param="ticker.any_of",
    poll_keys=("Symbol", "Date"),
    page_size=50_000,
)
SHARE_FLOAT: Final = DatasetSpec(
    name="stocks.share_float",
    summary="The free float of US-listed stocks: the latest count of shares the public can trade.",
    asset_class=_STOCKS,
    fetch_path=_FLOAT,
    keys={(_R, _FLOAT): _ticker_keys()},
    symbol_param="ticker.any_of",
    page_size=5_000,
)

# --- Financials ---------------------------------------------------------------------------------

_STATEMENT_KEYS: Final = ("Cik", "PeriodEnd", "Timeframe")
"""One filer's statement for one period: the `poll()` key of the three statements."""


def _statement(dataset: str, path: str, *, summary: str) -> DatasetSpec:
    """Return the spec of one financial statement: no `Symbol`, axis `period_end`.

    Args:
        dataset: The dataset name below `stocks.financials`, such as `"balance_sheets"`.
        path: The statement's REST path.
        summary: The dataset's one-sentence summary.

    Returns:
        The spec.
    """
    return DatasetSpec(
        name=f"stocks.financials.{dataset}",
        summary=summary,
        asset_class=_STOCKS,
        fetch_path=path,
        time_axis=("period_end.gte", "period_end.lte"),
        symbol_param="tickers.any_of",
        poll_keys=_STATEMENT_KEYS,
        # Below the documented 50,000: a statement row holds some fifty line items, over a
        # kilobyte of JSON, so a full page would pass 60 MB; 10,000 keeps it near 15 MB.
        page_size=10_000,
    )


BALANCE_SHEETS: Final = _statement(
    "balance_sheets",
    "/stocks/financials/v1/balance-sheets",
    summary="Balance sheets of US public companies, one row per filer and reporting period.",
)
INCOME_STATEMENTS: Final = _statement(
    "income_statements",
    "/stocks/financials/v1/income-statements",
    summary="Income statements of US public companies, one row per filer and reporting period.",
)
CASH_FLOW_STATEMENTS: Final = _statement(
    "cash_flow_statements",
    "/stocks/financials/v1/cash-flow-statements",
    summary="Cash flow statements of US public companies, one row per filer and reporting period.",
)
_RATIOS: Final = "/stocks/financials/v1/ratios"
RATIOS: Final = DatasetSpec(
    name="stocks.financials.ratios",
    summary="Daily valuation, profitability, liquidity, and leverage ratios of US stocks.",
    asset_class=_STOCKS,
    fetch_path=_RATIOS,
    keys={(_R, _RATIOS): _ticker_keys()},
    symbol_param="ticker.any_of",
    poll_keys=("Symbol", "Date"),
    page_size=50_000,
)

# --- Filings ------------------------------------------------------------------------------------

_FILING_AXIS: Final = ("filing_date.gte", "filing_date.lte")
_INDEX: Final = "/stocks/filings/vX/index"
_SECTIONS_10K: Final = "/stocks/filings/10-K/vX/sections"
_TEXT_8K: Final = "/stocks/filings/8-K/vX/text"
_DISCLOSURES_8K: Final = "/stocks/filings/8-K/vX/disclosures"
_RISK_FACTORS: Final = "/stocks/filings/vX/risk-factors"
_FORM_13F: Final = "/stocks/filings/vX/13-F"
_FORM_3: Final = "/stocks/filings/vX/form-3"
_FORM_4: Final = "/stocks/filings/vX/form-4"
_INSIDER_ACTIVITY: Final = "/stocks/insiders/vX/activity"
_INSIDER_HOLDINGS: Final = "/stocks/insiders/vX/holdings"
_RISK_TAXONOMY: Final = "/stocks/taxonomies/vX/risk-factors"
_DISCLOSURE_TAXONOMY: Final = "/stocks/taxonomies/vX/disclosures"
_CATEGORIES: Final = ("PrimaryCategory", "SecondaryCategory", "TertiaryCategory")
_INSIDER_HOLDING: Final = (
    "AccessionNumber",
    "OwnerCik",
    "SecurityType",
    "SecurityTitle",
    "DirectOrIndirect",
    "NatureOfOwnership",
)
"""One owner's holding of one security in one Form 3 or Form 4 filing."""

_DERIVATIVE_ONLY: Final = "set for derivative securities only"
_INDIRECT: Final = {"NatureOfOwnership": "the nature of indirect ownership, empty for direct"}
_DERIVATIVE: Final = {"ExercisePrice": _DERIVATIVE_ONLY}


def _filing(
    dataset: str,
    path: str,
    *,
    summary: str,
    symbol_param: str | None,
    poll_keys: tuple[str, ...],
    nullable_poll_keys: Mapping[str, str] | None = None,
    keys: SourceKeys | None = None,
    page_size: int,
) -> DatasetSpec:
    """Return the spec of one filings dataset: axis `filing_date`, appended when polled.

    Args:
        dataset: The dataset name below `stocks.filings`, such as `"index"`.
        path: The REST path.
        summary: The dataset's one-sentence summary.
        symbol_param: The parameter `symbols` is sent in, or None for a dataset without one.
        poll_keys: The columns that identify one row, the key of a keyed `poll()` (a mode
            other than the default).
        nullable_poll_keys: The poll keys the vendor leaves empty on some rows, each with
            its reason; a null there is a value, not a data failure. None for no such key.
        keys: The source's keys; by default the optional `Ticker` of `_OPTIONAL_TICKER`.
        page_size: The largest `limit` the path documents, or less for rows that carry long
            text.

    Returns:
        The spec.
    """
    return DatasetSpec(
        name=f"stocks.filings.{dataset}",
        summary=summary,
        asset_class=_STOCKS,
        fetch_path=path,
        keys={(_R, path): _OPTIONAL_TICKER if keys is None else keys},
        time_axis=_FILING_AXIS,
        symbol_param=symbol_param,
        poll_mode=PollMode.APPEND,
        poll_keys=poll_keys,
        nullable_poll_keys={} if nullable_poll_keys is None else nullable_poll_keys,
        page_size=page_size,
    )


_NO_KEYS: Final = SourceKeys()

_OPTIONAL_TICKER: Final = SourceKeys(symbol=KeySpec("Ticker", required=False))
"""The symbol key of a filing whose rows carry one `ticker` that Massive does not promise.

No filings response schema marks `ticker` required, and the index documents it as present
only "if available": a filer with no listed security has none. Such a row keeps a null
`Symbol` rather than failing the whole request."""

FILINGS_INDEX: Final = _filing(
    "index",
    _INDEX,
    symbol_param="ticker.any_of",
    # EDGAR's index lists a filing once under each filer (a Form 4 under the issuer and each
    # reporting owner, a joint filing under each co-registrant), and so can this one.
    poll_keys=("Cik", "AccessionNumber"),
    page_size=10_000,
    summary="The index of SEC EDGAR filings by US-listed companies, one row per filing and filer.",
)
SECTIONS_10K: Final = _filing(
    "sections_10k",
    _SECTIONS_10K,
    symbol_param="ticker.any_of",
    poll_keys=("FilingUrl", "Section"),
    page_size=100,
    summary="The text of sections of 10-K annual reports, one row per filing and section.",
)
TEXT_8K: Final = _filing(
    "text_8k",
    _TEXT_8K,
    symbol_param="ticker.any_of",
    # A joint 8-K of several co-registrants can come once under each filer's CIK, as the
    # index lists it.
    poll_keys=("Cik", "AccessionNumber"),
    page_size=100,
    summary="The item text of 8-K current reports, one row per filing and filer.",
)
DISCLOSURES_8K: Final = _filing(
    "disclosures_8k",
    _DISCLOSURES_8K,
    symbol_param="tickers.any_of",
    poll_keys=("AccessionNumber", *_CATEGORIES, "SupportingText"),
    keys=_NO_KEYS,
    page_size=1_000,
    summary="Disclosures in 8-K current reports, classified by category, one row per disclosure.",
)
RISK_FACTORS: Final = _filing(
    "risk_factors",
    _RISK_FACTORS,
    symbol_param="ticker.any_of",
    poll_keys=("Cik", "FilingDate", *_CATEGORIES, "SupportingText"),
    # Below the documented 50,000: each row quotes the filing's text for its risk factor, so a
    # page of 5,000 stays at a few megabytes.
    page_size=5_000,
    summary="Risk factors disclosed in company filings, classified by category, one row each.",
)
_13F_AMOUNTS: Final = (
    "SharesOrPrincipalAmount",
    "SharesOrPrincipalType",
    "VotingAuthoritySole",
    "VotingAuthorityShared",
    "VotingAuthorityNone",
)
"""The amount and voting authority of one 13-F line, which tell apart several lines of one
filing for the same security."""

INSTITUTIONAL_HOLDINGS: Final = _filing(
    "institutional_holdings",
    _FORM_13F,
    symbol_param=None,
    poll_keys=(
        "AccessionNumber",
        "Cusip",
        "TitleOfClass",
        "PutCall",
        "InvestmentDiscretion",
        "OtherManagers",
        # One filing can list the same security on several lines, split by amount or voting
        # authority. The market value follows from the amount, so it tells no lines apart.
        *_13F_AMOUNTS,
    ),
    nullable_poll_keys={
        "PutCall": "PUT or CALL for an option, and empty for common stock",
        "OtherManagers": "the other managers sharing discretion, only if applicable",
        **dict.fromkeys(_13F_AMOUNTS, "Massive marks the field optional"),
    },
    keys=SourceKeys(fanout=KeySpec("FilerCik", instrument=False)),
    page_size=1_000,
    summary="Holdings that institutional managers report on Form 13-F, one row per position.",
)
INITIAL_INSIDER_OWNERSHIP: Final = _filing(
    "initial_insider_ownership",
    _FORM_3,
    symbol_param="tickers.any_of",
    poll_keys=(*_INSIDER_HOLDING, "ExercisePrice", "ExerciseDate"),
    nullable_poll_keys={**_INDIRECT, **_DERIVATIVE, "ExerciseDate": _DERIVATIVE_ONLY},
    keys=_NO_KEYS,
    page_size=10_000,
    summary="Initial insider ownership statements (SEC Form 3), one row per reported holding.",
)
INSIDER_TRANSACTIONS: Final = _filing(
    "insider_transactions",
    _FORM_4,
    symbol_param="tickers.any_of",
    poll_keys=(
        *_INSIDER_HOLDING,
        "RecordType",
        "TransactionDate",
        "TransactionCode",
        "TransactionShares",
        "TransactionPricePerShare",
        "TransactionAcquiredDisposed",
        "SharesOwnedFollowingTransaction",
        "ExercisePrice",
        "ExerciseDate",
        "ExpirationDate",
    ),
    nullable_poll_keys={
        **_INDIRECT,
        **_DERIVATIVE,
        "ExerciseDate": _DERIVATIVE_ONLY,
        "ExpirationDate": _DERIVATIVE_ONLY,
        "SharesOwnedFollowingTransaction": "the SEC's Form 4 instructions let a filer report "
        "the value owned afterwards instead of the shares (valueOwnedFollowingTransaction in "
        "the EDGAR Form 4 XML technical specification), and the row does not carry that value",
        **dict.fromkeys(
            (
                "TransactionDate",
                "TransactionCode",
                "TransactionShares",
                "TransactionPricePerShare",
                "TransactionAcquiredDisposed",
            ),
            "a 'holding' record (RecordType) reports no transaction",
        ),
    },
    keys=_NO_KEYS,
    page_size=10_000,
    summary="Insider transactions (SEC Form 4), one row per reported transaction or holding.",
)
INSIDER_ACTIVITY: Final = DatasetSpec(
    name="stocks.filings.insider_activity",
    summary="Open-market insider purchases and sales, summed per company and transaction day.",
    asset_class=_STOCKS,
    fetch_path=_INSIDER_ACTIVITY,
    keys={(_R, _INSIDER_ACTIVITY): _NO_KEYS},
    time_axis=("transaction_date.gte", "transaction_date.lte"),
    symbol_param="tickers.any_of",
    # One row sums every insider of one issuer on one transaction day. A filing that arrives
    # later for the same day revises that row's totals and `LastFilingDate`, so the row is
    # replaced, not appended.
    poll_keys=("IssuerCik", "TransactionDate"),
    page_size=10_000,
)
INSIDER_HOLDINGS: Final = DatasetSpec(
    name="stocks.filings.insider_holdings",
    summary="Each company's insiders and the shares each one holds now, one row per insider.",
    asset_class=_STOCKS,
    fetch_path=_INSIDER_HOLDINGS,
    keys={(_R, _INSIDER_HOLDINGS): _NO_KEYS},
    time_axis=("last_filing_date.gte", "last_filing_date.lte"),
    symbol_param="tickers.any_of",
    # One row is one reporting owner's current position in one issuer; each new filing by
    # that owner updates the row in place.
    poll_keys=("IssuerCik", "OwnerCik"),
    page_size=10_000,
)
RISK_FACTOR_TAXONOMY: Final = DatasetSpec(
    name="stocks.filings.risk_factor_taxonomy",
    summary="The taxonomy that classifies filing risk factors, one row per category.",
    fetch_path=_RISK_TAXONOMY,
    poll_keys=("Taxonomy", *_CATEGORIES),
    page_size=1_000,
)
DISCLOSURE_TAXONOMY: Final = DatasetSpec(
    name="stocks.filings.disclosure_taxonomy",
    summary="The taxonomy that classifies 8-K disclosures, one row per category.",
    fetch_path=_DISCLOSURE_TAXONOMY,
    poll_keys=("Taxonomy", *_CATEGORIES),
    page_size=1_000,
)

DATASETS: Final[tuple[DatasetSpec, ...]] = (
    TRADES,
    QUOTES,
    BARS,
    LULD,
    IMBALANCES,
    FMV,
    SNAPSHOTS,
    MOVERS,
    DAILY_OPEN_CLOSE,
    DIVIDENDS,
    SPLITS,
    IPOS,
    SHORT_INTEREST,
    SHORT_VOLUME,
    SHARE_FLOAT,
    BALANCE_SHEETS,
    INCOME_STATEMENTS,
    CASH_FLOW_STATEMENTS,
    RATIOS,
    FILINGS_INDEX,
    SECTIONS_10K,
    TEXT_8K,
    DISCLOSURES_8K,
    RISK_FACTORS,
    INSTITUTIONAL_HOLDINGS,
    INITIAL_INSIDER_OWNERSHIP,
    INSIDER_TRANSACTIONS,
    INSIDER_ACTIVITY,
    INSIDER_HOLDINGS,
    RISK_FACTOR_TAXONOMY,
    DISCLOSURE_TAXONOMY,
)
"""Every dataset of `m.stocks`, one `DatasetSpec` each (for example `stocks.trades`)."""


def _list_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    symbols: Symbols | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    params: Mapping[str, object] | None = None,
    arguments: Mapping[str, str] | None = None,
    sort: str | None,
    order: Order | None,
    limit: int | None,
    filters: Mapping[str, FilterValue] | None,
) -> Query:
    """Build the query of a REST list dataset, which refuses `dm.ALL` for `symbols=None`.

    Args:
        core: The session core.
        spec: The dataset.
        symbols: The tickers that filter the request, or None for every ticker.
        start: The first day of the dataset's axis, or None.
        end: The last day of the dataset's axis, or None.
        params: The dataset's other typed parameters by wire name; None values are dropped.
        arguments: The factory's argument name of each key of `params` it names differently,
            such as `{"issuer_cik.any_of": "issuer_cik"}`, for the message that refuses that
            key in `filters`; None when every argument has its wire name.
        sort: The field to sort by, or None.
        order: The sort direction, or None.
        limit: The cap on the total number of rows, or None.
        filters: The `filters` mapping, or None.

    Returns:
        The `Query`.

    Every `TypeError` and `ValueError` of `list_params` and `build_query` propagates.
    """
    query = build_query(
        core,
        spec,
        symbols=symbols,
        all_alternative=_EVERY_TICKER,
        start=start,
        end=end,
        params={**list_params(sort=sort, order=order), **(params or {})},
        arguments=arguments,
        filters=filters,
        limit=limit,
    )
    return cast("Query", query)


# --- Namespaces ---------------------------------------------------------------------------------


class _StockFinancials(Namespace):
    """`m.stocks.financials`: company financial statements and daily valuation ratios.

    Created once per session by `_Stocks`; holds no state of its own, so it is safe to use from
    any thread. Each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            sheets = m.stocks.financials.balance_sheets("AAPL", timeframe=dm.Timeframe.ANNUAL)
            t = sheets.fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "stocks.financials"

    def balance_sheets(
        self,
        symbols: Symbols | None = None,
        *,
        timeframe: Timeframe | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Balance sheets of US public companies, one row per filer and reporting period.

        Each row is a company's assets, liabilities, and equity at the end of a quarter or
        fiscal year, as filed with the SEC. A filing can name several share classes, so a row
        has no `Symbol`: its `Tickers` column holds the JSON list of tickers, and `symbols`
        only selects the filings that list any of them. Served by REST
        (`/stocks/financials/v1/balance-sheets`) through `fetch()` and `poll()`; `poll()`
        replaces the table each cycle, keyed by `Cik`, `PeriodEnd`, and `Timeframe`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every company. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#financials-balance_sheets,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as a `str` timeframe; `ValueError` if `symbols` is `dm.ALL` or empty, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: Tickers whose filings to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every filing.
            timeframe: `dm.Timeframe.ANNUAL` or `dm.Timeframe.QUARTERLY`; None for both.
                A balance sheet is a position at the period's end, so it has no trailing
                twelve months.
            start: The first period end date to include (`period_end`), inclusive; None for
                no lower bound.
            end: The last period end date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"period_end"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"fiscal_year_gte": 2020}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Raises:
            ValueError: If `timeframe` is `dm.Timeframe.TRAILING_TWELVE_MONTHS`, which the
                balance sheets do not offer.

        Example:
            ::

                q = m.stocks.financials.balance_sheets(
                    ["AAPL", "MSFT"], timeframe=dm.Timeframe.QUARTERLY, start="2023-01-01"
                )
                t = q.fetch()
        """
        if timeframe is Timeframe.TRAILING_TWELVE_MONTHS:
            raise ValueError(
                "balance sheets have no trailing twelve months; "
                "use dm.Timeframe.ANNUAL or dm.Timeframe.QUARTERLY"
            )
        return _statement_query(
            self._core,
            BALANCE_SHEETS,
            symbols=symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def income_statements(
        self,
        symbols: Symbols | None = None,
        *,
        timeframe: Timeframe | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Income statements of US public companies, one row per filer and reporting period.

        Each row is a company's revenue, expenses, earnings, and per-share figures for a
        quarter or fiscal year, as filed with the SEC. A filing can name several share
        classes, so a row has no `Symbol`: its `Tickers` column holds the JSON list of
        tickers, and `symbols` only selects the filings that list any of them. Served by REST
        (`/stocks/financials/v1/income-statements`) through `fetch()` and `poll()`; `poll()`
        replaces the table each cycle, keyed by `Cik`, `PeriodEnd`, and `Timeframe`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every company. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#financials-income_statements,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as a `str` timeframe; `ValueError` if `symbols` is `dm.ALL` or empty, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: Tickers whose filings to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every filing.
            timeframe: `dm.Timeframe.ANNUAL`, `QUARTERLY`, or `TRAILING_TWELVE_MONTHS`;
                None for every timeframe the endpoint holds.
            start: The first period end date to include (`period_end`), inclusive; None for
                no lower bound.
            end: The last period end date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"period_end"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"fiscal_year_gte": 2020}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.financials.income_statements(
                    "NVDA", timeframe=dm.Timeframe.ANNUAL
                ).fetch()
        """
        return _statement_query(
            self._core,
            INCOME_STATEMENTS,
            symbols=symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def cash_flow_statements(
        self,
        symbols: Symbols | None = None,
        *,
        timeframe: Timeframe | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Cash flow statements of US public companies, one row per filer and reporting period.

        Each row is a company's operating, investing, and financing cash flows for a quarter
        or fiscal year, as filed with the SEC. A filing can name several share classes, so a
        row has no `Symbol`: its `Tickers` column holds the JSON list of tickers, and `symbols`
        only selects the filings that list any of them. Served by REST
        (`/stocks/financials/v1/cash-flow-statements`) through `fetch()` and `poll()`;
        `poll()` replaces the table each cycle, keyed by `Cik`, `PeriodEnd`, and `Timeframe`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every company. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#financials-cash_flow_statements,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as a `str` timeframe; `ValueError` if `symbols` is `dm.ALL` or empty, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: Tickers whose filings to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every filing.
            timeframe: `dm.Timeframe.ANNUAL`, `QUARTERLY`, or `TRAILING_TWELVE_MONTHS`;
                None for every timeframe the endpoint holds.
            start: The first period end date to include (`period_end`), inclusive; None for
                no lower bound.
            end: The last period end date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"period_end"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"fiscal_year_gte": 2020}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.financials.cash_flow_statements("AAPL", start="2024-01-01").fetch()
        """
        return _statement_query(
            self._core,
            CASH_FLOW_STATEMENTS,
            symbols=symbols,
            timeframe=timeframe,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def ratios(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Daily valuation, profitability, liquidity, and leverage ratios of US stocks.

        Each row is one ticker's ratios (price to earnings, return on equity, debt to equity,
        and so on) as of the row's `Date`, computed by Massive from the trailing twelve months
        of filings and the daily close. The endpoint has no date filter, so the dataset has no
        `start` or `end`. Served by REST (`/stocks/financials/v1/ratios`) through `fetch()` and
        `poll()`; `symbols` is sent as one `ticker.any_of` list, and `poll()` replaces the
        table each cycle, keyed by `Symbol` and `Date`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#financials-ratios,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, or a filter
        is unknown, reserved, or of the wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            sort: The field to sort by, such as `"market_cap"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"price_to_earnings_lt": 15}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                cheap = m.stocks.financials.ratios(filters={"price_to_earnings_lt": 10}).fetch()
        """
        return _list_query(
            self._core,
            RATIOS,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


def _statement_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    symbols: Symbols | None,
    timeframe: object,
    start: DateLike | None,
    end: DateLike | None,
    sort: str | None,
    order: Order | None,
    limit: int | None,
    filters: Mapping[str, FilterValue] | None,
) -> Query:
    """Build the query of one financial statement, checking its `timeframe`.

    Args:
        core: The session core.
        spec: The statement's dataset.
        symbols: The tickers that filter the request, or None.
        timeframe: The `timeframe` argument as passed.
        start: The first period end date, or None.
        end: The last period end date, or None.
        sort: The field to sort by, or None.
        order: The sort direction, or None.
        limit: The cap on the total number of rows, or None.
        filters: The `filters` mapping, or None.

    Returns:
        The `Query`.

    A `timeframe` that is not a `dm.Timeframe` raises the `TypeError` of `_require_enum`; the
    errors of `_list_query` propagate.
    """
    frame = None if timeframe is None else _require_enum(timeframe, Timeframe, "timeframe")
    return _list_query(
        core,
        spec,
        symbols=symbols,
        start=start,
        end=end,
        params={"timeframe": frame},
        sort=sort,
        order=order,
        limit=limit,
        filters=filters,
    )


class _StockFilings(Namespace):
    """`m.stocks.filings`: SEC filings, the insider data built from them, and taxonomies.

    Created once per session by `_Stocks`; holds no state of its own, so it is safe to use from
    any thread. Each method builds a query and never blocks. By default `poll()` on a filings
    dataset appends only the rows it has not seen before, while the insider activity, the
    insider holdings, and the two taxonomies replace their table each cycle.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            latest = m.stocks.filings.index(form_type="8-K", start="2025-06-02").fetch()
    """

    __slots__ = ()
    _path: ClassVar[str] = "stocks.filings"

    def index(
        self,
        symbols: Symbols | None = None,
        *,
        form_type: str | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The index of SEC EDGAR filings by US-listed companies, one row per filing and filer.

        Each row names a filing's form type, filer CIK, issuer, ticker, filing date, and the URL of
        the document on EDGAR. A filing with several filers can have a row under each filer's
        CIK, as EDGAR's own index lists it: a Form 4 under the issuer and each reporting owner,
        a joint 10-K or 8-K under each co-registrant. Served by REST (`/stocks/filings/vX/index`)
        through `fetch()` and `poll()`. By default `poll()` appends only the rows it has not
        seen before, telling rows apart by `Cik` and `AccessionNumber`, so every filer's row is
        kept. A filing whose company has no ticker keeps a null `Symbol`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every filer. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-index,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as a non-`str` form type; `ValueError` if `symbols` is `dm.ALL`, empty, or
        holds an invalid ticker, a bound is not a date or ISO date, `end` is before `start`, or a
        filter is unknown, reserved, or of the wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every filer.
            form_type: The SEC form type, such as `"10-K"` or `"8-K"`; None for every form.
            start: The first filing date to include, inclusive; None for no lower bound,
                which `poll()` refuses unless `limit` is set: it would re-read every filing
                each cycle. A fixed start keeps widening the window of a long-running poll.
            end: The last filing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"filing_date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"cik": "0000320193"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                annual = m.stocks.filings.index("AAPL", form_type="10-K").fetch()
        """
        return _list_query(
            self._core,
            FILINGS_INDEX,
            symbols=symbols,
            start=start,
            end=end,
            params={"form_type": optional_text("form_type", form_type)},
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def sections_10k(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The text of sections of 10-K annual reports, one row per filing and section.

        Each row holds one section (the business description or the risk factors) of a company's
        10-K, with its ticker, CIK, period end, and filing date. Served by REST
        (`/stocks/filings/10-K/vX/sections`) through `fetch()` and `poll()`. By default `poll()`
        appends only the rows it has not seen before, telling rows apart by the dataset's key
        columns. A filing whose company has no ticker keeps a null `Symbol`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every filer. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-sections_10k,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every filer.
            start: The first filing date to include, inclusive; None for no lower bound,
                which `poll()` refuses unless `limit` is set: it would re-read every filing
                each cycle. A fixed start keeps widening the window of a long-running poll.
            end: The last filing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"filing_date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"section": "risk_factors"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.filings.sections_10k("AAPL", filters={"section": "business"}).fetch()
        """
        return _list_query(
            self._core,
            SECTIONS_10K,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def text_8k(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The item text of 8-K current reports, one row per filing and filer.

        Each row holds the text of the items a company reported on an 8-K (or an amendment), with
        its ticker, CIK, form type, and filing date. A joint 8-K of several co-registrants can have
        a row under each filer's CIK. Served by REST (`/stocks/filings/8-K/vX/text`) through
        `fetch()` and `poll()`. By default `poll()` appends only the rows it has not seen before,
        telling rows apart by `Cik` and `AccessionNumber`, so every filer's row is kept. A
        filing whose company has no ticker keeps a null `Symbol`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every filer. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-text_8k,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every filer.
            start: The first filing date to include, inclusive; None for no lower bound,
                which `poll()` refuses unless `limit` is set: it would re-read every filing
                each cycle. A fixed start keeps widening the window of a long-running poll.
            end: The last filing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"filing_date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"form_type": "8-K/A"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.filings.text_8k("TSLA", start="2025-01-01").fetch()
        """
        return _list_query(
            self._core,
            TEXT_8K,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def disclosures_8k(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Disclosures in 8-K current reports, classified by category, one row per disclosure.

        Each row is one disclosure of an 8-K, with its primary, secondary, and tertiary categories,
        the supporting text, and the filing's accession number and date. A filing can name several
        share classes, so a row has no `Symbol`: its `Tickers` column holds the JSON list of
        tickers, and `symbols` only selects the filings that list any of them. Served by REST
        (`/stocks/filings/8-K/vX/disclosures`) through `fetch()` and `poll()`. By default `poll()`
        appends only the rows it has not seen before, telling rows apart by the dataset's key
        columns.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every filer. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-disclosures_8k,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL` or empty, a bound is not a date or ISO date,
        `end` is before `start`, or a filter is unknown, reserved, or of the wrong kind.

        Args:
            symbols: Tickers whose filings to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every filing.
            start: The first filing date to include, inclusive; None for no lower bound,
                which `poll()` refuses unless `limit` is set: it would re-read every filing
                each cycle. A fixed start keeps widening the window of a long-running poll.
            end: The last filing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"filing_date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"tertiary_category": "Dividends"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.filings.disclosures_8k(["AAPL", "MSFT"], start="2025-01-01").fetch()
        """
        return _list_query(
            self._core,
            DISCLOSURES_8K,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def risk_factors(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Risk factors disclosed in company filings, classified by category, one row each.

        Each row is one risk factor a company disclosed, with its primary, secondary, and tertiary
        categories (see `risk_factor_taxonomy()`), the supporting text, and the filing date. Served
        by REST (`/stocks/filings/vX/risk-factors`) through `fetch()` and `poll()`. By default
        `poll()` appends only the rows it has not seen before, telling rows apart by the dataset's
        key columns. A filing whose company has no ticker keeps a null `Symbol`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every filer. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-risk_factors,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every filer.
            start: The first filing date to include, inclusive; None for no lower bound,
                which `poll()` refuses unless `limit` is set: it would re-read every filing
                each cycle. A fixed start keeps widening the window of a long-running poll.
            end: The last filing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"filing_date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"cik": "0000320193"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.filings.risk_factors("AAPL").fetch()
        """
        return _list_query(
            self._core,
            RISK_FACTORS,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def institutional_holdings(
        self,
        *,
        filer_cik: str | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Holdings that institutional managers report on Form 13-F, one row per position.

        Each row is one security an institutional investment manager held at the end of a quarter:
        the issuer, CUSIP, share count, market value, and voting authority, with the filer's CIK
        (`FilerCik`) and the filing's accession number and date. Rows name a CUSIP, not a ticker, so
        the dataset takes no `symbols`. Served by REST (`/stocks/filings/vX/13-F`) through `fetch()`
        and `poll()`. By default `poll()` appends only the rows it has not seen before, telling rows
        apart by the dataset's key columns.

        `limit` caps the total number of rows across pages; the page size is internal. No
        `symbols` means `dm.ALL` does not apply. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-institutional_holdings,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as an `int` CIK; `ValueError` if a bound is not a date or ISO date, `end` is
        before `start`, or a filter is unknown, reserved, or of the wrong kind.

        Args:
            filer_cik: The SEC CIK of the filing manager, as text with its leading zeros,
                such as `"0001067983"`; None for every filer.
            start: The first filing date to include, inclusive; None for no lower bound,
                which `poll()` refuses unless `limit` is set: it would re-read every filing
                each cycle. A fixed start keeps widening the window of a long-running poll.
            end: The last filing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"filing_date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.filings.institutional_holdings(
                    filer_cik="0001067983", start="2025-01-01"
                ).fetch()
        """
        return _list_query(
            self._core,
            INSTITUTIONAL_HOLDINGS,
            start=start,
            end=end,
            params={"filer_cik": optional_text("filer_cik", filer_cik)},
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def initial_insider_ownership(
        self,
        symbols: Symbols | None = None,
        *,
        issuer_cik: str | Sequence[str] | None = None,
        owner_cik: str | Sequence[str] | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Initial insider ownership statements (SEC Form 3), one row per reported holding.

        Each row is a security an officer, director, or 10% owner held on becoming an insider: the
        issuer and owner CIKs and names, the owner's role, the security, and the shares owned. A
        filing can name several share classes, so a row has no `Symbol`: its `Tickers` column holds
        the JSON list of tickers, and `symbols` only selects the filings that list any of them.
        Served by REST (`/stocks/filings/vX/form-3`) through `fetch()` and `poll()`. By default
        `poll()` appends only the rows it has not seen before, telling rows apart by the dataset's
        key columns.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every issuer. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-initial_insider_ownership,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as an `int` CIK; `ValueError` if `symbols` is `dm.ALL` or empty, a CIK
        sequence is empty, a bound is not a date or ISO date, `end` is before `start`, or a
        filter is unknown, reserved, or of the wrong kind.

        Args:
            symbols: Tickers whose filings to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every filing.
            issuer_cik: The SEC CIK of the issuing company, as text with its leading zeros
                (sent as `issuer_cik`), or a sequence of them for any of several issuers
                (sent as `issuer_cik.any_of`); None for every issuer.
            owner_cik: The SEC CIK of the reporting owner, one or a sequence, sent the same
                way (`owner_cik` or `owner_cik.any_of`); None for every owner.
            start: The first filing date to include, inclusive; None for no lower bound,
                which `poll()` refuses unless `limit` is set: it would re-read every filing
                each cycle. A fixed start keeps widening the window of a long-running poll.
            end: The last filing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"filing_date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.filings.initial_insider_ownership("NVDA", start="2024-01-01").fetch()
        """
        return _insider_query(
            self._core,
            INITIAL_INSIDER_OWNERSHIP,
            symbols=symbols,
            issuer_cik=issuer_cik,
            owner_cik=owner_cik,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def insider_transactions(
        self,
        symbols: Symbols | None = None,
        *,
        issuer_cik: str | Sequence[str] | None = None,
        owner_cik: str | Sequence[str] | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Insider transactions (SEC Form 4), one row per reported transaction or holding.

        Each row is a purchase, sale, grant, or exercise an officer, director, or 10% owner
        reported, or a holding reported beside them: the transaction code, date, shares, price, and
        the shares owned afterwards, with the issuer and owner. A filing can name several share
        classes, so a row has no `Symbol`: its `Tickers` column holds the JSON list of tickers, and
        `symbols` only selects the filings that list any of them. Served by REST
        (`/stocks/filings/vX/form-4`) through `fetch()` and `poll()`. By default `poll()` appends
        only the rows it has not seen before, telling rows apart by the dataset's key columns.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every issuer. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-insider_transactions,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as an `int` CIK; `ValueError` if `symbols` is `dm.ALL` or empty, a CIK
        sequence is empty, a bound is not a date or ISO date, `end` is before `start`, or a
        filter is unknown, reserved, or of the wrong kind.

        Args:
            symbols: Tickers whose filings to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every filing.
            issuer_cik: The SEC CIK of the issuing company, as text with its leading zeros
                (sent as `issuer_cik`), or a sequence of them for any of several issuers
                (sent as `issuer_cik.any_of`); None for every issuer.
            owner_cik: The SEC CIK of the reporting owner, one or a sequence, sent the same
                way (`owner_cik` or `owner_cik.any_of`); None for every owner.
            start: The first filing date to include, inclusive; None for no lower bound,
                which `poll()` refuses unless `limit` is set: it would re-read every filing
                each cycle. A fixed start keeps widening the window of a long-running poll.
            end: The last filing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"filing_date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"transaction_code": "P"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                buys = m.stocks.filings.insider_transactions(
                    "AAPL", filters={"transaction_code": "P"}
                ).fetch()
        """
        return _insider_query(
            self._core,
            INSIDER_TRANSACTIONS,
            symbols=symbols,
            issuer_cik=issuer_cik,
            owner_cik=owner_cik,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def insider_activity(
        self,
        symbols: Symbols | None = None,
        *,
        issuer_cik: str | Sequence[str] | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Open-market insider purchases and sales, summed per company and transaction day.

        An insider is a company's director, officer, or holder of more than 10% of its shares;
        the SEC requires each to report every trade in the company's stock on Form 4 (or, for
        some late or exempt trades, Form 5). Massive sums the open-market purchases and sales of
        every insider of one issuer on one transaction day into one row: how many purchases and
        sales, by how many distinct insiders, the shares and US-dollar values bought and sold,
        their net (`NetShares`, `NetValue`, negative for net selling), the part traded under a
        Rule 10b5-1 trading plan (a sale or purchase scheduled in advance, so it says less about
        the insider's view), and the date of the latest filing that reported one of the trades.
        Use it to gauge net insider buying or selling over a window without reading individual
        filings; `insider_transactions()` returns the Form 4 lines themselves.

        A company can have several share classes, so a row has no `Symbol`: its `Tickers`
        column holds the JSON list of the issuer's tickers on the transaction day, the totals
        cover the issuer rather than each ticker, and `symbols` only selects the issuers that
        list any of them. Served by REST (`/stocks/insiders/vX/activity`) through `fetch()` and
        `poll()`. By default `poll()` replaces its table each cycle, keyed by `IssuerCik` and
        `TransactionDate`: a filing that arrives later for a day already shown revises that day's
        totals in place. Massive updates the data daily.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every issuer. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-insider_activity,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as an `int` CIK; `ValueError` if `symbols` is `dm.ALL` or empty, a CIK
        sequence is empty, a bound is not a date or ISO date, `end` is before `start`, or a
        filter is unknown, reserved, or of the wrong kind.

        Args:
            symbols: Tickers whose issuers to return (one `str`, a sequence, or a `Table`
                with a `Symbol` column, re-read each `poll()` cycle); None for every issuer.
            issuer_cik: The SEC Central Index Key (CIK) of the issuing company, as text with
                its leading zeros, such as `"0001045810"` (sent as `issuer_cik`), or a
                sequence of them for any of several issuers (sent as `issuer_cik.any_of`);
                None for every issuer.
            start: The first transaction date to include, inclusive; None for no lower bound.
            end: The last transaction date to include, inclusive; None for no upper bound.
            sort: The field to sort by, such as `"net_value"`; None for the server default,
                the newest transaction date first.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores; checked now. The endpoint has no parameter left for it: the
                arguments above reserve every form of the tickers, the CIKs, and the date
                (`tickers.all_of` and the exclusive `.gt` and `.lt` bounds included), so
                today any key raises `ValueError`.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.filings.insider_activity("NVDA", start="2026-01-01").fetch()
        """
        return _list_query(
            self._core,
            INSIDER_ACTIVITY,
            symbols=symbols,
            start=start,
            end=end,
            params=_ciks(issuer_cik=issuer_cik),
            arguments=_CIK_ARGUMENTS,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def insider_holdings(
        self,
        symbols: Symbols | None = None,
        *,
        issuer_cik: str | Sequence[str] | None = None,
        owner_cik: str | Sequence[str] | None = None,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Each company's insiders and the shares each one holds now, one row per insider.

        An insider is a company's director, officer, or holder of more than 10% of its shares,
        who reports an initial position on SEC Form 3 and every later change on Forms 4 and 5.
        Massive combines all of one insider's filings for one issuer into one current row: the
        owner's name, CIK, roles (a JSON list of `director`, `officer`, `ten_percent_owner`,
        `other`), and officer title; the dates of the first and latest filing and their count;
        the shares owned directly, indirectly (through trusts, LLCs, family members, and other
        vehicles), and in total, as filed and not adjusted for splits, with no options or other
        derivative securities; and that total as a percentage of the shares outstanding
        (`OwnershipPct`, in percent, not a fraction: 0.0198 means 0.0198%).
        `OldestPositionDate` far behind `LastFilingDate` warns that a summed position has not
        been restated lately and may be stale. Use it to see who a company's insiders are and
        how much they own, or, with `owner_cik`, every company one person is an insider of;
        `initial_insider_ownership()` (Form 3) and `insider_transactions()` (Form 4) return the
        filings themselves.

        A company can have several share classes, so a row has no `Symbol`: its `Tickers`
        column holds the JSON list of the issuer's tickers, and `symbols` only selects the
        issuers that list any of them. Served by REST (`/stocks/insiders/vX/holdings`) through
        `fetch()` and `poll()`. By default `poll()` replaces its table each cycle, keyed by
        `IssuerCik` and `OwnerCik`, so a new filing updates its insider's row in place. Massive
        updates the data daily.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every issuer. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-insider_holdings,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type, such as an `int` CIK; `ValueError` if `symbols` is `dm.ALL` or empty, a CIK
        sequence is empty, a bound is not a date or ISO date, `end` is before `start`, or a
        filter is unknown, reserved, or of the wrong kind.

        Args:
            symbols: Tickers whose issuers' insiders to return (one `str`, a sequence, or a
                `Table` with a `Symbol` column, re-read each `poll()` cycle); None for every
                issuer.
            issuer_cik: The SEC Central Index Key (CIK) of the issuing company, as text with
                its leading zeros, such as `"0001045810"` (sent as `issuer_cik`), or a
                sequence of them for any of several issuers (sent as `issuer_cik.any_of`);
                None for every issuer.
            owner_cik: The SEC CIK of the insider, as text with its leading zeros, such as
                `"0001588670"`, the same for one person across every issuer; or a sequence
                of them, sent as `owner_cik.any_of`. None for every insider.
            start: The earliest latest-filing date (`LastFilingDate`) to include, inclusive;
                an insider who has not filed since is left out. None for no lower bound.
            end: The last latest-filing date to include, inclusive; None for no upper bound.
            sort: The field to sort by, such as `"shares_owned"`; None for the server default,
                the most recent latest-filing date first.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores; checked now. The endpoint has no parameter left for it: the
                arguments above reserve every form of the tickers, the CIKs, and the date
                (`tickers.all_of` and the exclusive `.gt` and `.lt` bounds included), so
                today any key raises `ValueError`.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                insiders = m.stocks.filings.insider_holdings("NVDA").fetch()
                one_person = m.stocks.filings.insider_holdings(owner_cik="0001588670").fetch()
        """
        return _insider_query(
            self._core,
            INSIDER_HOLDINGS,
            symbols=symbols,
            issuer_cik=issuer_cik,
            owner_cik=owner_cik,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def risk_factor_taxonomy(
        self,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The taxonomy that classifies filing risk factors, one row per category.

        Each row is one tertiary category of the risk-factor classification with its primary and
        secondary parents, its description, and the taxonomy version; the categories of
        `risk_factors()` come from it. Served by REST (`/stocks/taxonomies/vX/risk-factors`) through
        `fetch()` and `poll()`. `poll()` replaces the table each cycle, keyed by `Taxonomy` and the
        three categories.

        `limit` caps the total number of rows across pages; the page size is internal. The
        dataset takes no `symbols`, so `dm.ALL` does not apply. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-risk_factor_taxonomy,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if a filter is unknown, reserved, or of the wrong kind.

        Args:
            sort: The field to sort by, such as `"primary_category"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"primary_category": "Financial"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.filings.risk_factor_taxonomy().fetch()
        """
        return _list_query(
            self._core,
            RISK_FACTOR_TAXONOMY,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def disclosure_taxonomy(
        self,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The taxonomy that classifies 8-K disclosures, one row per category.

        Each row is one tertiary category of the 8-K disclosure classification with its primary and
        secondary parents, its description, and the taxonomy version; the categories of
        `disclosures_8k()` come from it. Served by REST (`/stocks/taxonomies/vX/disclosures`)
        through `fetch()` and `poll()`. `poll()` replaces the table each cycle, keyed by `Taxonomy`
        and the three categories.

        `limit` caps the total number of rows across pages; the page size is internal. The
        dataset takes no `symbols`, so `dm.ALL` does not apply. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#filings-disclosure_taxonomy,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if a filter is unknown, reserved, or of the wrong kind.

        Args:
            sort: The field to sort by, such as `"primary_category"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"taxonomy": "1.0"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.filings.disclosure_taxonomy().fetch()
        """
        return _list_query(
            self._core,
            DISCLOSURE_TAXONOMY,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )


_CIK_ARGUMENTS: Final = {"issuer_cik.any_of": "issuer_cik", "owner_cik.any_of": "owner_cik"}
"""The argument that sets each CIK list parameter, for the message that rejects it in `filters`."""


def _ciks(**ciks: object) -> dict[str, object]:
    """Return the wire parameters of the insider datasets' CIK arguments.

    Every insider path takes each CIK both alone and as `.any_of`, so one CIK is sent as
    `issuer_cik` (or `owner_cik`) and a sequence as `issuer_cik.any_of`.

    Args:
        ciks: Each CIK argument by name (`issuer_cik`, `owner_cik`) as passed: one CIK, a
            sequence of CIKs, or None.

    Returns:
        Both wire forms of each argument, the unset one None.

    The `TypeError` and `ValueError` of `one_or_any_of` propagate: a CIK that is neither
    text, a sequence, nor None, or an empty sequence.
    """
    params: dict[str, object] = {}
    for name, value in ciks.items():
        params |= one_or_any_of(name, value, every=f"every {name.removesuffix('_cik')}")
    return params


def _insider_query(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    symbols: Symbols | None,
    issuer_cik: object,
    owner_cik: object,
    start: DateLike | None,
    end: DateLike | None,
    sort: str | None,
    order: Order | None,
    limit: int | None,
    filters: Mapping[str, FilterValue] | None,
) -> Query:
    """Build the query of an insider dataset that takes an issuer and an owner CIK.

    Form 3, Form 4, and the insider holdings share the arguments and their checks.

    Args:
        core: The session core.
        spec: The dataset.
        symbols: The tickers that filter the request, or None.
        issuer_cik: The `issuer_cik` argument as passed: one CIK, a sequence, or None.
        owner_cik: The `owner_cik` argument as passed: one CIK, a sequence, or None.
        start: The first day of the dataset's date axis, or None.
        end: The last day of the dataset's date axis, or None.
        sort: The field to sort by, or None.
        order: The sort direction, or None.
        limit: The cap on the total number of rows, or None.
        filters: The `filters` mapping, or None.

    Returns:
        The `Query`.

    The errors of `_ciks` and `_list_query` propagate.
    """
    return _list_query(
        core,
        spec,
        symbols=symbols,
        start=start,
        end=end,
        params=_ciks(issuer_cik=issuer_cik, owner_cik=owner_cik),
        arguments=_CIK_ARGUMENTS,
        sort=sort,
        order=order,
        limit=limit,
        filters=filters,
    )


class _Stocks(Namespace):
    """`m.stocks`: the US equities datasets of a session.

    Created once by `MassiveSession` and reached as `m.stocks`, together with its nested
    `m.stocks.financials` and `m.stocks.filings` namespaces. It holds no mutable state, so it
    is safe to use from any thread; each method builds a query and never blocks.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            live = m.stocks.trades(["AAPL", "MSFT"]).last_by()
            daily = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2025-01-02", "2025-06-30")
    """

    __slots__ = ("_filings", "_financials")
    _path: ClassVar[str] = "stocks"

    def __init__(self, core: SessionCore) -> None:
        """Create the namespace and its two nested namespaces.

        Args:
            core: The session's internal core.
        """
        super().__init__(core)
        self._financials = _StockFinancials(core)
        self._filings = _StockFilings(core)

    @property
    def financials(self) -> _StockFinancials:
        """The financial statements and ratios datasets (`m.stocks.financials`)."""
        return self._financials

    @property
    def filings(self) -> _StockFilings:
        """The SEC filings and taxonomy datasets (`m.stocks.filings`)."""
        return self._filings

    def trades(self, symbols: Symbols) -> StreamHistoryLatestQuery:
        """Trades of US-listed stocks, tick by tick.

        Each row is one trade reported to the SIP: price, size, exchange, conditions, and the
        SIP, participant, and TRF timestamps (`Timestamp` is the SIP time). Each row also
        carries its trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date of
        `Timestamp`), and `TimestampInsert`, when the library received it. Streams over the
        WebSocket (`T`) through `blink()`, `append_only()`, `ring()`, and `last_by()`;
        `history()` reads REST (`/v3/trades/{stockTicker}`) or the flat files
        (`us_stocks_sip/trades_v1`); `fetch()` returns each symbol's last trade
        (`/v2/last/trade/{stocksTicker}`), one request per symbol.

        `history()` has no `limit`: it pages internally. `dm.ALL` streams every ticker and
        reads every ticker from the flat files; REST `history()` and `fetch()` refuse it.
        Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#trades,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if `symbols` is not one of the
        accepted forms; `ValueError` if `symbols` is empty or holds an invalid ticker, or a `Table`
        has no `Symbol` column.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (streams follow its rows), or `dm.ALL`.

        Returns:
            The query; nothing is requested or subscribed until a verb runs.

        Example:
            ::

                q = m.stocks.trades(["AAPL", "MSFT"])
                recent = q.ring(10_000)
                friday = q.history("2025-06-06", "2025-06-06")
        """
        return cast("StreamHistoryLatestQuery", build_query(self._core, TRADES, symbols=symbols))

    def quotes(self, symbols: Symbols) -> StreamHistoryLatestQuery:
        """National best bid and offer quotes of US-listed stocks, tick by tick.

        Each row is one NBBO update: bid and ask prices, sizes, and exchanges, conditions,
        indicators, and the SIP and participant timestamps (`Timestamp` is the SIP time). Each
        row also carries its trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date of
        `Timestamp`), and `TimestampInsert`, when the library received it. Streams over the
        WebSocket (`Q`) through `blink()`, `append_only()`, `ring()`, and `last_by()`;
        `history()` reads REST (`/v3/quotes/{stockTicker}`) or the flat files
        (`us_stocks_sip/quotes_v1`); `fetch()` returns each symbol's last NBBO
        (`/v2/last/nbbo/{stocksTicker}`), one request per symbol.

        `history()` has no `limit`: it pages internally. `dm.ALL` streams every ticker and
        reads every ticker from the flat files; REST `history()` and `fetch()` refuse it.
        Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#quotes,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if `symbols` is not one of the
        accepted forms; `ValueError` if `symbols` is empty or holds an invalid ticker, or a `Table`
        has no `Symbol` column.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (streams follow its rows), or `dm.ALL`.

        Returns:
            The query; nothing is requested or subscribed until a verb runs.

        Example:
            ::

                top = m.stocks.quotes(["AAPL", "SPY"]).last_by()
        """
        return cast("StreamHistoryLatestQuery", build_query(self._core, QUOTES, symbols=symbols))

    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: StreamSpan = Timespan.MINUTE,
        *,
        multiplier: Literal[1] = 1,
        adjusted: bool = True,
    ) -> StreamHistoryQuery: ...
    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: DaySpan,
        *,
        multiplier: Literal[1] = 1,
        adjusted: bool = True,
    ) -> HistoryLatestQuery: ...
    @overload
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan = Timespan.MINUTE,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery: ...
    def bars(
        self,
        symbols: Symbols,
        timespan: Timespan = Timespan.MINUTE,
        *,
        multiplier: int = 1,
        adjusted: bool = True,
    ) -> HistoryQuery:
        """Aggregate bars (open, high, low, close, volume) of US-listed stocks.

        Each row is one bar of `multiplier` times `timespan`: its window, from `TimestampStart`
        to the exclusive `TimestampEnd`, its trading day `Date` (ISO `yyyy-MM-dd` text, New York
        time), its open, high, low, close, volume, VWAP, and trade count, and `TimestampInsert`,
        when the library received it. The timespan picks the transports and the query class:
        one-second and one-minute bars also stream over the WebSocket (`A`, `AM`) as a
        `StreamHistoryQuery`; one-day bars add `fetch()` of the previous completed session
        (`/v2/aggs/ticker/{stocksTicker}/prev`, one request per symbol) as a
        `HistoryLatestQuery`; every other combination is history only. `history()` reads REST
        (`/v2/aggs/ticker/{stocksTicker}/range/...`) or, for unadjusted one-minute and one-day
        bars, the flat files (`us_stocks_sip/minute_aggs_v1`, `us_stocks_sip/day_aggs_v1`); with
        `dm.ALL`, one-day REST history reads the grouped daily bars
        (`/v2/aggs/grouped/locale/us/market/stocks/{date}`), one request per day.

        `TimestampEnd` is the first instant at which the bar is complete, so join on it
        (`aj(t, bars, on=["Symbol", "Timestamp>=TimestampEnd"])`) to attach only bars whose
        window had closed; it is not when Massive published the bar. An intraday bar ends
        its length after its start. A bar of a day or longer runs from 00:00 New York time
        of its window's first day to 00:00 after its last, whatever the source: the range
        stamps a 1-day bar at the day's start, while grouped daily, `/prev`, and the day
        files stamp it at the 16:00 session close (13:00 on a short trading day), which is
        moved to the day's start. A week runs Monday to Friday and a month, quarter, or year
        from its first to its last weekday. A bar of several days counts trading days,
        ending as late as holidays could push it (one weekday more, and one more for every
        five further days), since there is no holiday calendar. A longer bar must be stamped
        at 00:00 on the day its period starts: a week on its Monday or the Saturday or
        Sunday before, a month, quarter, or year on its 1st. Any other stamp fails the call
        with `MassiveError` naming the source, since the bar may cover other days than its
        label. `Date` is null for a bar that spans more than one date: several days, a week
        or longer, or an intraday bar across midnight. Joining an intraday row to the same
        day's daily bar on `Date` attaches values not known until that day ended.

        Prices are split-adjusted only when `adjusted` is True, the default, and only REST
        serves adjusted bars: a price from before a split is restated in the split's terms, so
        it is not the price that traded, and volumes are restated the other way. Massive
        adjusts for splits only, never for cash dividends. The flat files and the WebSocket are
        as traded, so `history()` reads the flat files only with `adjusted=False`, and one
        `history()` never mixes the two bases: every page's own `adjusted` must match the
        request, or the call raises `MassiveError`. So the default
        `bars(dm.ALL, dm.Timespan.MINUTE).history(...)` raises `ValueError`: the flat files need
        `adjusted=False`, and REST needs explicit symbols. Live bars are never adjusted after
        the fact, so a live table, or a merge of adjusted history and live bars, shows a jump
        at a split's execution date; rebuild from history after a split, or adjust
        unadjusted bars yourself with the factors of `splits()`.

        `history()` has no `limit`: it pages internally. It returns only the bars whose
        whole window lies inside the range and has ended, judged by `TimestampEnd`: a week
        runs Monday to Friday and a month from its first to its last weekday, so the current
        day's, week's, or month's bar never appears, and a 2-day bar from a Thursday is left
        out of a range that ends on the Friday, since a holiday could carry it into Monday.
        `dm.ALL` streams every ticker and works with `history()` where a flat file or the
        grouped daily path serves it; `fetch()` refuses it. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#bars,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if `timespan` is not a
        `dm.Timespan`, `multiplier` is not an `int`, `adjusted` is not a `bool`, or `symbols` is not
        an accepted form; `ValueError` if `timespan` is `SESSION`, `multiplier` is below 1, or
        `symbols` is empty or holds an invalid ticker.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (streams follow its rows), or `dm.ALL`.
            timespan: The bar unit, a `dm.Timespan` other than `SESSION`; defaults to
                `MINUTE`.
            multiplier: The number of `timespan` units per bar, a positive `int`; defaults to
                1. Only 1 streams or has `fetch()`.
            adjusted: True (the default) for bars adjusted for splits, from REST only; False
                for bars as traded, which the flat files can also serve. Streams are always as
                traded.

        Returns:
            The query, of the class the timespan and multiplier give; nothing is requested
            or subscribed until a verb runs.

        Example:
            ::

                live = m.stocks.bars(["AAPL"]).append_only()  # one-minute bars, streamed
                daily = m.stocks.bars(dm.ALL, dm.Timespan.DAY).history("2025-06-02", "2025-06-06")
                hourly = m.stocks.bars("SPY", dm.Timespan.HOUR, multiplier=4).history(
                    "2025-01-01", "2025-03-31"
                )
        """
        query = build_query(
            self._core,
            BARS,
            symbols=symbols,
            timespan=timespan,
            multiplier=multiplier,
            adjusted=require_bool("adjusted", adjusted),
        )
        return cast("HistoryQuery", query)

    def luld(self, symbols: Symbols) -> StreamQuery:
        """Limit up-limit down price bands of US-listed stocks, as they change.

        Each row is one band update from the SIP: the upper and lower price limits, the
        indicators, and the tape. Streams over the WebSocket (`LULD`) through `blink()`,
        `append_only()`, `ring()`, and `last_by()`; there is no REST or flat-file source, so the
        query has no `fetch()` or `history()`. Each row also carries its time, `Timestamp`, its
        trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date of `Timestamp`), and
        `TimestampInsert`, when the library received it.

        No verb takes a `limit`. `dm.ALL` streams every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#luld,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if `symbols` is not one of the
        accepted forms; `ValueError` if `symbols` is empty or holds an invalid ticker, or a `Table`
        has no `Symbol` column.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (the stream follows its rows), or `dm.ALL`.

        Returns:
            The query; nothing is subscribed until a verb runs.

        Example:
            ::

                bands = m.stocks.luld(dm.ALL).last_by()
        """
        return cast("StreamQuery", build_query(self._core, LULD, symbols=symbols))

    def imbalances(self, symbols: Symbols) -> StreamQuery:
        """Net order imbalances of US-listed stocks ahead of opening and closing auctions.

        Each row is one imbalance message: the auction type and time, the imbalance and paired
        quantities, the book clearing price, and the exchange. Streams over the WebSocket
        (`NOI`) through `blink()`, `append_only()`, `ring()`, and `last_by()`; there is no REST
        or flat-file source, so the query has no `fetch()` or `history()`. Each row also carries
        its time, `Timestamp`, its trading day, `Date` (ISO `yyyy-MM-dd` text, the New York date
        of `Timestamp`), and `TimestampInsert`, when the library received it.

        No verb takes a `limit`. `dm.ALL` streams every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#imbalances,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if `symbols` is not one of the
        accepted forms; `ValueError` if `symbols` is empty or holds an invalid ticker, or a `Table`
        has no `Symbol` column.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (the stream follows its rows), or `dm.ALL`.

        Returns:
            The query; nothing is subscribed until a verb runs.

        Example:
            ::

                closing = m.stocks.imbalances(["AAPL", "MSFT"]).append_only()
        """
        return cast("StreamQuery", build_query(self._core, IMBALANCES, symbols=symbols))

    def fmv(self, symbols: Symbols) -> StreamQuery:
        """Massive's fair market value estimate of US-listed stocks, as it updates.

        Each row is one fair market value with the time it was computed. Streams over the
        WebSocket (`FMV`) from the Business host (`business.massive.com/stocks`), which needs a
        Business plan, through `blink()`, `append_only()`, `ring()`, and `last_by()`; there is
        no REST or flat-file source, so the query has no `fetch()` or `history()`. Each row also
        carries its time, `Timestamp`, its trading day, `Date` (ISO `yyyy-MM-dd` text, the New
        York date of `Timestamp`), and `TimestampInsert`, when the library received it.

        No verb takes a `limit`. `dm.ALL` streams every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#fmv,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if `symbols` is not one of the
        accepted forms; `ValueError` if `symbols` is empty or holds an invalid ticker, or a `Table`
        has no `Symbol` column.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (the stream follows its rows), or `dm.ALL`.

        Returns:
            The query; nothing is subscribed until a verb runs.

        Example:
            ::

                fair = m.stocks.fmv(["AAPL"]).last_by()
        """
        return cast("StreamQuery", build_query(self._core, FMV, symbols=symbols))

    def snapshots(self, symbols: Symbols, *, include_otc: bool = False) -> Query:
        """Current market snapshots of US-listed stocks, one row per ticker.

        Each row is a ticker's state now: the day's and previous day's bars, the latest
        minute bar, last trade, and last quote, today's change, and the fair market value
        where the plan includes it. Served by REST
        (`/v2/snapshot/locale/us/markets/stocks/tickers`) through `fetch()` and `poll()`, one
        request for every symbol; `poll()` replaces the table each cycle, keyed by `Symbol`.
        `snapshots(symbols).poll(n)` is the way to get current state on every plan.

        `fetch()` and `poll()` take no `limit`: the endpoint returns one row per ticker.
        `dm.ALL` returns the full market. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#snapshots,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if `include_otc` is not a `bool`,
        or `symbols` is not an accepted form; `ValueError` if `symbols` is empty or holds an invalid
        ticker, or a `Table` has no `Symbol` column.

        Args:
            symbols: The tickers: one `str`, a sequence of them, a `Table` with a `Symbol`
                column (re-read each `poll()` cycle), or `dm.ALL` for the full market.
            include_otc: True to include OTC securities; False (the default) leaves them out.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                board = m.stocks.snapshots(["AAPL", "MSFT", "NVDA"]).poll(5)
        """
        query = build_query(
            self._core,
            SNAPSHOTS,
            symbols=symbols,
            params={"include_otc": require_bool("include_otc", include_otc)},
        )
        return cast("Query", query)

    def movers(self, direction: Direction, *, include_otc: bool = False) -> Query:
        """The twenty US stocks with the largest percentage move today, up or down.

        Each row is the full snapshot of one mover (as `snapshots()` returns it), for the
        tickers with the largest gain or loss since the previous close. Served by REST
        (`/v2/snapshot/locale/us/markets/stocks/{direction}`) through `fetch()` and `poll()`;
        `poll()` replaces the table each cycle, keyed by `Symbol`, so tickers that leave the
        list are removed.

        `fetch()` and `poll()` take no `limit`: the endpoint returns at most twenty rows. The
        dataset takes no `symbols`, so `dm.ALL` does not apply. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#movers,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if `direction` is not a
        `dm.Direction` or `include_otc` is not a `bool`.

        Args:
            direction: `dm.Direction.GAINERS` or `dm.Direction.LOSERS`.
            include_otc: True to include OTC securities; False (the default) leaves them out.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                gainers = m.stocks.movers(dm.Direction.GAINERS).poll(60)
        """
        query = build_query(
            self._core,
            MOVERS,
            params={
                "direction": _require_enum(direction, Direction, "direction"),
                "include_otc": require_bool("include_otc", include_otc),
            },
        )
        return cast("Query", query)

    def daily_open_close(self, symbols: Symbols, date: DateLike, *, adjusted: bool = True) -> Query:
        """The open, close, and pre- and after-market prices of US stocks on one day.

        Each row is one ticker's session on `date`: its `Date` (ISO `yyyy-MM-dd` text), the
        day's window as a 1-day bar's (`TimestampStart` and `TimestampEnd`, the New York
        midnights around it), open, high, low, close, volume, and the pre-market and after-hours
        prices. Served by REST (`/v1/open-close/{stocksTicker}/{date}`) through `fetch()` and
        `poll()`, one request per symbol; a symbol with no session that day (a holiday) is
        dropped with a warning. `poll()` replaces the table each cycle, keyed by `Symbol`.

        `fetch()` and `poll()` take no `limit`: each symbol gives one row. `dm.ALL` is
        refused: use `m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(date, date)`. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#daily_open_close,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if `date` is not a `str`, `date`,
        or `datetime`, `adjusted` is not a `bool`, or `symbols` is not an accepted form;
        `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, or `date` is a
        `datetime` or not an ISO-8601 date.

        Args:
            symbols: The tickers: one `str`, a sequence of them, or a `Table` with a `Symbol`
                column (re-read each `poll()` cycle).
            date: The trading day: an ISO-8601 date or a `date`. It is a calendar date, so a
                `datetime` raises `ValueError`.
            adjusted: True (the default) for prices and volume adjusted for splits (never for
                dividends); False for the prices as traded. Massive does not echo the basis
                here, so it cannot be checked.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.daily_open_close(["AAPL", "MSFT"], "2025-06-06").fetch()
        """
        query = build_query(
            self._core,
            DAILY_OPEN_CLOSE,
            symbols=symbols,
            all_alternative="m.stocks.bars(dm.ALL, dm.Timespan.DAY).history(date, date)",
            adjusted=require_bool("adjusted", adjusted),
            params={"date": calendar_day(DAILY_OPEN_CLOSE, date, "date")},
        )
        return cast("Query", query)

    def dividends(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Cash dividends of US-listed stocks, one row per distribution.

        Each row is one dividend: its ex-dividend, record, pay, and declaration dates, the
        cash amount (as declared and split-adjusted), the currency, frequency, and
        distribution type. Served by REST (`/stocks/v1/dividends`) through `fetch()` and
        `poll()`; `poll()` replaces the table each cycle, keyed by `Id`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#dividends,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            start: The first ex-dividend date to include, inclusive; None for no lower bound.
            end: The last ex-dividend date to include, inclusive. None (the default) sends no upper
                bound, so an announced dividend whose ex-date is still to come is returned too; pass
                `end=` to stop at a date.
            sort: The field to sort by, such as `"ex_dividend_date"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"distribution_type": "special"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.dividends(["AAPL", "MSFT"], start="2024-01-01").fetch()
        """
        return _list_query(
            self._core,
            DIVIDENDS,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def splits(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Stock splits, reverse splits, and stock dividends of US-listed stocks.

        Each row is one split: its execution date (`ExecutionDate`, ISO `yyyy-MM-dd` text), the
        split ratio (`SplitFrom` to `SplitTo`), the adjustment type, and the factor that adjusts
        earlier prices (`HistoricalAdjustmentFactor`). Served by REST (`/stocks/v1/splits`)
        through `fetch()` and `poll()`; `poll()` replaces the table each cycle, keyed by `Id`.

        Use it to adjust bars as traded (`bars(..., adjusted=False)`, the flat files, or a
        stream) yourself. The factor is cumulative: for a bar dated D, take the first split
        whose execution date is after D and multiply the bar's prices by its factor and divide
        its volume by it; a bar with no later split is already on today's basis. Both dates are
        ISO text, which orders as dates do, so a reverse as-of join finds that split:
        `bars.raj(splits, on=["Symbol", "Date<ExecutionDate"],
        joins=["F=HistoricalAdjustmentFactor"])`, then `Close * F` and `Volume / F`, with a
        null `F` meaning 1. A split takes effect overnight: on its execution date all trading,
        the pre-market included, is already on the new basis.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#splits,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            start: The first execution date to include, inclusive; None for no lower bound.
            end: The last execution date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"execution_date"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"adjustment_type": "reverse_split"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.splits(start="2025-01-01", end="2025-06-30").fetch()

                # NVDA's daily bars as traded, adjusted for its splits by hand.
                bars = m.stocks.bars("NVDA", dm.Timespan.DAY, adjusted=False).history(
                    "2024-06-03", "2024-06-14"
                )
                splits = m.stocks.splits("NVDA").fetch()
                adjusted = bars.raj(
                    splits,
                    on=["Symbol", "Date<ExecutionDate"],
                    joins=["F=HistoricalAdjustmentFactor"],
                ).update(
                    ["F = isNull(F) ? 1.0 : F", "AdjClose = Close * F", "AdjVolume = Volume / F"]
                )
        """
        return _list_query(
            self._core,
            SPLITS,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def ipos(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Initial public offerings on US exchanges, upcoming and past, one row per offering.

        Each row is one IPO: the issuer, ticker, listing and announcement dates, status (rumored,
        pending, new, withdrawn, and so on), offer price range, final issue price, shares offered,
        and the listing exchange. Served by REST (`/vX/reference/ipos`) through `fetch()` and
        `poll()`; the endpoint filters one ticker per request, so a symbol list fans out to one
        request per symbol. By default `poll()` updates each offering in place, keyed by
        `Symbol` and `IssuerName`, as it moves from rumor or pending to new and history (or is
        postponed or withdrawn); rows are never deleted. Two issuers that used one ticker stay
        two rows, while an issuer's withdrawn offering and its later offering under the same
        ticker share one row, which shows the latest. The key assumes Massive keeps an
        offering's issuer name the same through its statuses, which Massive does not
        document: if it revised the name (a legal suffix added, say), the row under the old
        name would stay, showing the offering's earlier state beside the new row.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every offering. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#ipos,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every offering.
            start: The first listing date to include, inclusive; None for no lower bound.
            end: The last listing date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"listing_date"`; None for the server default.
            order: The sort direction, sent as the IPO path's own `order` parameter, so it works
                with or without `sort`; None for the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"ipo_status": "pending"}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                upcoming = m.stocks.ipos(filters={"ipo_status": "pending"}).fetch()
        """
        return _list_query(
            self._core,
            IPOS,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def short_interest(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Short interest of US-listed stocks, one row per ticker and FINRA settlement date.

        Each row is the number of shares sold short and not yet covered on a settlement
        date, with the average daily volume and the days to cover, as reported to FINRA
        twice a month. Served by REST (`/stocks/v1/short-interest`) through `fetch()` and
        `poll()`; `poll()` replaces the table each cycle, keyed by `Symbol` and
        `SettlementDate`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#short_interest,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            start: The first settlement date to include, inclusive; None for no lower bound.
            end: The last settlement date to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"settlement_date"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"days_to_cover_gte": 5}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.short_interest("GME", start="2024-01-01").fetch()
        """
        return _list_query(
            self._core,
            SHORT_INTEREST,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def short_volume(
        self,
        symbols: Symbols | None = None,
        *,
        start: DateLike | None = None,
        end: DateLike | None = None,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Daily short sale volume of US-listed stocks, one row per ticker and trading day.

        Each row is a day's short volume, exempt and non-exempt volume, total volume, and
        short volume ratio, with the per-venue breakdown FINRA publishes (NYSE, Nasdaq
        Carteret and Chicago, and the ADF). Served by REST (`/stocks/v1/short-volume`)
        through `fetch()` and `poll()`; `poll()` replaces the table each cycle, keyed by
        `Symbol` and `Date`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#short_volume,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is
        not a date or ISO date, `end` is before `start`, or a filter is unknown, reserved, or of the
        wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            start: The first trading day to include, inclusive; None for no lower bound.
            end: The last trading day to include, inclusive. None (the default) sends no upper
                bound, so rows dated in the future are returned too; pass `end=` to stop at a date.
            sort: The field to sort by, such as `"date"`; None for the server default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"short_volume_ratio_gte": 50}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.short_volume(["AAPL", "TSLA"], start="2025-06-01").fetch()
        """
        return _list_query(
            self._core,
            SHORT_VOLUME,
            symbols=symbols,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    def share_float(
        self,
        symbols: Symbols | None = None,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """The free float of US-listed stocks: the latest count of shares the public can trade.

        Each row is one ticker's most recent free float, its percentage of shares
        outstanding, and the date it took effect (`EffectiveDate`). The endpoint has no date
        filter, so the dataset has no `start` or `end`. Served by REST (`/stocks/vX/float`)
        through `fetch()` and `poll()`; `poll()` replaces the table each cycle, keyed by
        `Symbol`.

        `limit` caps the total number of rows across pages; the page size is internal.
        `dm.ALL` is refused: pass `symbols=None` for every ticker. Columns:
        https://github.com/chipkent/deephaven-massive/blob/main/docs/datasets/stocks.md#share_float,
        the web UI's column tooltips, or `columns()`.

        Arguments are checked when the query is built: `TypeError` if an argument has the wrong
        type; `ValueError` if `symbols` is `dm.ALL`, empty, or holds an invalid ticker, or a filter
        is unknown, reserved, or of the wrong kind.

        Args:
            symbols: The tickers (one `str`, a sequence, or a `Table` with a `Symbol` column,
                re-read each `poll()` cycle); None for every ticker.
            sort: The field to sort by, such as `"free_float_percent"`; None for the server
                default.
            order: The direction of `sort`; without `sort` it raises `ValueError`. None for
                the server default.
            limit: The most rows to return in total, a positive `int`; None for every row.
            filters: Further query parameters of the endpoint by wire name with dots written
                as underscores, such as `{"free_float_percent_lt": 20}`; checked now.

        Returns:
            The query; nothing is requested until a verb runs.

        Example:
            ::

                t = m.stocks.share_float(["AAPL", "MSFT"]).fetch()
        """
        return _list_query(
            self._core,
            SHARE_FLOAT,
            symbols=symbols,
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )
