"""The `m.stocks.filings` namespace: SEC filings, insider ownership, and their taxonomies.

Datasets: the EDGAR filings index, 10-K sections, 8-K text and disclosures, risk factors,
institutional holdings (13-F), initial insider ownership (Form 3) and insider transactions
(Form 4), insider activity and holdings, and the risk-factor and disclosure taxonomies. The
rows of Forms 3 and 4, the insider activity and holdings, and the 8-K disclosures each list
several tickers, so those datasets have no `Symbol` column and keep `Tickers`; a filing
without a ticker keeps a null `Symbol`.

Each dataset is one factory method that validates its arguments and returns a query; nothing
touches the network until a verb is called on the query. This module owns the namespace's
`DatasetSpec`s, exported as `DATASETS` and composed into the dataset catalog (`CATALOG` in
`_session.py`). Every dataset is a REST list dataset: `symbols` is optional and only filters
the request (the 13-F holdings and the taxonomies take none), `dm.ALL` is refused in favour of
`symbols=None`, `start` and `end` bound the filing date where the dataset has one, and the
`sort`, `order`, `limit`, and `filters` block works as described in the package
documentation. `m.stocks` (`stocks.py`) creates the namespace.

The module is one subject although it runs past 1,000 lines: it is one namespace, and each
dataset's spec and its factory method, whose docstring is the dataset's documentation, belong
together.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, Order, PollMode
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.query_building import (
    EVERY_TICKER,
    Namespace,
    list_query,
    one_or_any_of,
    optional_text,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._query import Query
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike, FilterValue, Symbols

__all__ = [
    "DATASETS",
    "_StockFilings",
]

_R: Final = Transport.REST
_STOCKS: Final = AssetClass.STOCKS


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
"""Every dataset of `m.stocks.filings`, one `DatasetSpec` each."""


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
            start: The first filing date to include, inclusive; None for no lower bound.
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
        return list_query(
            self._core,
            FILINGS_INDEX,
            all_alternative=EVERY_TICKER,
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
            start: The first filing date to include, inclusive; None for no lower bound.
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
        return list_query(
            self._core,
            SECTIONS_10K,
            all_alternative=EVERY_TICKER,
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
            start: The first filing date to include, inclusive; None for no lower bound.
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
        return list_query(
            self._core,
            TEXT_8K,
            all_alternative=EVERY_TICKER,
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
            start: The first filing date to include, inclusive; None for no lower bound.
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
        return list_query(
            self._core,
            DISCLOSURES_8K,
            all_alternative=EVERY_TICKER,
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
            start: The first filing date to include, inclusive; None for no lower bound.
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
        return list_query(
            self._core,
            RISK_FACTORS,
            all_alternative=EVERY_TICKER,
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
            start: The first filing date to include, inclusive; None for no lower bound.
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
        return list_query(
            self._core,
            INSTITUTIONAL_HOLDINGS,
            all_alternative=EVERY_TICKER,
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
            start: The first filing date to include, inclusive; None for no lower bound.
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
            start: The first filing date to include, inclusive; None for no lower bound.
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
        return list_query(
            self._core,
            INSIDER_ACTIVITY,
            all_alternative=EVERY_TICKER,
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
        return list_query(
            self._core,
            RISK_FACTOR_TAXONOMY,
            all_alternative=EVERY_TICKER,
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
        return list_query(
            self._core,
            DISCLOSURE_TAXONOMY,
            all_alternative=EVERY_TICKER,
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

    The errors of `_ciks` and `list_query` propagate.
    """
    return list_query(
        core,
        spec,
        all_alternative=EVERY_TICKER,
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
