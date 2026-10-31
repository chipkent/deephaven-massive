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
`symbols=None`, `start` and `end` bound the filing date where the dataset has one, and
`sort`, `order`, `limit`, and `filters` work as README.md#sort-order-limit and
README.md#filters describe. `m.stocks` (`stocks.py`) creates the namespace.

The module is one subject although it runs past 1,000 lines: it is one namespace, and each
dataset's spec and its factory method, whose docstring is the dataset's documentation, belong
together.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, Order, PollMode
from deephaven_massive._errors import raises_through
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
    page_size: int | None = None,
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
        page_size: A page size below the largest `limit` the path documents, for rows that
            carry long text; None, the default, requests that largest `limit`.

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
    summary="The index of SEC EDGAR filings by US-listed companies, one row per filing and filer.",
)

SECTIONS_10K: Final = _filing(
    "sections_10k",
    _SECTIONS_10K,
    symbol_param="ticker.any_of",
    poll_keys=("FilingUrl", "Section"),
    summary="The text of sections of 10-K annual reports, one row per filing and section.",
)

TEXT_8K: Final = _filing(
    "text_8k",
    _TEXT_8K,
    symbol_param="ticker.any_of",
    # A joint 8-K of several co-registrants can come once under each filer's CIK, as the
    # index lists it.
    poll_keys=("Cik", "AccessionNumber"),
    summary="The item text of 8-K current reports, one row per filing and filer.",
)

DISCLOSURES_8K: Final = _filing(
    "disclosures_8k",
    _DISCLOSURES_8K,
    symbol_param="tickers.any_of",
    poll_keys=("AccessionNumber", *_CATEGORIES, "SupportingText"),
    keys=_NO_KEYS,
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
    summary="Holdings that institutional managers report on Form 13-F, one row per position.",
)

INITIAL_INSIDER_OWNERSHIP: Final = _filing(
    "initial_insider_ownership",
    _FORM_3,
    symbol_param="tickers.any_of",
    poll_keys=(*_INSIDER_HOLDING, "ExercisePrice", "ExerciseDate"),
    nullable_poll_keys={**_INDIRECT, **_DERIVATIVE, "ExerciseDate": _DERIVATIVE_ONLY},
    keys=_NO_KEYS,
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
)

RISK_FACTOR_TAXONOMY: Final = DatasetSpec(
    name="stocks.filings.risk_factor_taxonomy",
    summary="The taxonomy that classifies filing risk factors, one row per category.",
    fetch_path=_RISK_TAXONOMY,
    poll_keys=("Taxonomy", *_CATEGORIES),
)

DISCLOSURE_TAXONOMY: Final = DatasetSpec(
    name="stocks.filings.disclosure_taxonomy",
    summary="The taxonomy that classifies 8-K disclosures, one row per category.",
    fetch_path=_DISCLOSURE_TAXONOMY,
    poll_keys=("Taxonomy", *_CATEGORIES),
)


@raises_through(one_or_any_of)
def _ciks(**ciks: object) -> dict[str, object]:
    """Return the wire parameters of the insider datasets' CIK arguments.

    Every insider path takes each CIK both alone and as `.any_of`, so one CIK is sent as
    `issuer_cik` (or `owner_cik`) and a sequence as `issuer_cik.any_of`.

    Args:
        ciks: Each CIK argument by name (`issuer_cik`, `owner_cik`) as passed: one CIK, a
            sequence of CIKs, or None.

    Returns:
        Both wire forms of each argument, the unset one None.

    Raises:
        TypeError: If a CIK is neither text, a sequence, nor None (`one_or_any_of`).
        ValueError: If a CIK is an empty sequence (`one_or_any_of`).
    """
    params: dict[str, object] = {}
    for name, value in ciks.items():
        params |= one_or_any_of(name, value, every=f"every {name.removesuffix('_cik')}")
    return params


@raises_through(_ciks, list_query)
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

    Raises:
        TypeError: If a CIK or another argument has the wrong type (`_ciks`, `list_query`).
        ValueError: If a CIK is an empty sequence, or an argument has a value the dataset
            refuses (`_ciks`, `list_query`).
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

    @raises_through(list_query, optional_text)
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

        Each row names a filing's form type, filer CIK, issuer, ticker, filing date, and EDGAR URL.
        A filing with several filers has a row under each filer's CIK, as EDGAR's index lists it.
        `poll()` appends new rows, told apart by `Cik` and `AccessionNumber`, and needs a `start` or
        a `limit`. A filing whose company has no ticker keeps a null `Symbol`.

        Args:
            symbols: Stock tickers; None, the default, for every filer. See README.md#symbols.
            form_type: The SEC form type, such as `"10-K"`; None, the default, for every form.
            start: The first filing date. See README.md#start-and-end.
            end: The last filing date. See README.md#start-and-end.
            sort: The field to sort by, such as `"filing_date"`. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters, such as `{"cik": "0000320193"}`. See
                README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.index.md.

        Raises:
            TypeError: If an argument has the wrong type, such as a non-`str` form type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

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

    @raises_through(list_query)
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
        10-K, with its ticker, CIK, period end, and filing date. `poll()` appends new rows, told
        apart by the dataset's key columns, and needs a `start` or a `limit`. A filing whose company
        has no ticker keeps a null `Symbol`.

        Args:
            symbols: Stock tickers; None, the default, for every filer. See README.md#symbols.
            start: The first filing date. See README.md#start-and-end.
            end: The last filing date. See README.md#start-and-end.
            sort: The field to sort by, such as `"filing_date"`. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters, such as `{"section": "risk_factors"}`. See
                README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.sections_10k.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

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

    @raises_through(list_query)
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

        Each row holds the text of the items a company reported on an 8-K or an amendment, with its
        ticker, CIK, form type, and filing date. A joint 8-K of several co-registrants can have a
        row under each filer's CIK. `poll()` appends new rows, told apart by `Cik` and
        `AccessionNumber`, and needs a `start` or a `limit`. A filing whose company has no ticker
        keeps a null `Symbol`.

        Args:
            symbols: Stock tickers; None, the default, for every filer. See README.md#symbols.
            start: The first filing date. See README.md#start-and-end.
            end: The last filing date. See README.md#start-and-end.
            sort: The field to sort by, such as `"filing_date"`. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters, such as `{"form_type": "8-K/A"}`. See
                README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.text_8k.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

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

    @raises_through(list_query)
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

        Each row is one disclosure of an 8-K, with its primary, secondary, and tertiary categories
        (see `disclosure_taxonomy()`), the supporting text, and the filing's accession number and
        date. A filing can name several share classes, so a row has no `Symbol`: its `Tickers`
        column holds the filing's tickers, and `symbols` selects the filings that list any of them.
        `poll()` appends new rows, told apart by the dataset's key columns, and needs a `start` or a
        `limit`.

        Args:
            symbols: Tickers whose filings to return; None, the default, for every filing. See
                README.md#symbols.
            start: The first filing date. See README.md#start-and-end.
            end: The last filing date. See README.md#start-and-end.
            sort: The field to sort by, such as `"filing_date"`. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters, such as `{"tertiary_category": "Dividends"}`. See
                README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.disclosures_8k.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL` or empty, a bound is not a date or ISO date, `end`
                is before `start`, `order` is given without `sort`, or a filter is unknown,
                reserved, or of the wrong kind.

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

    @raises_through(list_query)
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
        categories (see `risk_factor_taxonomy()`), the supporting text, and the filing date.
        `poll()` appends new rows, told apart by the dataset's key columns, and needs a `start` or a
        `limit`. A filing whose company has no ticker keeps a null `Symbol`.

        Args:
            symbols: Stock tickers; None, the default, for every filer. See README.md#symbols.
            start: The first filing date. See README.md#start-and-end.
            end: The last filing date. See README.md#start-and-end.
            sort: The field to sort by, such as `"filing_date"`. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters, such as `{"cik": "0000320193"}`. See
                README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.risk_factors.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `symbols` is `dm.ALL`, empty, or holds an invalid ticker, a bound is not
                a date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

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

    @raises_through(list_query, one_or_any_of)
    def institutional_holdings(
        self,
        *,
        filer_cik: str | Sequence[str] | None = None,
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
        the dataset takes no `symbols`. `poll()` appends new rows, told apart by the dataset's key
        columns, and needs a `start` or a `limit`.

        Args:
            filer_cik: The filing manager's SEC CIK, as text with its leading zeros, such as
                `"0001067983"`, or a sequence of them; None, the default, for every filer.
            start: The first filing date. See README.md#start-and-end.
            end: The last filing date. See README.md#start-and-end.
            sort: The field to sort by, such as `"filing_date"`. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters. See README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.institutional_holdings.md.

        Raises:
            TypeError: If an argument has the wrong type, such as an `int` CIK.
            ValueError: If a CIK sequence is empty, a bound is not a date or ISO date, `end` is
                before `start`, `order` is given without `sort`, or a filter is unknown, reserved,
                or of the wrong kind.

        Example:
            ::

                t = m.stocks.filings.institutional_holdings(filer_cik="0001067983").fetch()
        """
        return list_query(
            self._core,
            INSTITUTIONAL_HOLDINGS,
            all_alternative=EVERY_TICKER,
            start=start,
            end=end,
            params=one_or_any_of("filer_cik", filer_cik, every="every filer"),
            sort=sort,
            order=order,
            limit=limit,
            filters=filters,
        )

    @raises_through(_insider_query)
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
        the filing's tickers, and `symbols` selects the filings that list any of them. `poll()`
        appends new rows, told apart by the dataset's key columns, and needs a `start` or a `limit`.

        Args:
            symbols: Tickers whose filings to return; None, the default, for every filing. See
                README.md#symbols.
            issuer_cik: The issuing company's SEC CIK, as text with its leading zeros, or a sequence
                of them; None, the default, for every issuer.
            owner_cik: The reporting owner's SEC CIK, one or a sequence, as `issuer_cik`; None, the
                default, for every owner.
            start: The first filing date. See README.md#start-and-end.
            end: The last filing date. See README.md#start-and-end.
            sort: The field to sort by, such as `"filing_date"`. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters. See README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.initial_insider_ownership.md.

        Raises:
            TypeError: If an argument has the wrong type, such as an `int` CIK.
            ValueError: If `symbols` is `dm.ALL` or empty, a CIK sequence is empty, a bound is not a
                date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

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

    @raises_through(_insider_query)
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
        classes, so a row has no `Symbol`: its `Tickers` column holds the filing's tickers, and
        `symbols` selects the filings that list any of them. `poll()` appends new rows, told apart
        by the dataset's key columns, and needs a `start` or a `limit`.

        Args:
            symbols: Tickers whose filings to return; None, the default, for every filing. See
                README.md#symbols.
            issuer_cik: The issuing company's SEC CIK, as text with its leading zeros, or a sequence
                of them; None, the default, for every issuer.
            owner_cik: The reporting owner's SEC CIK, one or a sequence, as `issuer_cik`; None, the
                default, for every owner.
            start: The first filing date. See README.md#start-and-end.
            end: The last filing date. See README.md#start-and-end.
            sort: The field to sort by, such as `"filing_date"`. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters, such as `{"transaction_code": "P"}`. See
                README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.insider_transactions.md.

        Raises:
            TypeError: If an argument has the wrong type, such as an `int` CIK.
            ValueError: If `symbols` is `dm.ALL` or empty, a CIK sequence is empty, a bound is not a
                date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

        Example:
            ::

                buys = m.stocks.filings.insider_transactions("AAPL", start="2025-01-01").fetch()
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

    @raises_through(list_query, _ciks)
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

        Insiders (directors, officers, and holders of over 10%) report each trade on SEC Form 4 or
        5. Massive sums one issuer's insider buys and sells of one day into one row: counts, shares
        and dollar values, their net (`NetShares`, `NetValue`, negative for net selling), and the
        part traded under a prearranged Rule 10b5-1 plan. `insider_transactions()` returns the Form
        4 lines themselves. A company can have several share classes, so a row has no `Symbol`: its
        `Tickers` column holds the issuer's tickers, and `symbols` selects the issuers that list any
        of them. The totals cover the issuer, not each ticker. `poll()` replaces the table each
        cycle, keyed by `IssuerCik` and `TransactionDate`, so a late filing revises its day in
        place. Massive updates the data daily.

        Args:
            symbols: Tickers whose issuers to return; None, the default, for every issuer. See
                README.md#symbols.
            issuer_cik: The issuing company's SEC CIK, as text with its leading zeros, or a sequence
                of them; None, the default, for every issuer.
            start: The first transaction date. See README.md#start-and-end.
            end: The last transaction date. See README.md#start-and-end.
            sort: The field to sort by, such as `"net_value"`; the default is the newest transaction
                date first. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Unused today: the arguments above reserve every endpoint parameter, so any key
                raises `ValueError`.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.insider_activity.md.

        Raises:
            TypeError: If an argument has the wrong type, such as an `int` CIK.
            ValueError: If `symbols` is `dm.ALL` or empty, a CIK sequence is empty, a bound is not a
                date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

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

    @raises_through(_insider_query)
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

        Massive combines one insider's Form 3, 4, and 5 filings for one issuer into one current row:
        the owner, roles, and title; the first and latest filing dates; the shares owned directly,
        indirectly, and in total, as filed (not split-adjusted, no derivatives); and `OwnershipPct`,
        in percent (0.0198 means 0.0198%). `OldestPositionDate` far behind `LastFilingDate` warns
        that a position may be stale. A company can have several share classes, so a row has no
        `Symbol`: its `Tickers` column holds the issuer's tickers, and `symbols` selects the issuers
        that list any of them. `poll()` replaces the table each cycle, keyed by `IssuerCik` and
        `OwnerCik`, so a new filing updates its insider's row in place. Massive updates the data
        daily.

        Args:
            symbols: Tickers whose issuers' insiders to return; None, the default, for every issuer.
                See README.md#symbols.
            issuer_cik: The issuing company's SEC CIK, as text with its leading zeros, or a sequence
                of them; None, the default, for every issuer.
            owner_cik: The insider's SEC CIK, or a sequence of them, to list every company one
                person is an insider of; None, the default, for every insider.
            start: The earliest latest-filing date (`LastFilingDate`); an insider who has not filed
                since is left out. See README.md#start-and-end.
            end: The last latest-filing date. See README.md#start-and-end.
            sort: The field to sort by, such as `"shares_owned"`; the default is the most recent
                latest-filing date first. See README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Unused today: the arguments above reserve every endpoint parameter, so any key
                raises `ValueError`.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.insider_holdings.md.

        Raises:
            TypeError: If an argument has the wrong type, such as an `int` CIK.
            ValueError: If `symbols` is `dm.ALL` or empty, a CIK sequence is empty, a bound is not a
                date or ISO date, `end` is before `start`, `order` is given without `sort`, or a
                filter is unknown, reserved, or of the wrong kind.

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

    @raises_through(list_query)
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
        `risk_factors()` come from it. The dataset takes no `symbols`. `poll()` replaces the table
        each cycle, keyed by `Taxonomy` and the three categories.

        Args:
            sort: The field to sort by, such as `"primary_category"`. See
                README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters, such as `{"primary_category": "Financial"}`. See
                README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.risk_factor_taxonomy.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `order` is given without `sort`, or a filter is unknown, reserved, or of
                the wrong kind.

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

    @raises_through(list_query)
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
        `disclosures_8k()` come from it. The dataset takes no `symbols`. `poll()` replaces the table
        each cycle, keyed by `Taxonomy` and the three categories.

        Args:
            sort: The field to sort by, such as `"primary_category"`. See
                README.md#sort-order-limit.
            order: The direction of `sort`. See README.md#sort-order-limit.
            limit: The most rows to return, in total. See README.md#sort-order-limit.
            filters: Further endpoint parameters, such as `{"taxonomy": "1.0"}`. See
                README.md#filters.

        Returns:
            A query. Columns: docs/datasets/stocks.filings.disclosure_taxonomy.md.

        Raises:
            TypeError: If an argument has the wrong type.
            ValueError: If `order` is given without `sort`, or a filter is unknown, reserved, or of
                the wrong kind.

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
