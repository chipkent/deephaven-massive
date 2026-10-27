"""Tests for the shared dataset factory and the query class of every dataset.

Two rules over the factory methods live here too. The annotation rule: for every dataset of
the catalog, the return annotation of its factory method is the class `query_class` derives
from the spec (for `bars`, each overload matches `bar_query_class` and the implementation
returns `HistoryQuery`), and every public factory method maps to exactly one dataset. The
summary rule: every factory's docstring starts with its `DatasetSpec.summary`. Each rule is a
function over a session class and a catalog, so it is checked against the real tree and
against synthetic namespaces that break it.
"""

import collections.abc
import datetime as dt
import typing
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, date, datetime
from typing import Any, Literal, cast, overload

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive import _enums, _query, _types
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, FuturesVenue, Order, Timespan
from deephaven_massive._massive import Transport
from deephaven_massive._namespaces.base import (
    Namespace,
    bar_query_class,
    build_query,
    list_params,
    query_class,
)
from deephaven_massive._query import (
    HistoryLatestQuery,
    HistoryQuery,
    Query,
    QueryBase,
    StreamHistoryLatestQuery,
    StreamHistoryQuery,
    StreamQuery,
)
from deephaven_massive._session import CATALOG
from deephaven_massive._types import DaySpan, StreamSpan

FF = Transport.FLAT_FILES
SESSION_FILES = "us_futures_{venue}/session_aggs_v1"
INDEX_FILES = "us_indices/minute_aggs_v1"

STOCK_BARS = DatasetSpec(
    name="stocks.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.STOCKS,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    fetch_path="/v2/aggs/ticker/{stocksTicker}/prev",
    history_path="/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}",
)
FUTURES_BARS = DatasetSpec(
    name="futures.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.FUTURES,
    channels={Timespan.SECOND: "A", Timespan.MINUTE: "AM"},
    history_path="/futures/v1/aggs/{ticker}",
    flat_file_prefixes={Timespan.SESSION: SESSION_FILES},
    keys={
        (FF, SESSION_FILES): SourceKeys(
            KeySpec("Ticker"), KeySpec("WindowStart"), session=KeySpec("SessionEndDate")
        )
    },
)
INDEX_BARS = DatasetSpec(
    name="indices.bars",
    summary="A dataset for the tests.",
    is_bars=True,
    asset_class=AssetClass.INDICES,
    flat_file_prefixes={Timespan.MINUTE: INDEX_FILES},
    keys={(FF, INDEX_FILES): SourceKeys(KeySpec("Ticker"), KeySpec("WindowStart"))},
)
TRADES_SPEC = DatasetSpec(
    name="stocks.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={None: "T"},
)
DIVIDENDS = DatasetSpec(
    name="stocks.dividends",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    fetch_path="/stocks/v1/dividends",
    keys={(Transport.REST, "/stocks/v1/dividends"): SourceKeys(KeySpec("Ticker"))},
    time_axis=("ex_dividend_date.gte", "ex_dividend_date.lte"),
    symbol_param="ticker.any_of",
)
INDICATOR = DatasetSpec(
    name="indicators.sma",
    summary="A dataset for the tests.",
    fetch_path="/v1/indicators/sma/{stockTicker}",
)


def build(session: dm.MassiveSession, spec: DatasetSpec, **kwargs: object) -> QueryBase:
    return build_query(session._core, spec, **cast("dict[str, Any]", kwargs))


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ((STOCK_BARS, Timespan.SECOND, 1), StreamHistoryQuery),
        ((STOCK_BARS, Timespan.MINUTE, 1), StreamHistoryQuery),
        ((STOCK_BARS, Timespan.DAY, 1), HistoryLatestQuery),
        ((STOCK_BARS, Timespan.MINUTE, 5), HistoryQuery),
        ((STOCK_BARS, Timespan.DAY, 2), HistoryQuery),
        ((STOCK_BARS, Timespan.HOUR, 1), HistoryQuery),
        ((FUTURES_BARS, Timespan.MINUTE, 1), StreamHistoryQuery),
        ((FUTURES_BARS, Timespan.SESSION, 1), HistoryQuery),
        ((INDEX_BARS, Timespan.DAY, 1), HistoryQuery),
    ],
)
def test_bar_query_class_selection(case: tuple[DatasetSpec, Timespan, int], expected: type) -> None:
    assert bar_query_class(*case) is expected


def test_bar_timespan_rules() -> None:
    with pytest.raises(ValueError, match="futures bars have no DAY timespan"):
        bar_query_class(FUTURES_BARS, Timespan.DAY, 1)
    with pytest.raises(ValueError, match="SESSION exists only for futures"):
        bar_query_class(STOCK_BARS, Timespan.SESSION, 1)
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan, e\.g\. dm\.Timespan\.DAY"):
        bar_query_class(STOCK_BARS, "day", 1)
    with pytest.raises(ValueError, match="multiplier must be positive"):
        bar_query_class(STOCK_BARS, Timespan.DAY, 0)


@pytest.mark.parametrize("multiplier", [True, 1.0])
def test_bar_multiplier_must_be_an_int(multiplier: object) -> None:
    with pytest.raises(TypeError, match="multiplier must be an int"):
        bar_query_class(STOCK_BARS, Timespan.DAY, multiplier)


def test_list_params() -> None:
    assert list_params(sort=None, order=None) == {}
    assert list_params(sort="ticker", order=Order.DESC) == {"sort": "ticker", "order": Order.DESC}
    with pytest.raises(TypeError, match="sort must be a str"):
        list_params(sort=cast("str", 1), order=None)
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        list_params(sort=None, order=cast("Order", "desc"))


def test_limit_is_a_row_cap_not_a_wire_parameter(massive_session: dm.MassiveSession) -> None:
    # The total-row cap is kept apart from the wire parameters, whose `limit` is a page size.
    q = build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=5)
    assert q._request.limit == 5
    assert "limit" not in dict(q._request.params)
    assert "limit=5" in repr(q)
    assert build(massive_session, DIVIDENDS, symbols=["AAPL"])._request.limit is None
    with pytest.raises(TypeError, match="limit must be an int"):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=cast("int", "10"))
    with pytest.raises(TypeError, match="limit must be an int"):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=True)
    with pytest.raises(ValueError, match="limit must be positive"):
        build(massive_session, DIVIDENDS, symbols=["AAPL"], limit=0)


def test_a_dataset_without_a_class_takes_any_class_by_prefix(
    massive_session: dm.MassiveSession,
) -> None:
    # Indicators, reference data, and the unified snapshot span asset classes.
    q = build(massive_session, INDICATOR, symbols=["X:BTCUSD", "i:spx", "aapl", "C:EURUSD"])
    assert q._request.symbols == ("AAPL", "C:EURUSD", "I:SPX", "X:BTCUSD")


def test_build_query_normalizes_the_request(massive_session: dm.MassiveSession) -> None:
    q = build(
        massive_session,
        DIVIDENDS,
        symbols=["msft", "AAPL"],
        start="2025-01-01",
        end=datetime(2025, 6, 30, tzinfo=UTC),
        params={"sort": "ex_dividend_date", "limit": None, "tickers": ["B", "A"]},
        filters={"frequency_lte": 12, "frequency": 4, "distribution_type": "recurring"},
    )
    assert isinstance(q, Query)
    request = q._request
    assert request.symbols == ("AAPL", "MSFT")
    assert request.start == date(2025, 1, 1)
    assert request.end == datetime(2025, 6, 30, tzinfo=UTC)
    assert request.params == (("sort", "ex_dividend_date"), ("tickers", ("B", "A")))
    assert request.filters == (
        ("distribution_type", "recurring"),
        ("frequency", 4),
        ("frequency_lte", 12),
    )
    assert hash(q)


def test_build_query_without_symbols_or_axis(massive_session: dm.MassiveSession) -> None:
    q = build(massive_session, INDICATOR, timespan=Timespan.DAY)
    assert type(q) is Query  # not a bars dataset: the timespan does not pick the class
    assert q._request.symbols is None
    assert q._request.timespan is Timespan.DAY


def test_symbols_follow_the_asset_class(massive_session: dm.MassiveSession) -> None:
    fx = DatasetSpec(
        name="forex.quotes",
        summary="A dataset for the tests.",
        asset_class=AssetClass.FOREX,
        channels={None: "C"},
    )
    assert build(massive_session, fx, symbols="EUR/USD")._request.symbols == ("C:EURUSD",)
    # A dataset without an asset class reads the class from each prefix; none is a stock.
    assert build(massive_session, INDICATOR, symbols="aapl")._request.symbols == ("AAPL",)


def test_all_where_the_dataset_cannot_serve_it(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"stocks\.dividends does not accept dm\.ALL; use X"):
        build(massive_session, DIVIDENDS, symbols=dm.ALL, all_alternative="X")
    assert build(massive_session, TRADES_SPEC, symbols=dm.ALL)._request.symbols is dm.ALL


def test_bars_pick_their_class(massive_session: dm.MassiveSession) -> None:
    q = build(massive_session, STOCK_BARS, symbols="AAPL", timespan=Timespan.MINUTE)
    assert type(q) is StreamHistoryQuery
    q = build(massive_session, FUTURES_BARS, symbols="ESZ5", timespan=Timespan.SESSION)
    assert type(q) is HistoryQuery
    assert type(build(massive_session, STOCK_BARS, symbols="AAPL")) is HistoryQuery


def test_venue_is_an_enum(massive_session: dm.MassiveSession) -> None:
    q = build(massive_session, FUTURES_BARS, symbols="ESZ5", venue=FuturesVenue.CME)
    assert q._request.venue is FuturesVenue.CME
    with pytest.raises(TypeError, match=r"venue must be dm\.FuturesVenue"):
        build(massive_session, FUTURES_BARS, symbols="ESZ5", venue="cme")


def test_filter_validation(massive_session: dm.MassiveSession) -> None:
    # Every form of the axis, not only the start/end pair, is set through start= and end=.
    for key in ("ex_dividend_date_gte", "ex_dividend_date", "ex_dividend_date_gt"):
        with pytest.raises(ValueError, match=rf"^{key} is set through start= and end="):
            build(massive_session, DIVIDENDS, filters={key: "2025-01-01"})
    with pytest.raises(TypeError, match="filters must be a mapping"):
        build(massive_session, DIVIDENDS, filters=[("a", 1)])
    with pytest.raises(TypeError, match="filter names must be str"):
        build(massive_session, DIVIDENDS, filters={1: "a"})
    with pytest.raises(TypeError, match="sequence of str; got dict"):
        build(massive_session, DIVIDENDS, filters={"a": {"b": 1}})
    with pytest.raises(TypeError, match=r"^a: element 1 of the sequence is int, not str$"):
        build(massive_session, DIVIDENDS, filters={"a": ["A", 1]})
    # A dataset without a time axis takes its timestamp filters through `filters`.
    assert build(
        massive_session, INDICATOR, filters={"timestamp_gte": "2025-01-02"}
    )._request.filters


def test_time_bounds_must_be_aware(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match=r"^start='2025-01-01T09:30' needs a timezone"):
        build(massive_session, DIVIDENDS, start="2025-01-01T09:30")


def test_naive_datetime_filters_and_params_are_rejected(
    massive_session: dm.MassiveSession,
) -> None:
    naive = datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None)  # naive on purpose
    message = (
        r"^timestamp_gte=datetime\.datetime\(2025, 1, 2, 9, 30\) needs a timezone: "
        r"pass an aware datetime or a date$"
    )
    with pytest.raises(ValueError, match=message):
        build(massive_session, INDICATOR, filters={"timestamp_gte": naive})
    with pytest.raises(ValueError, match=message):
        build(massive_session, INDICATOR, params={"timestamp_gte": naive})
    aware = naive.replace(tzinfo=UTC)
    assert build(massive_session, INDICATOR, filters={"timestamp_gte": aware})._request.filters


def test_namespaces_hold_the_core(massive_session: dm.MassiveSession) -> None:
    namespace = Namespace(massive_session._core)
    assert namespace._core is massive_session._core
    assert not hasattr(namespace, "_query")


def test_filters_go_through_the_catalog_check(massive_session: dm.MassiveSession) -> None:
    build(massive_session, DIVIDENDS, filters={"frequency": 4})
    with pytest.raises(ValueError, match="has no filter 'pay_date_lte'"):
        build(massive_session, DIVIDENDS, filters={"pay_date_lte": "2025-12-31"})
    with pytest.raises(ValueError, match="filter 'frequency' takes a integer value"):
        build(massive_session, DIVIDENDS, filters={"frequency": "4"})


def test_timespan_is_an_enum_on_every_dataset(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(TypeError, match=r"timespan must be dm\.Timespan, e\.g\. dm\.Timespan\.DAY"):
        build(massive_session, INDICATOR, timespan="day")


def test_namespaces_repr_as_their_path(massive_session: dm.MassiveSession) -> None:
    label = repr(massive_session).removeprefix("<").split(" open")[0]
    m = massive_session
    namespaces = {
        "stocks": m.stocks,
        "options": m.options,
        "futures": m.futures,
        "indices": m.indices,
        "forex": m.forex,
        "crypto": m.crypto,
        "reference": m.reference,
        "indicators": m.indicators,
        "economy": m.economy,
        "partners": m.partners,
        "partners.benzinga": m.partners.benzinga,
        "partners.etf_global": m.partners.etf_global,
        "partners.tmx": m.partners.tmx,
        "partners.fable": m.partners.fable,
    }
    for path, namespace in namespaces.items():
        assert repr(namespace) == f"<{label} {path}>"


def test_a_time_axis_must_not_end_before_it_starts(massive_session: dm.MassiveSession) -> None:
    with pytest.raises(ValueError, match="must be before end"):
        build(massive_session, DIVIDENDS, start="2025-02-01", end="2025-01-01")
    with pytest.raises(ValueError, match="must be before end"):
        build(
            massive_session,
            DIVIDENDS,
            start=datetime(2025, 1, 2, 10, tzinfo=UTC),
            end=datetime(2025, 1, 2, 9, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="must be before end"):
        build(massive_session, DIVIDENDS, start="2025-01-03", end=datetime(2025, 1, 2, tzinfo=UTC))
    # One whole day, an open-ended axis, and a datetime within the end day are all valid.
    assert build(massive_session, DIVIDENDS, start="2025-01-02", end="2025-01-02")._request.end
    assert build(massive_session, DIVIDENDS, start="2025-01-02")._request.end is None
    same_day = datetime(2025, 1, 2, 9, tzinfo=UTC)
    assert build(massive_session, DIVIDENDS, start="2025-01-02", end=same_day)._request.start


def test_adjusted_is_a_typed_bool(massive_session: dm.MassiveSession) -> None:
    spec = STOCK_BARS
    q = build_query(massive_session._core, spec, symbols=["AAPL"], adjusted=False)
    assert q._request.adjusted is False
    assert build_query(massive_session._core, spec, symbols=["AAPL"])._request.adjusted is None
    with pytest.raises(TypeError, match="adjusted must be a bool, not str"):
        build_query(massive_session._core, spec, symbols=["AAPL"], adjusted=cast("bool", "false"))
    # One channel only: adjusted never travels in params, where a transport could drop it.
    with pytest.raises(ValueError, match=r"adjusted is set through build_query\(adjusted=\.\.\.\)"):
        build_query(massive_session._core, spec, symbols=["AAPL"], params={"adjusted": False})


def test_adjusted_is_refused_in_filters(massive_session: dm.MassiveSession) -> None:
    # A filter would reach REST but not the source choice, which could then pick flat files,
    # whose bars are unadjusted.
    with pytest.raises(
        ValueError, match=r"adjusted is set through build_query\(adjusted=\.\.\.\), not filters"
    ):
        build_query(massive_session._core, STOCK_BARS, symbols=["AAPL"], filters={"adjusted": True})


def test_a_symbol_table_is_boxed_by_identity(
    massive_session: dm.MassiveSession, dh_exec_ctx: object
) -> None:
    table = new_table([string_col("Symbol", ["AAPL"])])
    q = build(massive_session, TRADES_SPEC, symbols=table)
    boxed = q._request.symbols
    assert isinstance(boxed, SymbolTable)
    assert boxed.table is table
    assert build(massive_session, TRADES_SPEC, symbols=table) == q


def test_a_symbol_table_needs_a_symbol_column(
    massive_session: dm.MassiveSession, dh_exec_ctx: object
) -> None:
    with pytest.raises(ValueError, match="a symbol table must have a Symbol column"):
        build(massive_session, TRADES_SPEC, symbols=new_table([string_col("Ticker", ["AAPL"])]))


# The query class of a dataset.

_FETCH = {"fetch_path": "/v2/last/trade/{stocksTicker}"}
_HISTORY = {"history_path": "/v3/trades/{stockTicker}"}
_CHANNELS = {"channels": {None: "T"}}


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({**_FETCH}, Query),
        ({**_CHANNELS}, StreamQuery),
        ({**_HISTORY}, HistoryQuery),
        ({"grouped_daily_path": "/v2/aggs/grouped/locale/us/market/stocks/{date}"}, HistoryQuery),
        ({**_CHANNELS, **_HISTORY}, StreamHistoryQuery),
        ({**_CHANNELS, **_HISTORY, **_FETCH}, StreamHistoryLatestQuery),
    ],
)
def test_the_query_class_follows_the_transports(
    changes: dict[str, Any], expected: type[QueryBase]
) -> None:
    spec = DatasetSpec(
        name="stocks.trades",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        **changes,
    )
    assert query_class(spec) is expected


def test_flat_files_are_a_history_source() -> None:
    assert query_class(INDEX_BARS) is HistoryQuery
    values = DatasetSpec(
        name="indices.values",
        summary="A dataset for the tests.",
        asset_class=AssetClass.INDICES,
        channels={None: "V"},
        flat_file_prefixes={None: "us_indices/values_v1"},
        keys={(FF, "us_indices/values_v1"): SourceKeys(KeySpec("Ticker"), KeySpec("Timestamp"))},
    )
    assert query_class(values) is StreamHistoryQuery


def test_a_bars_dataset_has_the_class_of_its_general_overload() -> None:
    # Every bars spec, whatever its transports; each call picks its own class.
    assert {query_class(STOCK_BARS), query_class(FUTURES_BARS)} == {HistoryQuery}


# The annotation rule: factories return the class their spec derives.

# Datasets served by a method of the session itself rather than of a namespace.
_SESSION_FACTORIES = {"reference.snapshots": "snapshots"}


def _hints(function: Callable[..., object]) -> dict[str, Any]:
    """Resolve a function's annotations, including names its module imports for typing only.

    Args:
        function: A factory, an overload of one, or a property getter.

    Returns:
        The annotations by parameter name, and `"return"`.
    """
    # A namespace module imports its typing-only names (`Mapping`, `Sequence`, `date`) under
    # `TYPE_CHECKING`, so they are not in its globals; supply them here.
    names = {
        **vars(collections.abc),
        **vars(dt),
        **vars(_query),
        **vars(_types),
        **vars(_enums),
        "Literal": Literal,
    }
    return typing.get_type_hints(function, localns=names)


def _factories(owner: type, path: str) -> Iterator[tuple[str, Callable[..., object]]]:
    """Yield every public factory method reachable from `owner`, by its dotted dataset path.

    A property whose return annotation is a `Namespace` subclass leads to a nested namespace
    (`m.stocks`, `m.partners.benzinga`, `m.stocks.financials`); every other public function
    defined on a namespace class is a factory.

    Args:
        owner: The session class or a namespace class.
        path: The dotted path of `owner` from the session, empty for the session.

    Yields:
        `(dataset path, factory)` pairs, such as `("stocks.trades", _Stocks.trades)`.
    """
    for name, member in vars(owner).items():
        if name.startswith("_"):
            continue
        dotted = f"{path}.{name}" if path else name
        if isinstance(member, property) and member.fget is not None:
            target = _hints(member.fget)["return"]
            if isinstance(target, type) and issubclass(target, Namespace):
                yield from _factories(target, dotted)
        elif callable(member) and issubclass(owner, Namespace):
            yield dotted, member


def _bar_problems(name: str, spec: DatasetSpec, factory: Callable[..., object]) -> list[str]:
    """Check each `bars` overload against `bar_query_class`, and the implementation.

    An overload's return type must be a base of the class `bar_query_class` picks for every
    timespan and multiplier its annotations allow, and that class itself for at least one.

    Args:
        name: The dataset name, for the messages.
        spec: The bars dataset.
        factory: Its factory method.

    Returns:
        One message per mismatch.
    """
    problems = []
    if _hints(factory)["return"] is not HistoryQuery:
        problems.append(f"{name}: the bars implementation must return HistoryQuery")
    overloads = typing.get_overloads(factory)
    if not overloads:
        problems.append(f"{name}: a bars factory needs its overloads")
    for variant in overloads:
        hints = _hints(variant)
        returned = hints["return"]
        spans = typing.get_args(hints["timespan"]) or tuple(Timespan)
        multipliers = typing.get_args(hints["multiplier"]) or (1, 2)
        picked = set()
        for span in spans:
            for multiplier in multipliers:
                try:
                    picked.add(bar_query_class(spec, span, multiplier))
                except ValueError:  # a timespan the asset class's endpoint does not accept
                    continue
        if returned not in picked or not all(issubclass(c, returned) for c in picked):
            problems.append(
                f"{name}: an overload returns {returned.__name__}, but its timespans give "
                f"{sorted(c.__name__ for c in picked)}"
            )
    return problems


def _factory_map(session: type) -> dict[str, Callable[..., object]]:
    """Return every factory method reachable from a session class, by dataset name.

    Args:
        session: The session class whose namespaces hold the factories.

    Returns:
        The namespace factories (`_factories`) and the session's own dataset methods.
    """
    factories = dict(_factories(session, ""))
    for dataset, method in _SESSION_FACTORIES.items():
        if hasattr(session, method):
            factories[dataset] = getattr(session, method)
    return factories


def summary_problems(session: type, specs: Mapping[str, DatasetSpec]) -> list[str]:
    """Return every factory whose docstring does not start with its dataset's summary.

    The summary is the sentence a result's table description and the column reference show, so
    the dataset method's docstring, which a console user reads, opens with the same words. A
    factory without a spec, or a spec without a factory, is the annotation rule's concern and
    is skipped here.

    Args:
        session: The session class whose namespaces hold the factories.
        specs: The catalog's specs by name.

    Returns:
        One message per factory whose docstring is missing or starts otherwise.
    """
    factories = _factory_map(session)
    problems = []
    for name, spec in specs.items():
        factory = factories.get(name)
        if factory is None:
            continue
        if not (factory.__doc__ or "").lstrip().startswith(spec.summary):
            problems.append(f"{name}: the docstring must start with {spec.summary!r}")
    return problems


def annotation_problems(session: type, specs: Mapping[str, DatasetSpec]) -> list[str]:
    """Return every break of the annotation rule.

    Args:
        session: The session class whose namespaces hold the factories.
        specs: The catalog's specs by name.

    Returns:
        One message per factory without a spec, spec without a factory, or factory whose return
        annotation is not the class its spec derives.
    """
    factories = _factory_map(session)
    problems = [f"{name}: a factory with no dataset" for name in sorted(factories - specs.keys())]
    for name, spec in specs.items():
        factory = factories.get(name)
        if factory is None:
            problems.append(f"{name}: a dataset with no factory")
        elif spec.is_bars:
            problems.extend(_bar_problems(name, spec, factory))
        elif _hints(factory)["return"] is not query_class(spec):
            problems.append(f"{name}: the factory must return {query_class(spec).__name__}")
    return problems


def test_every_factory_returns_the_class_of_its_dataset() -> None:
    assert annotation_problems(dm.MassiveSession, CATALOG.by_name) == []


class _Good(Namespace):
    __slots__ = ()

    @overload
    def bars(
        self, timespan: StreamSpan = Timespan.MINUTE, *, multiplier: Literal[1] = 1
    ) -> StreamHistoryQuery: ...
    @overload
    def bars(self, timespan: DaySpan, *, multiplier: Literal[1] = 1) -> HistoryLatestQuery: ...
    @overload
    def bars(
        self, timespan: Timespan = Timespan.MINUTE, *, multiplier: int = 1
    ) -> HistoryQuery: ...
    def bars(self, timespan: Timespan = Timespan.MINUTE, *, multiplier: int = 1) -> HistoryQuery:
        raise NotImplementedError

    def dividends(self) -> Query:
        raise NotImplementedError


class _Bad(Namespace):
    __slots__ = ()

    @overload
    def bars(self, timespan: DaySpan, *, multiplier: Literal[1] = 1) -> StreamHistoryQuery: ...
    @overload
    def bars(
        self, timespan: Timespan = Timespan.MINUTE, *, multiplier: int = 1
    ) -> HistoryQuery: ...
    def bars(self, timespan: Timespan = Timespan.MINUTE, *, multiplier: int = 1) -> QueryBase:
        raise NotImplementedError

    def trades(self) -> Query:  # the trades dataset streams, so this must be a StreamQuery
        raise NotImplementedError

    def news(self) -> Query:  # no dataset is named options.news
        raise NotImplementedError

    def minute_bars(self) -> HistoryQuery:  # a bars dataset served without its overloads
        raise NotImplementedError


class _GoodSession:
    @property
    def stocks(self) -> _Good:
        raise NotImplementedError

    @property
    def label(self) -> str:  # a property that leads to no namespace
        raise NotImplementedError

    def snapshots(self) -> Query:
        raise NotImplementedError


class _BadSession:
    @property
    def options(self) -> _Bad:
        raise NotImplementedError

    def snapshots(self) -> Query:  # no reference.snapshots dataset
        raise NotImplementedError


def _renamed(spec: DatasetSpec, name: str) -> DatasetSpec:
    return DatasetSpec(
        name=name,
        summary="A dataset for the tests.",
        is_bars=spec.is_bars,
        asset_class=spec.asset_class,
        channels=spec.channels,
        fetch_path=spec.fetch_path,
        history_path=spec.history_path,
    )


def _catalog(*specs: tuple[DatasetSpec, str]) -> dict[str, DatasetSpec]:
    return {name: _renamed(spec, name) for spec, name in specs}


def test_the_annotation_rule_passes_matching_factories() -> None:
    specs = _catalog(
        (STOCK_BARS, "stocks.bars"),
        (DIVIDENDS, "stocks.dividends"),
        (DIVIDENDS, "reference.snapshots"),  # served by the session's own `snapshots()`
    )
    assert annotation_problems(_GoodSession, specs) == []


def test_the_annotation_rule_reports_every_mismatch() -> None:
    specs = _catalog(
        (STOCK_BARS, "options.bars"),
        (TRADES_SPEC, "options.trades"),
        (STOCK_BARS, "options.minute_bars"),
        (DIVIDENDS, "options.splits"),
    )
    assert annotation_problems(_BadSession, specs) == [
        "options.news: a factory with no dataset",
        "reference.snapshots: a factory with no dataset",
        "options.bars: the bars implementation must return HistoryQuery",
        (
            "options.bars: an overload returns StreamHistoryQuery, but its timespans give "
            "['HistoryLatestQuery']"
        ),
        "options.trades: the factory must return StreamQuery",
        "options.minute_bars: a bars factory needs its overloads",
        "options.splits: a dataset with no factory",
    ]


# The summary rule: each dataset method's docstring starts with its dataset's summary.


def test_every_factory_docstring_starts_with_its_summary() -> None:
    assert summary_problems(dm.MassiveSession, CATALOG.by_name) == []


class _Summarized(Namespace):
    __slots__ = ()

    def dividends(self) -> Query:
        """Cash dividends of US-listed stocks. The rest of the summary line.

        The rest of the docstring.
        """
        raise NotImplementedError

    def splits(self) -> Query:
        """Something else entirely."""
        raise NotImplementedError

    def news(self) -> Query:
        raise NotImplementedError


class _SummarizedSession:
    @property
    def stocks(self) -> _Summarized:
        raise NotImplementedError


def _summarized(name: str, summary: str) -> DatasetSpec:
    return DatasetSpec(name=name, summary=summary, fetch_path="/v3/reference/dividends")


def test_the_summary_rule_reports_every_mismatch() -> None:
    specs = {
        spec.name: spec
        for spec in (
            _summarized("stocks.dividends", "Cash dividends of US-listed stocks."),
            _summarized("stocks.splits", "Stock splits of US-listed stocks."),
            _summarized("stocks.news", "News articles."),
            _summarized("stocks.ipos", "Initial public offerings."),  # no factory: skipped
        )
    }
    assert summary_problems(_SummarizedSession, specs) == [
        "stocks.splits: the docstring must start with 'Stock splits of US-listed stocks.'",
        "stocks.news: the docstring must start with 'News articles.'",
    ]
