"""Tests for the `m.partners` namespace: its four vendor namespaces and their datasets.

Every partner dataset is a REST list endpoint. The tests check each declaration against the
generated sources (the catalog gates, the schema each spec yields, the fixtures shaped from the
vendor's samples) and each factory's validation and the request it builds.
"""

import inspect
import json
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, cast

import pytest
from deephaven import new_table
from deephaven.column import string_col

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
from deephaven_massive._datasets.schema import ColumnType
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import Order, PollMode
from deephaven_massive._massive import Transport, sources
from deephaven_massive._namespaces import partners
from deephaven_massive._namespaces.base import Namespace, query_class
from deephaven_massive._query import Query
from deephaven_massive._session import CATALOG

if TYPE_CHECKING:
    from deephaven_massive._types import FilterValue

FIXTURES = "namespaces/partners"

# Each dataset's factory below `m.partners`, the REST path it reads, its `Symbol` source
# column (None for no `Symbol`), and the fixture shaped from the vendor's sample.
DATASETS = {
    "partners.benzinga.analysts": ("/benzinga/v1/analysts", None),
    "partners.benzinga.analyst_insights": ("/benzinga/v1/analyst-insights", "ticker"),
    "partners.benzinga.ratings": ("/benzinga/v1/ratings", "ticker"),
    "partners.benzinga.consensus_ratings": ("/benzinga/v1/consensus-ratings/{ticker}", "ticker"),
    "partners.benzinga.bulls_bears_say": ("/benzinga/v1/bulls-bears-say", "ticker"),
    "partners.benzinga.guidance": ("/benzinga/v1/guidance", "ticker"),
    "partners.benzinga.earnings": ("/benzinga/v1/earnings", "ticker"),
    "partners.benzinga.firms": ("/benzinga/v1/firms", None),
    "partners.benzinga.news": ("/benzinga/v2/news", None),
    "partners.etf_global.analytics": ("/etf-global/v1/analytics", "composite_ticker"),
    "partners.etf_global.constituents": ("/etf-global/v1/constituents", "composite_ticker"),
    "partners.etf_global.fund_flows": ("/etf-global/v1/fund-flows", "composite_ticker"),
    "partners.etf_global.profiles": ("/etf-global/v1/profiles", "composite_ticker"),
    "partners.etf_global.taxonomies": ("/etf-global/v1/taxonomies", "composite_ticker"),
    "partners.tmx.corporate_events": ("/tmx/v1/corporate-events", "ticker"),
    "partners.fable.merchant_aggregates": ("/consumer-spending/eu/v1/merchant-aggregates", None),
    "partners.fable.merchant_hierarchy": ("/consumer-spending/eu/v1/merchant-hierarchy", None),
}
WITH_SYMBOLS = [name for name, (_, symbol) in DATASETS.items() if symbol is not None]
LIST_WITH_SYMBOLS = [n for n in WITH_SYMBOLS if n != "partners.benzinga.consensus_ratings"]
WITHOUT_SYMBOLS = [
    "partners.benzinga.analysts",
    "partners.benzinga.firms",
    "partners.fable.merchant_aggregates",
    "partners.fable.merchant_hierarchy",
]
# The documented maximum `limit` of each path, which the page size requests.
SMALL_PAGES = {
    "partners.benzinga.bulls_bears_say",
    "partners.etf_global.analytics",
    "partners.etf_global.constituents",
    "partners.etf_global.fund_flows",
    "partners.etf_global.profiles",
    "partners.etf_global.taxonomies",
    "partners.fable.merchant_aggregates",
}


def spec_of(name: str) -> DatasetSpec:
    return CATALOG.by_name[name]


def factory(session: dm.MassiveSession, name: str) -> Callable[..., Query]:
    # "partners.benzinga.ratings" is reached as session.partners.benzinga.ratings.
    target: Any = session
    for part in name.split("."):
        target = getattr(target, part)
    return cast("Callable[..., Query]", target)


def fixture_rows(load_fixture: Callable[[str, str], bytes], name: str) -> list[dict[str, Any]]:
    stem = name.removeprefix("partners.").replace(".", "_")
    rows: list[dict[str, Any]] = json.loads(load_fixture(FIXTURES, f"{stem}.json"))["results"]
    return rows


# The namespace tree.


def test_vendors_are_nested(massive_session: dm.MassiveSession) -> None:
    vendors = massive_session.partners
    assert type(vendors) is partners._Partners
    assert type(vendors.benzinga) is partners._Benzinga
    assert type(vendors.etf_global) is partners._EtfGlobal
    assert type(vendors.tmx) is partners._Tmx
    assert type(vendors.fable) is partners._Fable
    for vendor in (vendors.benzinga, vendors.etf_global, vendors.tmx, vendors.fable):
        assert isinstance(vendor, Namespace)
        assert vendor._core is massive_session._core


def test_every_partner_dataset_is_declared_once() -> None:
    assert [spec.name for spec in partners.DATASETS] == list(DATASETS)
    for spec in partners.DATASETS:
        assert CATALOG.by_name[spec.name] is spec


@pytest.mark.parametrize("name", DATASETS)
def test_each_dataset_is_one_rest_list_path(name: str) -> None:
    spec = spec_of(name)
    path, _ = DATASETS[name]
    assert spec.fetch_path == path
    assert spec.sources == ((Transport.REST, path),)
    assert query_class(spec) is Query
    assert spec.kind is None


def test_the_partner_datasets_pass_every_catalog_gate() -> None:
    assert Catalog(partners.DATASETS).problems() == ()


@pytest.mark.parametrize("name", DATASETS)
def test_the_page_size_is_the_documented_maximum(name: str) -> None:
    assert spec_of(name).page_size == (5_000 if name in SMALL_PAGES else 50_000)


# The schemas the declarations yield.


@pytest.mark.parametrize("name", DATASETS)
def test_symbol_is_the_one_instrument_of_a_row(name: str) -> None:
    path, symbol = DATASETS[name]
    schema = CATALOG.dataset_schema(spec_of(name))
    names = schema.column_names
    fields = {f.wire_key: f.column for f in sources(Transport.REST)[path].fields}
    assert "Underlying" not in names
    if symbol is None:
        assert "Symbol" not in names
        assert not CATALOG.takes_symbols(spec_of(name))
    else:
        assert names[0] == "Symbol"
        assert fields[symbol] not in names  # renamed, not repeated
        assert CATALOG.takes_symbols(spec_of(name))
    # Every other generated field is a column under its generated name.
    others = {column for key, column in fields.items() if key != symbol}
    if name == "partners.benzinga.news":
        others -= {"Published"}
    assert others <= set(names)


def test_news_keeps_its_tickers_and_is_timed_by_publication() -> None:
    spec = spec_of("partners.benzinga.news")
    schema = CATALOG.dataset_schema(spec)
    types = {column.name: column.type for column in schema.columns}
    assert schema.column_names[0] == "Timestamp"
    assert types["Timestamp"] is ColumnType.INSTANT
    assert "Published" not in types
    assert types["Tickers"] is ColumnType.STRING  # the JSON list, never a Symbol
    assert spec.asset_class is None
    assert spec.time_axis == ("published.gte", "published.lt")
    assert spec.symbol_param == "tickers.any_of"


@pytest.mark.parametrize("name", [n for n in DATASETS if n != "partners.benzinga.news"])
def test_only_news_has_a_timestamp(name: str) -> None:
    assert "Timestamp" not in CATALOG.dataset_schema(spec_of(name)).column_names


@pytest.mark.parametrize(
    ("name", "column"),
    [
        ("partners.tmx.corporate_events", "Date"),
        ("partners.fable.merchant_aggregates", "TransactionDate"),
        ("partners.etf_global.fund_flows", "EffectiveDate"),
        ("partners.benzinga.ratings", "Date"),
    ],
)
def test_a_calendar_date_keeps_its_own_column(name: str, column: str) -> None:
    types = {c.name: c.type for c in CATALOG.dataset_schema(spec_of(name)).columns}
    assert types[column] is ColumnType.LOCAL_DATE


@pytest.mark.parametrize("name", DATASETS)
def test_poll_keys_are_columns_and_news_appends(name: str) -> None:
    spec = spec_of(name)
    assert set(spec.poll_keys) <= set(CATALOG.dataset_schema(spec).column_names)
    expected = PollMode.APPEND if name == "partners.benzinga.news" else PollMode.REPLACE
    assert spec.poll_mode is expected


# The fixtures, shaped from the vendor's samples.


@pytest.mark.parametrize("name", DATASETS)
def test_each_fixture_has_every_generated_field(
    name: str, load_fixture: Callable[[str, str], bytes]
) -> None:
    path, symbol = DATASETS[name]
    rows = fixture_rows(load_fixture, name)
    fields = sources(Transport.REST)[path].fields
    assert rows
    for row in rows:
        assert set(row) == {f.wire_key for f in fields}
        if symbol is not None:
            assert row[symbol]
    # The poll keys tell the rows apart.
    wire_keys = {f.column: f.wire_key for f in fields}
    if symbol is not None:
        wire_keys["Symbol"] = symbol
    keys = [wire_keys[column] for column in spec_of(name).poll_keys]
    assert len({tuple(row[key] for key in keys) for row in rows}) == len(rows)


# The factories.


@pytest.mark.parametrize("name", LIST_WITH_SYMBOLS)
def test_list_factories_build_the_request(massive_session: dm.MassiveSession, name: str) -> None:
    q = factory(massive_session, name)(["msft", "AAPL"], sort="date", order=Order.DESC, limit=10)
    assert type(q) is Query
    assert q._spec is spec_of(name)
    request = q._request
    assert request.symbols == ("AAPL", "MSFT")
    assert request.params == (("order", Order.DESC), ("sort", "date"))
    assert request.limit == 10
    assert request.filters == ()
    assert request.start is None
    assert request.end is None


@pytest.mark.parametrize("name", [*LIST_WITH_SYMBOLS, *WITHOUT_SYMBOLS])
def test_every_list_factory_defaults_to_no_filter(
    massive_session: dm.MassiveSession, name: str
) -> None:
    request = factory(massive_session, name)()._request
    assert request.symbols is None
    assert request.params == ()
    assert request.limit is None


@pytest.mark.parametrize("name", WITHOUT_SYMBOLS)
def test_datasets_without_instruments_take_no_symbols(name: str) -> None:
    method = partners.__dict__[_class_of(name)].__dict__[name.rsplit(".", 1)[1]]
    assert "symbols" not in inspect.signature(method).parameters


def _class_of(name: str) -> str:
    vendor = name.split(".")[1]
    return {
        "benzinga": "_Benzinga",
        "etf_global": "_EtfGlobal",
        "tmx": "_Tmx",
        "fable": "_Fable",
    }[vendor]


@pytest.mark.parametrize("name", [*LIST_WITH_SYMBOLS, "partners.benzinga.news"])
def test_all_is_refused_by_list_datasets(massive_session: dm.MassiveSession, name: str) -> None:
    make = factory(massive_session, name)
    with pytest.raises(ValueError, match=r"does not accept dm\.ALL; use symbols=None"):
        make(symbols=dm.ALL)


@pytest.mark.parametrize("name", LIST_WITH_SYMBOLS)
def test_stock_datasets_refuse_another_class(massive_session: dm.MassiveSession, name: str) -> None:
    with pytest.raises(ValueError, match="prefix of another asset class"):
        factory(massive_session, name)("X:BTCUSD")


@pytest.mark.parametrize("name", WITH_SYMBOLS)
def test_symbols_may_be_a_table(massive_session: dm.MassiveSession, name: str) -> None:
    table = new_table([string_col("Symbol", ["AAPL"])])
    symbols = factory(massive_session, name)(table)._request.symbols
    assert isinstance(symbols, SymbolTable)
    assert symbols.table is table


@pytest.mark.parametrize("name", WITH_SYMBOLS)
def test_an_exhausted_generator_is_refused(massive_session: dm.MassiveSession, name: str) -> None:
    symbols = (symbol for symbol in ["AAPL"])
    assert list(symbols) == ["AAPL"]
    with pytest.raises(ValueError, match="no symbols"):
        factory(massive_session, name)(symbols)


def test_consensus_ratings_fan_out_per_symbol(massive_session: dm.MassiveSession) -> None:
    q = massive_session.partners.benzinga.consensus_ratings(["msft", "AAPL"])
    assert type(q) is Query
    assert q._request.symbols == ("AAPL", "MSFT")
    assert q._request.params == ()
    spec = q._spec
    assert spec.per_symbol
    assert spec.symbol_param is None
    assert spec.poll_keys == ("Symbol",)
    with pytest.raises(ValueError, match="takes one ticker per request"):
        massive_session.partners.benzinga.consensus_ratings(dm.ALL)
    # One row per stock: no sort, order, or limit; filters choose the ratings aggregated.
    assert list(inspect.signature(partners._Benzinga.consensus_ratings).parameters) == [
        "self",
        "symbols",
        "filters",
    ]
    window: dict[str, FilterValue] = {"date_gte": "2024-10-01", "date_lt": date(2025, 1, 1)}
    q = massive_session.partners.benzinga.consensus_ratings("AAPL", filters=window)
    assert dict(q._request.filters) == window
    with pytest.raises(ValueError, match="has no filter 'ticker'"):
        massive_session.partners.benzinga.consensus_ratings("AAPL", filters={"ticker": "MSFT"})


def test_news_selects_articles_by_ticker_and_time(massive_session: dm.MassiveSession) -> None:
    end = datetime(2025, 6, 3, 14, 30, tzinfo=UTC)
    q = massive_session.partners.benzinga.news(
        symbols="X:BTCUSD", start="2025-06-02", end=end, filters={"channels_any_of": ["news"]}
    )
    request = q._request
    assert request.symbols == ("X:BTCUSD",)  # no class: each prefix picks its own
    assert request.start == date(2025, 6, 2)
    assert request.end == end
    assert request.filters == (("channels_any_of", ("news",)),)
    news = inspect.signature(partners._Benzinga.news).parameters
    assert news["symbols"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize(
    ("name", "axis_filter"),
    [
        ("partners.benzinga.news", "published_gte"),
        ("partners.benzinga.news", "published"),
        ("partners.tmx.corporate_events", "date_lt"),
        ("partners.tmx.corporate_events", "date_any_of"),
        ("partners.fable.merchant_aggregates", "transaction_date"),
    ],
)
def test_the_time_axis_is_set_only_by_start_and_end(
    massive_session: dm.MassiveSession, name: str, axis_filter: str
) -> None:
    with pytest.raises(ValueError, match="through start= and end="):
        factory(massive_session, name)(filters={axis_filter: "2025-01-01"})


@pytest.mark.parametrize(
    "name",
    [
        "partners.benzinga.news",
        "partners.tmx.corporate_events",
        "partners.fable.merchant_aggregates",
    ],
)
def test_a_range_must_not_end_before_it_starts(
    massive_session: dm.MassiveSession, name: str
) -> None:
    make = factory(massive_session, name)
    q = make(start="2025-07-01", end="2025-07-01")  # one whole day
    assert q._request.start == q._request.end == date(2025, 7, 1)
    with pytest.raises(ValueError, match="before"):
        make(start="2025-07-02", end="2025-07-01")
    with pytest.raises(ValueError, match="needs a timezone"):
        make(start="2025-07-01T09:30")


@pytest.mark.parametrize(
    ("name", "reserved"),
    [
        ("partners.benzinga.ratings", "ticker_any_of"),
        ("partners.benzinga.ratings", "ticker"),
        ("partners.etf_global.profiles", "composite_ticker_any_of"),
        ("partners.tmx.corporate_events", "ticker"),
        ("partners.benzinga.news", "tickers_any_of"),
    ],
)
def test_the_symbol_parameter_is_set_only_by_symbols(
    massive_session: dm.MassiveSession, name: str, reserved: str
) -> None:
    with pytest.raises(ValueError, match="has no filter"):
        factory(massive_session, name)(filters={reserved: "AAPL"})


@pytest.mark.parametrize(
    ("name", "filters"),
    [
        ("partners.benzinga.ratings", {"ticker_gte": "A", "rating_action": "upgrades"}),
        ("partners.benzinga.analysts", {"firm_name_any_of": ["Wedbush", "Mizuho"]}),
        ("partners.benzinga.firms", {"benzinga_id": "5f3ac1"}),
        ("partners.benzinga.earnings", {"date_gte": date(2025, 7, 1), "importance_gte": 3}),
        ("partners.etf_global.constituents", {"constituent_ticker": "NVDA"}),
        ("partners.tmx.corporate_events", {"type": "dividend", "tmx_company_id_gte": 2000}),
        ("partners.fable.merchant_aggregates", {"channel": "online", "user_country": "UK"}),
        ("partners.fable.merchant_hierarchy", {"ticker": "AMZN US", "listing_status": "public"}),
    ],
)
def test_filters_reach_the_endpoint_parameters(
    massive_session: dm.MassiveSession, name: str, filters: dict[str, object]
) -> None:
    request = factory(massive_session, name)(filters=filters)._request
    assert dict(request.filters) == {
        key: tuple(value) if isinstance(value, list) else value for key, value in filters.items()
    }


@pytest.mark.parametrize(
    "case",
    [
        ("partners.fable.merchant_aggregates", {"channel": "mail"}, r"takes \['online'"),
        ("partners.fable.merchant_hierarchy", {"listing_status": "listed"}, r"takes \['public'"),
        ("partners.benzinga.earnings", {"importance": "high"}, "takes a integer value"),
        ("partners.benzinga.analysts", {"ticker": "AAPL"}, "has no filter 'ticker'"),
    ],
)
def test_bad_filters_are_refused(
    massive_session: dm.MassiveSession, case: tuple[str, dict[str, object], str]
) -> None:
    name, filters, message = case
    with pytest.raises(ValueError, match=message):
        factory(massive_session, name)(filters=filters)


@pytest.mark.parametrize(
    "name", [n for n in DATASETS if n != "partners.benzinga.consensus_ratings"]
)
def test_order_is_an_enum(massive_session: dm.MassiveSession, name: str) -> None:
    with pytest.raises(TypeError, match=r"order must be dm\.Order"):
        factory(massive_session, name)(sort="date", order=cast("Order", "desc"))


def test_bad_types_are_refused(massive_session: dm.MassiveSession) -> None:
    ratings = massive_session.partners.benzinga.ratings
    with pytest.raises(TypeError, match="sort must be a str"):
        ratings(sort=cast("str", 1))
    with pytest.raises(ValueError, match="limit must be positive"):
        ratings(limit=0)
    with pytest.raises(TypeError, match="filters must be a mapping"):
        ratings(filters=cast("dict[str, str]", [("date", "2025-01-01")]))


# The docstrings: a summary, then a link to the column reference of the dataset.


@pytest.mark.parametrize("name", DATASETS)
def test_each_factory_links_its_column_reference(name: str) -> None:
    method = partners.__dict__[_class_of(name)].__dict__[name.rsplit(".", 1)[1]]
    doc = inspect.getdoc(method)
    assert doc is not None
    anchor = name.removeprefix("partners.").replace(".", "-")
    assert f"docs/datasets/partners.md#{anchor}" in doc
    summary = doc.split("\n\n", 1)[0]
    assert summary == spec_of(name).summary
    assert "\n" not in summary
    assert summary.endswith(".")
    assert "REST only" in doc
