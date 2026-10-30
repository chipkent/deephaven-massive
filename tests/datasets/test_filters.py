"""Tests for the `filters=` rules: the keys a dataset takes, and the values it sends.

They run on synthetic specs over the real generated sources.
"""

import dataclasses
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

from deephaven_massive._datasets import filters
from deephaven_massive._datasets.catalog import Catalog, DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._datasets.filters import check_filters, param_base
from deephaven_massive._enums import AssetClass, ContractType
from deephaven_massive._massive import (
    Field,
    ParamKind,
    RestPath,
    Source,
    Transport,
    WireType,
    WireUnit,
    sources,
)

R, W, F = Transport.REST, Transport.WS, Transport.FLAT_FILES


def keys(
    symbol: str | KeySpec | None = None,
    time: str | KeySpec | None = None,
    *,
    fanout: str | KeySpec | None = None,
    date: str | KeySpec | None = None,
    end: str | KeySpec | None = None,
) -> SourceKeys:
    # A plain string names the source's own column; a KeySpec says more.
    def key(value: str | KeySpec | None) -> KeySpec | None:
        return KeySpec(value) if isinstance(value, str) else value

    return SourceKeys(key(symbol), key(time), end=key(end), fanout=key(fanout), date=key(date))


EXCHANGES = DatasetSpec(
    name="reference.exchanges",
    summary="A dataset for the tests.",
    fetch_path="/v3/reference/exchanges",
    poll_keys=("Id",),
)

CONTRACTS = DatasetSpec(
    name="options.contracts",
    summary="A dataset for the tests.",
    asset_class=AssetClass.OPTIONS,
    fetch_path="/v3/reference/options/contracts",
    extra_paths=("/v3/reference/options/contracts/{options_ticker}",),
    keys={
        (R, "/v3/reference/options/contracts"): keys("Ticker", fanout="UnderlyingTicker"),
        # The per-contract rows carry their ticker, although the request names it too.
        (R, "/v3/reference/options/contracts/{options_ticker}"): keys(
            "Ticker", fanout="UnderlyingTicker"
        ),
    },
    symbol_param="ticker",
)

DIVIDENDS = DatasetSpec(
    name="stocks.dividends",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    fetch_path="/stocks/v1/dividends",
    keys={(R, "/stocks/v1/dividends"): keys("Ticker")},
    time_axis=("ex_dividend_date.gte", "ex_dividend_date.lte"),
    symbol_param="ticker.any_of",
)


def change(spec: DatasetSpec, **changes: object) -> DatasetSpec:
    return dataclasses.replace(spec, **cast("dict[str, Any]", changes))


def check(spec: DatasetSpec, filters: Mapping[str, object]) -> dict[str, object]:
    # The values pass through as given: the check itself must refuse what does not fit.
    return dict(check_filters(Catalog((spec,)), spec, cast("Mapping[str, Any]", filters)))


def test_filters_are_the_query_parameters_of_the_dataset_paths() -> None:
    check(
        DIVIDENDS,
        {
            "frequency_gte": 4,
            "distribution_type": "recurring",
            "distribution_type_any_of": ("recurring", "special"),
        },
    )
    assert check(EXCHANGES, {}) == {}  # nothing to check, so no path is read
    with pytest.raises(ValueError, match=r"has no filter 'pay_date_lte'; valid filters: \["):
        check(DIVIDENDS, {"pay_date_lte": "2025-01-01"})


def test_a_path_parameter_is_no_filter() -> None:
    indicator = DatasetSpec(
        name="indicators.sma",
        summary="A dataset for the tests.",
        fetch_path="/v1/indicators/sma/{stockTicker}",
    )
    check(indicator, {"window": 50})
    with pytest.raises(ValueError, match=r"has no filter 'stockTicker'"):
        check(indicator, {"stockTicker": "AAPL"})


@pytest.mark.parametrize(
    "key",
    [
        "limit",
        "sort",
        # The symbol parameter's family: its base and every suffixed form.
        "ticker",
        "ticker_any_of",
        # The time axis's family, not only the start/end pair.
        "ex_dividend_date",
        "ex_dividend_date_gt",
        "ex_dividend_date_gte",
        "ex_dividend_date_lt",
        "ex_dividend_date_lte",
    ],
)
def test_the_library_sets_some_parameters_itself(key: str) -> None:
    with pytest.raises(ValueError, match=rf"has no filter '{key}'"):
        check(DIVIDENDS, {key: "x"})


def test_the_symbol_parameter_reserves_only_its_equality_forms() -> None:
    # An alphabetical range of tickers is a filter `symbols` cannot express.
    check(DIVIDENDS, {"ticker_gte": "A", "ticker_lt": "B"})
    sheets = DatasetSpec(
        name="stocks.balance_sheets",
        summary="A dataset for the tests.",
        asset_class=AssetClass.STOCKS,
        fetch_path="/stocks/financials/v1/balance-sheets",
        symbol_param="tickers.any_of",
    )
    for key in ("tickers", "tickers_any_of", "tickers_all_of"):
        with pytest.raises(ValueError, match=rf"has no filter '{key}'"):
            check(sheets, {key: "AAPL"})
    assert filters.symbol_family("ticker.any_of") == {
        "ticker",
        "ticker.any_of",
        "ticker.all_of",
    }
    assert "date.all_of" in filters.param_family("date.gte")


def test_filter_values_must_fit_their_kind() -> None:
    with pytest.raises(
        ValueError, match=r"^stocks\.dividends filter 'frequency' takes integer values, not '4'$"
    ):
        check(DIVIDENDS, {"frequency": "4"})
    with pytest.raises(ValueError, match=r"takes one of \['recurring', 'special', .*\], not 'x'"):
        check(DIVIDENDS, {"distribution_type": "x"})
    # Without a time axis, the date parameter is an ordinary filter.
    unaxed = change(DIVIDENDS, time_axis=None)
    check(unaxed, {"ex_dividend_date": date(2025, 1, 2)})
    naive = datetime(2025, 1, 2, 9, 30, tzinfo=UTC).replace(tzinfo=None)  # naive on purpose
    with pytest.raises(ValueError, match=r"^ex_dividend_date=datetime\.datetime\(2025, 1, 2"):
        check(unaxed, {"ex_dividend_date": naive})
    # Text for a date parameter is ISO-8601, and a time in it carries an offset.
    with pytest.raises(ValueError, match=r"^ex_dividend_date='soon' is not an ISO-8601 date"):
        check(unaxed, {"ex_dividend_date": "soon"})
    with pytest.raises(ValueError, match=r"^ex_dividend_date='2025-01-02T09:30' needs a timezone"):
        check(unaxed, {"ex_dividend_date": "2025-01-02T09:30"})
    check(unaxed, {"ex_dividend_date": "2025-01-02T09:30:00Z"})


INDICATOR = DatasetSpec(
    name="indicators.sma",
    summary="A dataset for the tests.",
    fetch_path="/v1/indicators/sma/{stockTicker}",
)

OPTION_CONTRACTS = change(CONTRACTS, symbol_param=None)

NEWS = DatasetSpec(
    name="reference.news",
    summary="A dataset for the tests.",
    fetch_path="/v2/reference/news",
)


@pytest.mark.parametrize(
    ("spec", "key", "given", "kept"),
    [
        (OPTION_CONTRACTS, "underlying_ticker", "NVDA", "NVDA"),
        (DIVIDENDS, "frequency_gte", 4, 4),
        (OPTION_CONTRACTS, "strike_price_gte", 4, 4),
        (OPTION_CONTRACTS, "strike_price_gte", 4.5, 4.5),
        (OPTION_CONTRACTS, "expired", True, True),
        (OPTION_CONTRACTS, "contract_type", "call", "call"),
        (OPTION_CONTRACTS, "contract_type", ContractType.PUT, ContractType.PUT),
        # Text of a date parameter is stored as what it means, never as the text.
        (OPTION_CONTRACTS, "expiration_date_lte", "2026-01-16", date(2026, 1, 16)),
        (OPTION_CONTRACTS, "as_of", date(2025, 9, 18), date(2025, 9, 18)),
        (
            OPTION_CONTRACTS,
            "as_of",
            "2025-09-18T09:30:00-04:00",
            datetime(2025, 9, 18, 13, 30, tzinfo=UTC),
        ),
        # A date on a range filter of an instant parameter stays a date: the REST transport
        # places it in the day zone of each request's symbols. A millisecond timestamp can
        # end a day exactly, so every form takes one.
        (INDICATOR, "timestamp_gte", "2025-01-02", date(2025, 1, 2)),
        (INDICATOR, "timestamp_lte", date(2025, 1, 2), date(2025, 1, 2)),
        (INDICATOR, "timestamp", date(2025, 1, 2), date(2025, 1, 2)),  # no range bound
        (NEWS, "published_utc_gte", "2025-06-02", date(2025, 6, 2)),
        (NEWS, "published_utc_lt", "2025-06-02", date(2025, 6, 2)),
        (NEWS, "published_utc", "2025-06-02", date(2025, 6, 2)),
        # An instant given by the caller is exact already.
        (
            NEWS,
            "published_utc_lte",
            "2025-06-02T12:00:00Z",
            datetime(2025, 6, 2, 12, tzinfo=UTC),
        ),
        # Text finer than a microsecond rounds outward: an upper bound up, any other down.
        (
            NEWS,
            "published_utc_lt",
            "2025-01-02T14:30:00.000000500Z",
            datetime(2025, 1, 2, 14, 30, 0, 1, tzinfo=UTC),
        ),
        (
            NEWS,
            "published_utc_gte",
            "2025-01-02T14:30:00.000000500Z",
            datetime(2025, 1, 2, 14, 30, tzinfo=UTC),
        ),
        # A calendar-date parameter keeps the day.
        (OPTION_CONTRACTS, "expiration_date_gt", "2026-01-16", date(2026, 1, 16)),
        (
            INDICATOR,
            "timestamp_gte",
            "2025-01-02T14:30:00Z",
            datetime(2025, 1, 2, 14, 30, tzinfo=UTC),
        ),
        # A bare text for a list parameter is the list of that one value.
        (DIVIDENDS, "distribution_type_any_of", "special", ("special",)),
        (DIVIDENDS, "distribution_type_any_of", ("a", "b"), ("a", "b")),
        # The wire joins a list with commas, so every spelling of one list is one value.
        (DIVIDENDS, "distribution_type_any_of", "a, b", ("a", "b")),
        (DIVIDENDS, "distribution_type_any_of", ("a,b", "c"), ("a", "b", "c")),
    ],
)
def test_each_parameter_kind_keeps_the_value_it_sends(
    *, spec: DatasetSpec, key: str, given: object, kept: object
) -> None:
    (value,) = check(spec, {key: given}).values()
    assert value == kept
    assert type(value) is type(kept)


@pytest.mark.parametrize(
    ("spec", "key", "value", "message"),
    [
        (OPTION_CONTRACTS, "underlying_ticker", 1, "takes text, not 1"),
        (DIVIDENDS, "frequency_gte", True, "takes integer values, not True"),
        (DIVIDENDS, "frequency_gte", 4.5, "takes integer values, not 4.5"),
        (OPTION_CONTRACTS, "strike_price_gte", False, "takes number values, not False"),
        (OPTION_CONTRACTS, "strike_price_gte", float("nan"), "takes a finite number, not nan"),
        (OPTION_CONTRACTS, "expired", 1, "takes a bool, not 1"),
        (OPTION_CONTRACTS, "contract_type", "up", r"takes one of \['call', 'put'\], not 'up'"),
        (OPTION_CONTRACTS, "as_of", 1, "takes a date or an aware datetime, not 1"),
        (INDICATOR, "timestamp_gte", 1.5, "takes a date or an aware datetime, not 1.5"),
        # Milliseconds cannot carry a microsecond instant; rounding would move the bound.
        (
            INDICATOR,
            "timestamp_gte",
            datetime(2025, 1, 2, 0, 0, 0, 1, tzinfo=UTC),
            "takes whole ms since the epoch",
        ),
        (DIVIDENDS, "distribution_type_any_of", (), "takes at least one value"),
        (DIVIDENDS, "distribution_type_any_of", "a,,b", "has an empty value in"),
        (DIVIDENDS, "distribution_type_any_of", " ", "has an empty value in"),
        # ISO-8601 text of no documented precision cannot end a day exactly.
        (
            NEWS,
            "published_utc_lte",
            date(2025, 6, 2),
            (
                r"published_utc\.lte has no exact unit to end a day with; pass an aware "
                "datetime, or a date to published_utc_gte or published_utc_lt"
            ),
        ),
        (NEWS, "published_utc_gt", "2025-06-02", "no exact unit to end a day with"),
        (DIVIDENDS, "distribution_type_any_of", ("a", 1), "takes no numbers, not 1"),
        (DIVIDENDS, "distribution_type_any_of", 5, "takes a sequence of text"),
    ],
)
def test_a_value_the_wire_cannot_send_is_refused_at_once(
    *, spec: DatasetSpec, key: str, value: object, message: str
) -> None:
    with pytest.raises(ValueError, match=rf"^{spec.name} filter '{key}'.*{message}"):
        check(spec, {key: value})


RATINGS = DatasetSpec(
    name="partners.ratings",
    summary="A dataset for the tests.",
    fetch_path="/benzinga/v1/ratings",
)


def test_a_list_of_dates_writes_its_date_items_as_days() -> None:
    assert check(RATINGS, {"date_any_of": (date(2025, 6, 2), "2025-06-03,2025-06-04")}) == {
        "date_any_of": ("2025-06-02", "2025-06-03", "2025-06-04")
    }
    with pytest.raises(ValueError, match=r"filter 'ticker_any_of' takes no dates, not"):
        check(RATINGS, {"ticker_any_of": (date(2025, 6, 2),)})


SHORT_INTEREST = DatasetSpec(
    name="stocks.short_interest",
    summary="A dataset for the tests.",
    fetch_path="/stocks/v1/short-interest",
)


def test_a_list_of_numbers_writes_each_number_as_its_parameter_does() -> None:
    # `days_to_cover` is a number, `avg_daily_volume` an integer.
    assert check(SHORT_INTEREST, {"days_to_cover_any_of": (2, 2.5, "3")}) == {
        "days_to_cover_any_of": ("2", "2.5", "3")
    }
    assert check(SHORT_INTEREST, {"avg_daily_volume_any_of": (100, 200)}) == {
        "avg_daily_volume_any_of": ("100", "200")
    }
    with pytest.raises(ValueError, match=r"'avg_daily_volume_any_of' takes integer values, not"):
        check(SHORT_INTEREST, {"avg_daily_volume_any_of": (4.5,)})
    with pytest.raises(ValueError, match=r"filter 'ticker_any_of' takes no numbers, not 5"):
        check(RATINGS, {"ticker_any_of": (5,)})
    with pytest.raises(ValueError, match=r"filter 'days_to_cover_any_of' takes no dates, not"):
        check(SHORT_INTEREST, {"days_to_cover_any_of": (date(2025, 6, 2),)})
    # A bool is never a number: the encoder refuses it as an item of a list.
    with pytest.raises(ValueError, match=r"'days_to_cover_any_of'.*takes a sequence of text"):
        check(SHORT_INTEREST, {"days_to_cover_any_of": (True,)})


def _two_paths(as_text: tuple[str, ...]) -> tuple[Catalog, DatasetSpec]:
    # The dividends path and a copy that takes text for the parameters named `as_text*`.
    real = sources(R)["/stocks/v1/dividends"]
    assert isinstance(real, RestPath)
    params = tuple(
        dataclasses.replace(p, kind=ParamKind.STRING) if p.name.startswith(as_text) else p
        for p in real.params
    )
    copy = dataclasses.replace(real, key="/stocks/v1/dividends-copy", params=params)
    table: dict[str, Source] = {real.key: real, copy.key: copy}
    spec = DatasetSpec(
        name="stocks.two_paths",
        summary="A dataset for the tests.",
        fetch_path=real.key,
        extra_paths=(copy.key,),
    )
    return Catalog((spec,), source_tables=lambda t: table if t is R else {}), spec


def test_every_path_that_declares_a_filter_checks_it() -> None:
    catalog, spec = _two_paths(("frequency", "ex_dividend_date"))
    # The copy takes text where the real path takes an integer: no value suits both.
    with pytest.raises(ValueError, match=r"takes integer values, not '4' \(on /stocks/v1/div"):
        check_filters(catalog, spec, {"frequency": "4"})
    with pytest.raises(ValueError, match=r"takes text, not 4 \(on /stocks/v1/dividends-copy\)"):
        check_filters(catalog, spec, {"frequency": 4})
    # A date parameter on one path and a text one on the other read the text differently.
    with pytest.raises(ValueError, match=r"the dataset's paths read '2025-01-02' as different"):
        check_filters(catalog, spec, {"ex_dividend_date": "2025-01-02"})
    # Paths that agree pass.
    assert check_filters(catalog, spec, {"distribution_type": "special"}) == (
        ("distribution_type", "special"),
    )


def test_an_epoch_field_behind_a_date_parameter_compares_instants() -> None:
    news = sources(R)["/v2/reference/news"]
    assert isinstance(news, RestPath)
    assert filters.takes_instants(news, "published_utc.gte", ParamKind.DATE)
    epoch = Field(
        wire_key="published_utc", column="PublishedUtc", wire_type=WireType.EPOCH, unit=WireUnit.MS
    )
    fields = tuple(epoch if f.wire_key == "published_utc" else f for f in news.fields)
    assert filters.takes_instants(
        dataclasses.replace(news, fields=fields), "published_utc.gte", ParamKind.DATE
    )
    text = Field(wire_key="published_utc", column="PublishedUtc", wire_type=WireType.STRING)
    fields = tuple(text if f.wire_key == "published_utc" else f for f in news.fields)
    assert not filters.takes_instants(
        dataclasses.replace(news, fields=fields), "published_utc.gte", ParamKind.DATE
    )
    assert not filters.takes_instants(news, "ticker", ParamKind.STRING)


def test_the_valid_filters_named_are_only_those_the_caller_can_pass() -> None:
    catalog = Catalog((DIVIDENDS,))
    with pytest.raises(ValueError, match=r"has no filter 'pay_date'") as caught:
        check_filters(catalog, DIVIDENDS, {"pay_date": "2025-01-01"}, typed=("frequency",))
    named = str(caught.value).partition("valid filters: ")[2]
    # The typed parameter's family, the time axis, the symbol's equality forms, and the
    # parameters the library sets are reserved; the symbol's range forms stay filters.
    for reserved in ("'frequency", "'ex_dividend_date", "'ticker'", "'ticker_any_of'", "'limit'"):
        assert reserved not in named
    assert "'ticker_gte'" in named
    assert "'distribution_type'" in named
    with pytest.raises(ValueError, match=r"has no filter 'frequency_gte'"):
        check_filters(catalog, DIVIDENDS, {"frequency_gte": 4}, typed=("frequency",))


def test_a_parameter_s_base_is_its_text_before_the_first_dot() -> None:
    assert param_base("timestamp.gte") == "timestamp"
    assert param_base("ticker.any_of") == "ticker"
    assert param_base("date") == "date"
