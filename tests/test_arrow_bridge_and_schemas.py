"""Unit tests for arrow_bridge type coercions, RecordBatch conversion, and all 58 schema normalizers."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pyarrow as pa
import pytest

from deephaven_massive.engine.arrow_bridge import (
    STRING_LIST_TYPE,
    SchemaSpec,
    arrow_to_deephaven_table,
    flatten_dict,
    parse_int32_list,
    parse_str_list,
    record_batches_to_arrow_table,
    rows_to_record_batch,
    to_bool,
    to_epoch_days,
    to_float,
    to_int,
    to_pascal_case,
    to_str,
    to_utc_ns,
)
from deephaven_massive.engine.symbols import SymbolNormalizer
from deephaven_massive.schemas import (
    BARS_SCHEMA,
    BENZINGA_ANALYSTS_SCHEMA,
    BENZINGA_BULLS_BEARS_SCHEMA,
    BENZINGA_CONSENSUS_SCHEMA,
    BENZINGA_EARNINGS_SCHEMA,
    BENZINGA_FIRMS_SCHEMA,
    BENZINGA_GUIDANCE_SCHEMA,
    BENZINGA_NEWS_SCHEMA,
    BENZINGA_RATINGS_SCHEMA,
    BENZINGA_TRANSCRIPTS_SCHEMA,
    CONDITIONS_SCHEMA,
    CURRENCY_CONVERSION_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    DIVIDENDS_SCHEMA,
    ETF_GLOBAL_ANALYTICS_SCHEMA,
    ETF_GLOBAL_CONSTITUENTS_SCHEMA,
    ETF_GLOBAL_FUND_FLOWS_SCHEMA,
    ETF_GLOBAL_PROFILES_SCHEMA,
    ETF_GLOBAL_TAXONOMIES_SCHEMA,
    EXCHANGES_SCHEMA,
    FABLE_MERCHANT_AGGREGATES_SCHEMA,
    FABLE_MERCHANT_HIERARCHY_SCHEMA,
    FINANCIAL_STATEMENT_SCHEMA,
    FMV_SCHEMA,
    FUNDING_CONDITIONS_SCHEMA,
    FUTURES_CONTRACTS_SCHEMA,
    FUTURES_PRODUCTS_SCHEMA,
    FUTURES_SCHEDULES_SCHEMA,
    FUTURES_SNAPSHOT_SCHEMA,
    HOLIDAYS_SCHEMA,
    IMBALANCES_SCHEMA,
    INDEX_VALUES_SCHEMA,
    INDICATOR_SCHEMA,
    INFLATION_EXPECTATIONS_SCHEMA,
    INFLATION_SCHEMA,
    IPOS_SCHEMA,
    LABOR_MARKET_SCHEMA,
    LULD_SCHEMA,
    MARKET_STATUS_SCHEMA,
    NEWS_SCHEMA,
    OPTION_CHAIN_SCHEMA,
    OPTION_CONTRACTS_SCHEMA,
    QUOTES_SCHEMA,
    RATIOS_SCHEMA,
    RELATED_COMPANIES_SCHEMA,
    SEC_13F_SCHEMA,
    SEC_FILINGS_SCHEMA,
    SEC_INSIDER_SCHEMA,
    SHARE_FLOAT_SCHEMA,
    SHORT_INTEREST_SCHEMA,
    SHORT_VOLUME_SCHEMA,
    SPLITS_SCHEMA,
    STOCK_SNAPSHOT_SCHEMA,
    TICKER_DETAILS_SCHEMA,
    TICKER_EVENTS_SCHEMA,
    TICKER_TYPES_SCHEMA,
    TICKERS_SCHEMA,
    TMX_CORPORATE_EVENTS_SCHEMA,
    TRADES_SCHEMA,
    TREASURY_YIELDS_SCHEMA,
)
from tests.conftest import dh_to_pydict


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("net_income_loss", "NetIncomeLoss"),
        ("camelCaseName", "CamelCaseName"),
        ("10_year_yield", "F10YearYield"),
        ("___", "Field"),
    ],
)
def test_to_pascal_case_conversions(raw: str, expected: str) -> None:
    """Verify snake_case, camelCase, leading digits, and empty tokens convert to PascalCase."""
    assert to_pascal_case(raw) == expected


def test_to_utc_ns_epoch_units_and_temporal_inputs() -> None:
    """Verify to_utc_ns auto-detects seconds/ms/us/ns epochs and parses ISO-8601 strings and dates."""
    assert to_utc_ns(None) is None
    assert to_utc_ns("") is None
    assert to_utc_ns("   ") is None
    assert to_utc_ns(True) is None
    assert to_utc_ns(0) is None
    assert to_utc_ns(-5) is None
    assert to_utc_ns(0.0) is None

    expected_ns = 1_700_000_000_000_000_000
    assert to_utc_ns(1_700_000_000) == expected_ns
    assert to_utc_ns(1_700_000_000_000) == expected_ns
    assert to_utc_ns(1_700_000_000_000_000) == expected_ns
    assert to_utc_ns(1_700_000_000_000_000_000) == expected_ns
    assert to_utc_ns(1_700_000_000.0) == expected_ns
    assert to_utc_ns(1_700_000_000_000.0) == expected_ns
    assert to_utc_ns(1_700_000_000_000_000.0) == expected_ns
    assert to_utc_ns(1.7e18) == int(1.7e18)
    assert to_utc_ns("1700000000000") == expected_ns

    assert to_utc_ns(datetime(2025, 1, 2, tzinfo=timezone.utc)) == 1_735_776_000_000_000_000
    assert to_utc_ns(datetime(2025, 1, 2)) == 1_735_776_000_000_000_000
    assert to_utc_ns(date(2025, 1, 2)) == 1_735_776_000_000_000_000
    assert to_utc_ns("2025-01-02T10:00:00Z") == 1_735_812_000_000_000_000
    assert to_utc_ns("2025-01-02T10:00:00") == 1_735_812_000_000_000_000
    assert to_utc_ns("not-a-timestamp") is None
    assert to_utc_ns(object()) is None


def test_to_epoch_days_conversions() -> None:
    """Verify to_epoch_days converts dates, datetimes, ISO strings, and epoch ints to days since 1970-01-01."""
    assert to_epoch_days(None) is None
    assert to_epoch_days("") is None
    assert to_epoch_days("   ") is None
    assert to_epoch_days(True) is None
    assert to_epoch_days(-100) is None
    assert to_epoch_days("invalid-date") is None

    assert to_epoch_days(datetime(1970, 1, 2, tzinfo=timezone.utc)) == 1
    assert to_epoch_days(date(1970, 1, 3)) == 2
    assert to_epoch_days("1970-01-04") == 3
    assert to_epoch_days("1700000000000") == 19675
    assert to_epoch_days(1_700_000_000_000) == 19675


def test_primitive_scalar_coercions() -> None:
    """Verify to_float, to_int, to_bool, and to_str safely coerce valid inputs and return None on invalid inputs."""
    assert to_float(None) is None
    assert to_float(True) is None
    assert to_float("12.5") == 12.5
    assert to_float("bad") is None

    assert to_int(None) is None
    assert to_int(False) is None
    assert to_int("42.0") == 42
    assert to_int("42") == 42
    assert to_int("bad") is None

    assert to_bool(None) is None
    assert to_bool(True) is True
    assert to_bool("true") is True
    assert to_bool("no") is False
    assert to_bool("maybe") is None
    assert to_bool(1) is True

    assert to_str(None) is None
    assert to_str("  ") is None
    assert to_str(" abc ") == "abc"


def test_list_parsers_and_flatten_dict() -> None:
    """Verify parse_int32_list, parse_str_list, and flatten_dict normalize nested JSON structures."""
    assert parse_int32_list(None) is None
    assert parse_int32_list([12, "37", "bad"]) == [12, 37]
    assert parse_int32_list("[12, 37]") == [12, 37]
    assert parse_int32_list("[]") == []
    assert parse_int32_list("12, bad, 37") == [12, 37]
    assert parse_int32_list(14) == [14]
    assert parse_int32_list(object()) is None

    assert parse_str_list(None) is None
    assert parse_str_list(["a", " b "]) == ["a", "b"]
    assert parse_str_list("a, b") == ["a", "b"]
    assert parse_str_list("   ") == []
    assert parse_str_list(123) == ["123"]

    flat = flatten_dict(
        {
            "revenues": {"value": 100.0, "unit": "USD"},
            "nested": {"inner_key": 5},
            "tags": ["a", "b"],
            "complex_list": [{"k": 1}],
            "scalar": "ok",
        }
    )
    assert flat["Revenues"] == 100.0
    assert flat["NestedInnerKey"] == 5
    assert flat["Tags"] == ["a", "b"]
    assert isinstance(flat["ComplexList"], str)
    assert flat["Scalar"] == "ok"


def test_dynamic_columns_and_arrow_to_deephaven_conversion(
    dh_exec_ctx: object,
) -> None:
    """Verify dynamic column schema promotion across RecordBatches and zero-copy conversion to Deephaven."""
    dyn_spec = SchemaSpec(
        name="dyn_test",
        arrow_schema=pa.schema(
            [
                pa.field("Symbol", pa.string()),
                pa.field("Flag", pa.bool_()),
                pa.field("Tags", STRING_LIST_TYPE),
            ]
        ),
        key_cols=("Symbol",),
        normalizer=lambda r, _: dict(r),
        allow_dynamic_columns=True,
    )
    b1 = rows_to_record_batch(
        [
            {
                "Symbol": "AAPL",
                "Flag": True,
                "Tags": ["tech"],
                "DynFloat": 10.5,
                "DynBool": True,
                "DynNull": None,
                "DynStr": "hello",
            }
        ],
        dyn_spec,
        extra_columns={"__Extra": (pa.int64(), 1)},
    )
    b2 = rows_to_record_batch(
        [
            {
                "Symbol": "MSFT",
                "Flag": False,
                "Tags": ["cloud"],
                "DynFloat": 20.0,
                "ExtraMetric": 99.0,
            }
        ],
        dyn_spec,
        extra_columns={"__Extra": (pa.int64(), [2])},
    )
    tbl = record_batches_to_arrow_table([b1, b2], dyn_spec)
    assert {"ExtraMetric", "DynBool", "DynNull", "DynStr"} <= set(tbl.column_names)

    empty_tbl = record_batches_to_arrow_table(
        [], dyn_spec, extra_fields=[pa.field("__PollId", pa.int64())]
    )
    assert "__PollId" in empty_tbl.column_names

    dh_with_ctx = arrow_to_deephaven_table(tbl, dh_exec_ctx)  # type: ignore[arg-type]
    dh_without_ctx = arrow_to_deephaven_table(tbl, None)
    assert dh_to_pydict(dh_with_ctx)["Symbol"] == ["AAPL", "MSFT"]
    assert dh_to_pydict(dh_without_ctx)["Symbol"] == ["AAPL", "MSFT"]


def test_market_schemas_normalization_and_fallbacks() -> None:
    """Verify all 13 market schema normalizers handle REST, WebSocket, and nested snapshot payloads."""
    stocks_norm = SymbolNormalizer("stocks")
    forex_norm = SymbolNormalizer("forex")
    options_norm = SymbolNormalizer("options")

    assert (
        TRADES_SCHEMA.normalizer(
            {"sym": "AAPL", "p": 150.0, "s": 10, "t": 1700000000000}, stocks_norm
        )["Symbol"]
        == "AAPL"
    )
    assert (
        TRADES_SCHEMA.normalizer(
            {"ticker": "AAPL", "last_trade": {"p": 151.0}}, None
        )["Price"]
        == 151.0
    )
    assert TRADES_SCHEMA.normalizer({"ticker": "   "}, stocks_norm)["Symbol"] is None

    assert (
        QUOTES_SCHEMA.normalizer({"p": "EUR/USD", "a": 1.1, "b": 1.09}, forex_norm)[
            "Symbol"
        ]
        == "EUR/USD"
    )
    assert (
        QUOTES_SCHEMA.normalizer(
            {"last_quote": {"Symbol": "AAPL", "P": 151.0, "p": 150.0}}, stocks_norm
        )["Symbol"]
        == "AAPL"
    )

    assert (
        BARS_SCHEMA.normalizer(
            {
                "T": "AAPL",
                "o": 100,
                "h": 110,
                "l": 99,
                "c": 105,
                "v": 1000,
                "t": 1700000000000,
            },
            stocks_norm,
        )["Close"]
        == 105
    )
    assert (
        BARS_SCHEMA.normalizer(
            {
                "T": "AAPL",
                "o": 100,
                "c": 105,
                "s": 1700000000000,
                "e": 1700000060000,
            },
            stocks_norm,
        )["EndTimestamp"]
        == 1700000060000000000
    )
    assert (
        BARS_SCHEMA.normalizer(
            {
                "T": "AAPL",
                "o": 100,
                "c": 105,
                "t": 1700000000000,
                "_span_ns": 60_000_000_000,
            },
            stocks_norm,
        )["EndTimestamp"]
        == 1700000060000000000
    )
    assert (
        BARS_SCHEMA.normalizer(
            {
                "T": "AAPL",
                "o": 100,
                "c": 105,
                "t": 1700000000000,
                "timespan": "minute",
                "multiplier": 5,
            },
            stocks_norm,
        )["EndTimestamp"]
        == 1700000300000000000
    )
    assert (
        BARS_SCHEMA.normalizer({"T": "AAPL", "o": 100, "c": 105}, stocks_norm)[
            "EndTimestamp"
        ]
        is None
    )
    assert (
        BARS_SCHEMA.normalizer(
            {
                "ticker": "AAPL",
                "day": {"o": 100, "c": 105},
                "min": {"t": 1700000000000, "av": 5000},
            },
            stocks_norm,
        )["AccumulatedVolume"]
        == 5000
    )

    assert (
        LULD_SCHEMA.normalizer(
            {
                "T": "AAPL",
                "h": 160.0,
                "l": 140.0,
                "i": [1],
                "z": 3,
                "t": 1700000000000,
                "q": 10,
            },
            stocks_norm,
        )["HighPrice"]
        == 160.0
    )
    assert (
        IMBALANCES_SCHEMA.normalizer(
            {"T": "AAPL", "p": 150, "i": 500, "b": 100.0}, stocks_norm
        )["ImbalanceShares"]
        == 500
    )
    assert (
        FMV_SCHEMA.normalizer(
            {"sym": "AAPL", "fmv": 150.25, "t": 1700000000000}, stocks_norm
        )["FairMarketValue"]
        == 150.25
    )

    assert (
        INDEX_VALUES_SCHEMA.normalizer({"T": "I:SPX", "val": 5000.0}, None)["Value"]
        == 5000.0
    )
    assert (
        STOCK_SNAPSHOT_SCHEMA.normalizer(
            {
                "ticker": "AAPL",
                "day": {
                    "o": 149.0,
                    "h": 152.0,
                    "l": 148.0,
                    "c": 151.0,
                    "v": 1000,
                    "vw": 150.5,
                },
                "prevDay": {"o": 148.0, "c": 149.0},
                "min": {"o": 150.5, "c": 151.0, "t": 1700000000000},
                "lastTrade": {"p": 151.0, "s": 100, "t": 1700000000000},
                "lastQuote": {
                    "P": 151.1,
                    "S": 1,
                    "p": 150.9,
                    "s": 2,
                    "t": 1700000000000,
                },
            },
            stocks_norm,
        )["LastTradePrice"]
        == 151.0
    )

    assert (
        OPTION_CHAIN_SCHEMA.normalizer(
            {
                "details": {
                    "ticker": "O:AAPL251219C00150000",
                    "contract_type": "call",
                    "strike_price": 150.0,
                },
                "underlying_asset": {"ticker": "AAPL", "price": 151.0},
                "greeks": {
                    "delta": 0.55,
                    "gamma": 0.04,
                    "theta": -0.05,
                    "vega": 0.12,
                },
                "day": {"open": 5.0, "close": 5.5},
                "last_trade": {"price": 5.5, "size": 10},
                "last_quote": {"bid": 5.4, "ask": 5.6},
                "implied_volatility": 0.25,
                "open_interest": 1200,
            },
            options_norm,
        )["Delta"]
        == 0.55
    )

    assert (
        FUTURES_SNAPSHOT_SCHEMA.normalizer(
            {
                "ticker": "ESZ5",
                "details": {"product_code": "ES", "open_interest": 50000},
                "session": {
                    "open": 5000.0,
                    "close": 5010.0,
                    "settlement_price": 5008.0,
                },
                "last_trade": {"price": 5010.0},
                "last_quote": {"bid_price": 5009.75, "ask_price": 5010.25},
                "minute_bar": {"close": 5010.0},
            },
            None,
        )["SettlementPrice"]
        == 5008.0
    )

    assert (
        DAILY_SUMMARY_SCHEMA.normalizer(
            {"symbol": "AAPL", "from": "2025-01-02", "open": 150.0, "close": 152.0},
            stocks_norm,
        )["Close"]
        == 152.0
    )
    assert (
        CURRENCY_CONVERSION_SCHEMA.normalizer(
            {
                "from": "USD",
                "to": "EUR",
                "initialAmount": 100.0,
                "converted": 92.5,
                "last": {"ask": 0.926, "bid": 0.924},
            },
            None,
        )["ConvertedAmount"]
        == 92.5
    )
    assert (
        INDICATOR_SCHEMA.normalizer(
            {"Symbol": "AAPL", "timestamp": 1700000000000, "value": 148.5}, stocks_norm
        )["Value"]
        == 148.5
    )


def test_economy_schemas_normalization() -> None:
    """Verify all 5 economy schema normalizers coerce dates and macroeconomic numeric fields."""
    funding = FUNDING_CONDITIONS_SCHEMA.normalizer(
        {"date": "1970-01-02", "sofr": 5.31, "effr": 5.33}, None
    )
    assert funding["Date"] == "1970-01-02"
    assert funding["Sofr"] == 5.31
    assert funding["Effr"] == 5.33

    yields = TREASURY_YIELDS_SCHEMA.normalizer(
        {"date": "1970-01-02", "yield_2_year": 4.25, "yield_10_year": 4.50}, None
    )
    assert yields["Date"] == "1970-01-02"
    assert yields["Yield2Year"] == 4.25
    assert yields["Yield10Year"] == 4.50

    inflation = INFLATION_SCHEMA.normalizer(
        {"date": "1970-01-02", "cpi": 310.5, "cpi_year_over_year": 3.1}, None
    )
    assert inflation["Cpi"] == 310.5
    assert inflation["CpiYearOverYear"] == 3.1

    expectations = INFLATION_EXPECTATIONS_SCHEMA.normalizer(
        {"date": "1970-01-02", "market_5_year": 2.2, "forward_years_5_to_10": 2.3},
        None,
    )
    assert expectations["Market5Year"] == 2.2
    assert expectations["ForwardYears5To10"] == 2.3

    labor = LABOR_MARKET_SCHEMA.normalizer(
        {"date": "1970-01-02", "unemployment_rate": 3.9, "job_openings": 8800.0}, None
    )
    assert labor["UnemploymentRate"] == 3.9
    assert labor["JobOpenings"] == 8800.0

    for spec in (
        FUNDING_CONDITIONS_SCHEMA,
        TREASURY_YIELDS_SCHEMA,
        INFLATION_SCHEMA,
        INFLATION_EXPECTATIONS_SCHEMA,
        LABOR_MARKET_SCHEMA,
    ):
        rb = rows_to_record_batch([{"date": "2025-01-02"}], spec)
        assert rb.num_rows == 1


def test_reference_schemas_normalization() -> None:
    """Verify all 25 reference schema normalizers extract nested fields and build valid Arrow RecordBatches."""
    stocks_norm = SymbolNormalizer("stocks")
    sample_row = {
        "ticker": "AAPL",
        "symbol": "AAPL",
        "id": "id-1",
        "date": "2025-01-02",
        "address": {"city": "Cupertino", "state": "CA", "postal_code": "95014"},
        "branding": {"logo_url": "https://logo", "icon_url": "https://icon"},
        "publisher": {"name": "Benzinga", "homepage_url": "https://bz"},
        "insights": [{"sentiment": "positive", "sentiment_reasoning": "strong"}],
        "sip_mapping": {"CTA": "A"},
        "update_rules": {
            "consolidated": {"updates_high_low": True},
            "market_center": {"updates_volume": True},
        },
        "exchanges": {"nyse": "open", "nasdaq": "open", "otc": "closed"},
        "currencies": {"fx": "open", "crypto": "open"},
        "indicesGroups": {"s_and_p": "open", "dow_jones": "open", "nasdaq": "open"},
        "financials": {
            "income_statement": {"revenues": {"value": 1000.0, "unit": "USD"}}
        },
    }

    details = TICKER_DETAILS_SCHEMA.normalizer(sample_row, stocks_norm)
    assert details["Symbol"] == "AAPL"
    assert details["AddressCity"] == "Cupertino"
    assert details["BrandingLogoUrl"] == "https://logo"

    news = NEWS_SCHEMA.normalizer(sample_row, stocks_norm)
    assert news["PublisherName"] == "Benzinga"
    assert news["Sentiment"] == "positive"

    cond = CONDITIONS_SCHEMA.normalizer(sample_row, stocks_norm)
    assert cond["ConditionId"] == "id-1"

    mstatus = MARKET_STATUS_SCHEMA.normalizer(sample_row, stocks_norm)
    assert mstatus["NyseStatus"] == "open"
    assert mstatus["FxStatus"] == "open"

    fin = FINANCIAL_STATEMENT_SCHEMA.normalizer(sample_row, stocks_norm)
    assert fin["Revenue"] == 1000.0

    reference_specs = [
        SHORT_INTEREST_SCHEMA,
        SHORT_VOLUME_SCHEMA,
        SHARE_FLOAT_SCHEMA,
        FINANCIAL_STATEMENT_SCHEMA,
        RATIOS_SCHEMA,
        DIVIDENDS_SCHEMA,
        SPLITS_SCHEMA,
        IPOS_SCHEMA,
        TICKERS_SCHEMA,
        TICKER_DETAILS_SCHEMA,
        TICKER_EVENTS_SCHEMA,
        RELATED_COMPANIES_SCHEMA,
        TICKER_TYPES_SCHEMA,
        OPTION_CONTRACTS_SCHEMA,
        FUTURES_CONTRACTS_SCHEMA,
        FUTURES_PRODUCTS_SCHEMA,
        FUTURES_SCHEDULES_SCHEMA,
        SEC_FILINGS_SCHEMA,
        SEC_13F_SCHEMA,
        SEC_INSIDER_SCHEMA,
        NEWS_SCHEMA,
        EXCHANGES_SCHEMA,
        CONDITIONS_SCHEMA,
        HOLIDAYS_SCHEMA,
        MARKET_STATUS_SCHEMA,
    ]
    for spec in reference_specs:
        rb = rows_to_record_batch([sample_row], spec, stocks_norm)
        assert rb.num_rows == 1

    assert RATIOS_SCHEMA.normalizer({"ticker": "   "}, stocks_norm)["Symbol"] is None
    assert RATIOS_SCHEMA.normalizer({"ticker": "aapl"}, None)["Symbol"] == "AAPL"


def test_partner_schemas_normalization() -> None:
    """Verify all 17 partner schemas (Benzinga, ETF Global, TMX, Fable) normalize fields and build RecordBatches."""
    stocks_norm = SymbolNormalizer("stocks")
    sample_row = {
        "ticker": "AAPL",
        "symbol": "AAPL",
        "id": "id-1",
        "merchant_id": "M-100",
        "merchant_name": "Apple Retail EU",
        "parent_merchant_id": "M-001",
        "spend_amount": 125000.5,
        "transaction_count": 4200,
        "currency": "EUR",
        "country": "DE",
        "category": "Electronics",
        "date": "2025-01-02",
        "transaction_date": "2025-01-02",
    }

    fable_agg = FABLE_MERCHANT_AGGREGATES_SCHEMA.normalizer(sample_row, stocks_norm)
    assert fable_agg["MerchantId"] == "M-100"
    assert fable_agg["MerchantName"] == "Apple Retail EU"
    assert fable_agg["Symbol"] == "AAPL"
    assert fable_agg["SpendAmount"] == 125000.5
    assert fable_agg["TransactionCount"] == 4200
    assert fable_agg["Currency"] == "EUR"

    fable_hier = FABLE_MERCHANT_HIERARCHY_SCHEMA.normalizer(sample_row, stocks_norm)
    assert fable_hier["MerchantId"] == "M-100"
    assert fable_hier["ParentMerchantId"] == "M-001"
    assert fable_hier["Country"] == "DE"
    assert fable_hier["Category"] == "Electronics"

    partner_specs = [
        BENZINGA_RATINGS_SCHEMA,
        BENZINGA_EARNINGS_SCHEMA,
        BENZINGA_GUIDANCE_SCHEMA,
        BENZINGA_CONSENSUS_SCHEMA,
        BENZINGA_NEWS_SCHEMA,
        BENZINGA_ANALYSTS_SCHEMA,
        BENZINGA_FIRMS_SCHEMA,
        BENZINGA_BULLS_BEARS_SCHEMA,
        BENZINGA_TRANSCRIPTS_SCHEMA,
        ETF_GLOBAL_CONSTITUENTS_SCHEMA,
        ETF_GLOBAL_FUND_FLOWS_SCHEMA,
        ETF_GLOBAL_ANALYTICS_SCHEMA,
        ETF_GLOBAL_PROFILES_SCHEMA,
        ETF_GLOBAL_TAXONOMIES_SCHEMA,
        TMX_CORPORATE_EVENTS_SCHEMA,
        FABLE_MERCHANT_AGGREGATES_SCHEMA,
        FABLE_MERCHANT_HIERARCHY_SCHEMA,
    ]
    for spec in partner_specs:
        rb = rows_to_record_batch([sample_row], spec, stocks_norm)
        assert rb.num_rows == 1

    assert BENZINGA_RATINGS_SCHEMA.normalizer({"ticker": "   "}, None)["Symbol"] is None
    assert BENZINGA_RATINGS_SCHEMA.normalizer({}, None)["Symbol"] is None
    assert (
        BENZINGA_RATINGS_SCHEMA.normalizer({"ticker": "aapl"}, None)["Symbol"] == "AAPL"
    )
