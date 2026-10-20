"""Tests for arrow_bridge type coercions, RecordBatch conversion, and all schema normalizers."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pyarrow as pa

from deephaven_massive.engine.arrow_bridge import (
    DATE32_TYPE,
    INT32_LIST_TYPE,
    STRING_LIST_TYPE,
    UTC_NS_TYPE,
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


def test_scalar_coercions_and_helpers() -> None:
    assert to_pascal_case("net_income_loss") == "NetIncomeLoss"
    assert to_pascal_case("camelCaseName") == "CamelCaseName"
    assert to_pascal_case("10_year_yield") == "F10YearYield"
    assert to_pascal_case("___") == "Field"

    # to_utc_ns
    assert to_utc_ns(None) is None
    assert to_utc_ns("") is None
    assert to_utc_ns(True) is None
    assert to_utc_ns(0) is None
    assert to_utc_ns(-5) is None
    assert to_utc_ns(1_700_000_000) == 1_700_000_000_000_000_000  # seconds
    assert to_utc_ns(1_700_000_000_000) == 1_700_000_000_000_000_000  # ms
    assert to_utc_ns(1_700_000_000_000_000) == 1_700_000_000_000_000_000  # us
    assert to_utc_ns(1_700_000_000_000_000_000) == 1_700_000_000_000_000_000  # ns
    assert to_utc_ns(0.0) is None
    assert to_utc_ns(1_700_000_000.0) == 1_700_000_000_000_000_000
    assert to_utc_ns(1_700_000_000_000.0) == 1_700_000_000_000_000_000
    assert to_utc_ns(1_700_000_000_000_000.0) == 1_700_000_000_000_000_000
    assert to_utc_ns(1.7e18) == int(1.7e18)
    assert to_utc_ns(datetime(2025, 1, 2, tzinfo=timezone.utc)) is not None
    assert to_utc_ns(datetime(2025, 1, 2)) is not None
    assert to_utc_ns(date(2025, 1, 2)) is not None
    assert to_utc_ns("   ") is None
    assert to_utc_ns("1700000000000") == 1_700_000_000_000_000_000
    assert to_utc_ns("2025-01-02T10:00:00Z") is not None
    assert to_utc_ns("2025-01-02T10:00:00") is not None
    assert to_utc_ns("not-a-timestamp") is None
    assert to_utc_ns(object()) is None

    # to_epoch_days
    assert to_epoch_days(None) is None
    assert to_epoch_days("") is None
    assert to_epoch_days(datetime(1970, 1, 2, tzinfo=timezone.utc)) == 1
    assert to_epoch_days(date(1970, 1, 3)) == 2
    assert to_epoch_days("   ") is None
    assert to_epoch_days("1970-01-04") == 3
    assert to_epoch_days("1700000000000") is not None
    assert to_epoch_days("invalid-date") is None
    assert to_epoch_days(1_700_000_000_000) is not None
    assert to_epoch_days(-100) is None
    assert to_epoch_days(True) is None

    # to_float / to_int / to_bool / to_str
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

    # parse_int32_list / parse_str_list
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

    # flatten_dict
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


def test_dynamic_columns_and_arrow_to_deephaven(dh_exec_ctx: object) -> None:
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
        [{"Symbol": "MSFT", "Flag": False, "Tags": ["cloud"], "DynFloat": 20.0, "ExtraMetric": 99.0}],
        dyn_spec,
        extra_columns={"__Extra": (pa.int64(), [2])},
    )
    tbl = record_batches_to_arrow_table([b1, b2], dyn_spec)
    assert "ExtraMetric" in tbl.column_names
    assert "DynBool" in tbl.column_names
    assert "DynNull" in tbl.column_names
    assert "DynStr" in tbl.column_names

    # Empty table with extra_fields
    empty_tbl = record_batches_to_arrow_table(
        [], dyn_spec, extra_fields=[pa.field("__PollId", pa.int64())]
    )
    assert "__PollId" in empty_tbl.column_names

    dh_1 = arrow_to_deephaven_table(tbl, dh_exec_ctx)  # type: ignore[arg-type]
    dh_2 = arrow_to_deephaven_table(tbl, None)
    assert dh_to_pydict(dh_1)["Symbol"] == ["AAPL", "MSFT"]
    assert dh_to_pydict(dh_2)["Symbol"] == ["AAPL", "MSFT"]


def test_all_market_economy_reference_partner_schemas() -> None:
    stocks_norm = SymbolNormalizer("stocks")
    forex_norm = SymbolNormalizer("forex")
    options_norm = SymbolNormalizer("options")

    # Exercise market schemas (including nested snapshot fallbacks)
    assert TRADES_SCHEMA.normalizer({"sym": "AAPL", "p": 150.0, "s": 10, "t": 1700000000000}, stocks_norm)["Symbol"] == "AAPL"
    assert TRADES_SCHEMA.normalizer({"ticker": "AAPL", "last_trade": {"p": 151.0}}, None)["Symbol"] == "AAPL"
    assert TRADES_SCHEMA.normalizer({"ticker": "   "}, stocks_norm)["Symbol"] is None

    assert QUOTES_SCHEMA.normalizer({"p": "EUR/USD", "a": 1.1, "b": 1.09}, forex_norm)["Symbol"] == "EUR/USD"
    assert QUOTES_SCHEMA.normalizer({"last_quote": {"Symbol": "AAPL", "P": 151.0, "p": 150.0}}, stocks_norm)["Symbol"] == "AAPL"

    assert BARS_SCHEMA.normalizer({"T": "AAPL", "o": 100, "h": 110, "l": 99, "c": 105, "v": 1000, "t": 1700000000000}, stocks_norm)["Close"] == 105
    assert BARS_SCHEMA.normalizer({"ticker": "AAPL", "day": {"o": 100, "c": 105}, "min": {"t": 1700000000000, "av": 5000}}, stocks_norm)["AccumulatedVolume"] == 5000

    assert LULD_SCHEMA.normalizer({"T": "AAPL", "h": 160.0, "l": 140.0, "i": [1], "z": 3, "t": 1700000000000, "q": 10}, stocks_norm)["HighPrice"] == 160.0
    assert IMBALANCES_SCHEMA.normalizer({"T": "AAPL", "p": 150, "i": 500, "b": 100.0}, stocks_norm)["ImbalanceShares"] == 500
    assert FMV_SCHEMA.normalizer({"sym": "AAPL", "fmv": 150.25, "t": 1700000000000}, stocks_norm)["FairMarketValue"] == 150.25

    assert INDEX_VALUES_SCHEMA.normalizer({"T": "I:SPX", "val": 5000.0}, None)["Value"] == 5000.0
    assert STOCK_SNAPSHOT_SCHEMA.normalizer(
        {
            "ticker": "AAPL",
            "day": {"o": 149.0, "h": 152.0, "l": 148.0, "c": 151.0, "v": 1000, "vw": 150.5},
            "prevDay": {"o": 148.0, "c": 149.0},
            "min": {"o": 150.5, "c": 151.0, "t": 1700000000000},
            "lastTrade": {"p": 151.0, "s": 100, "t": 1700000000000},
            "lastQuote": {"P": 151.1, "S": 1, "p": 150.9, "s": 2, "t": 1700000000000},
        },
        stocks_norm,
    )["LastTradePrice"] == 151.0

    assert OPTION_CHAIN_SCHEMA.normalizer(
        {
            "details": {"ticker": "O:AAPL251219C00150000", "contract_type": "call", "strike_price": 150.0},
            "underlying_asset": {"ticker": "AAPL", "price": 151.0},
            "greeks": {"delta": 0.55, "gamma": 0.04, "theta": -0.05, "vega": 0.12},
            "day": {"open": 5.0, "close": 5.5},
            "last_trade": {"price": 5.5, "size": 10},
            "last_quote": {"bid": 5.4, "ask": 5.6},
            "implied_volatility": 0.25,
            "open_interest": 1200,
        },
        options_norm,
    )["Delta"] == 0.55

    assert FUTURES_SNAPSHOT_SCHEMA.normalizer(
        {
            "ticker": "ESZ5",
            "details": {"product_code": "ES", "open_interest": 50000},
            "session": {"open": 5000.0, "close": 5010.0, "settlement_price": 5008.0},
            "last_trade": {"price": 5010.0},
            "last_quote": {"bid_price": 5009.75, "ask_price": 5010.25},
            "minute_bar": {"close": 5010.0},
        },
        None,
    )["SettlementPrice"] == 5008.0

    assert DAILY_SUMMARY_SCHEMA.normalizer({"symbol": "AAPL", "from": "2025-01-02", "open": 150.0, "close": 152.0}, stocks_norm)["Close"] == 152.0
    assert CURRENCY_CONVERSION_SCHEMA.normalizer({"from": "USD", "to": "EUR", "initialAmount": 100.0, "converted": 92.5, "last": {"ask": 0.926, "bid": 0.924}}, None)["ConvertedAmount"] == 92.5
    assert INDICATOR_SCHEMA.normalizer({"Symbol": "AAPL", "timestamp": 1700000000000, "value": 148.5}, stocks_norm)["Value"] == 148.5

    # Economy schemas
    for spec in (
        FUNDING_CONDITIONS_SCHEMA,
        TREASURY_YIELDS_SCHEMA,
        INFLATION_SCHEMA,
        INFLATION_EXPECTATIONS_SCHEMA,
        LABOR_MARKET_SCHEMA,
    ):
        rb = rows_to_record_batch([{"date": "2025-01-02"}], spec)
        assert rb.num_rows == 1

    # Reference & Partner schemas
    all_other_specs = [
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
    ]
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
        "update_rules": {"consolidated": {"updates_high_low": True}, "market_center": {"updates_volume": True}},
        "exchanges": {"nyse": "open", "nasdaq": "open", "otc": "closed"},
        "currencies": {"fx": "open", "crypto": "open"},
        "indicesGroups": {"s_and_p": "open", "dow_jones": "open", "nasdaq": "open"},
        "financials": {"income_statement": {"revenues": {"value": 1000.0, "unit": "USD"}}},
    }
    for spec in all_other_specs:
        rb = rows_to_record_batch([sample_row], spec, stocks_norm)
        assert rb.num_rows == 1

    # Also test empty/missing symbol in reference/partners _norm_symbol
    assert RATIOS_SCHEMA.normalizer({"ticker": "   "}, stocks_norm)["Symbol"] is None
    assert BENZINGA_RATINGS_SCHEMA.normalizer({"ticker": "   "}, None)["Symbol"] is None
    assert BENZINGA_RATINGS_SCHEMA.normalizer({}, None)["Symbol"] is None
    assert BENZINGA_RATINGS_SCHEMA.normalizer({"ticker": "aapl"}, None)["Symbol"] == "AAPL"
    assert RATIOS_SCHEMA.normalizer({"ticker": "aapl"}, None)["Symbol"] == "AAPL"
