"""Unified PyArrow schemas and payload normalizers for all Massive.com feeds.

This package defines the canonical, strictly typed PyArrow schemas (`pa.Schema`)
and row-level normalizer callbacks used across the entire ``deephaven_massive``
engine. Every dataset—whether delivered via real-time WebSocket frames, REST API
responses, or S3 Flat File archives—is projected onto these PascalCase schemas
so that historical and streaming tables can be combined seamlessly with
Deephaven's ``merge()``, ``aj()`` (as-of join), and ``update()`` operations.

Modules:
    market: Schemas and normalizers for trades, quotes, bars, snapshots,
        technical indicators, and futures/options market data.
    reference: Schemas and normalizers for reference data, corporate actions,
        fundamental financials, filings, news, and short interest.
    economy: Schemas and normalizers for Federal Reserve and U.S. Treasury
        macroeconomic series.
    partners: Hybrid anchor + dynamic-column schemas and normalizers for
        partner feeds (Benzinga, ETF Global, TMX, Fable Data).
"""

from __future__ import annotations

from deephaven_massive.schemas.economy import (
    FUNDING_CONDITIONS_SCHEMA,
    INFLATION_EXPECTATIONS_SCHEMA,
    INFLATION_SCHEMA,
    LABOR_MARKET_SCHEMA,
    TREASURY_YIELDS_SCHEMA,
)
from deephaven_massive.schemas.market import (
    BARS_SCHEMA,
    CURRENCY_CONVERSION_SCHEMA,
    DAILY_SUMMARY_SCHEMA,
    FMV_SCHEMA,
    FUTURES_SNAPSHOT_SCHEMA,
    IMBALANCES_SCHEMA,
    INDEX_VALUES_SCHEMA,
    INDICATOR_SCHEMA,
    LULD_SCHEMA,
    OPTION_CHAIN_SCHEMA,
    QUOTES_SCHEMA,
    STOCK_SNAPSHOT_SCHEMA,
    TRADES_SCHEMA,
)
from deephaven_massive.schemas.partners import (
    BENZINGA_ANALYSTS_SCHEMA,
    BENZINGA_BULLS_BEARS_SCHEMA,
    BENZINGA_CONSENSUS_SCHEMA,
    BENZINGA_EARNINGS_SCHEMA,
    BENZINGA_FIRMS_SCHEMA,
    BENZINGA_GUIDANCE_SCHEMA,
    BENZINGA_NEWS_SCHEMA,
    BENZINGA_RATINGS_SCHEMA,
    BENZINGA_TRANSCRIPTS_SCHEMA,
    ETF_GLOBAL_ANALYTICS_SCHEMA,
    ETF_GLOBAL_CONSTITUENTS_SCHEMA,
    ETF_GLOBAL_FUND_FLOWS_SCHEMA,
    ETF_GLOBAL_PROFILES_SCHEMA,
    ETF_GLOBAL_TAXONOMIES_SCHEMA,
    FABLE_MERCHANT_AGGREGATES_SCHEMA,
    FABLE_MERCHANT_HIERARCHY_SCHEMA,
    TMX_CORPORATE_EVENTS_SCHEMA,
)
from deephaven_massive.schemas.reference import (
    CONDITIONS_SCHEMA,
    DIVIDENDS_SCHEMA,
    EXCHANGES_SCHEMA,
    FINANCIAL_STATEMENT_SCHEMA,
    FUTURES_CONTRACTS_SCHEMA,
    FUTURES_PRODUCTS_SCHEMA,
    FUTURES_SCHEDULES_SCHEMA,
    HOLIDAYS_SCHEMA,
    IPOS_SCHEMA,
    MARKET_STATUS_SCHEMA,
    NEWS_SCHEMA,
    OPTION_CONTRACTS_SCHEMA,
    RATIOS_SCHEMA,
    RELATED_COMPANIES_SCHEMA,
    SEC_13F_SCHEMA,
    SEC_FILINGS_SCHEMA,
    SEC_INSIDER_SCHEMA,
    SHARE_FLOAT_SCHEMA,
    SHORT_INTEREST_SCHEMA,
    SHORT_VOLUME_SCHEMA,
    SPLITS_SCHEMA,
    TICKER_DETAILS_SCHEMA,
    TICKER_EVENTS_SCHEMA,
    TICKER_TYPES_SCHEMA,
    TICKERS_SCHEMA,
)

__all__ = [
    "BARS_SCHEMA",
    "BENZINGA_ANALYSTS_SCHEMA",
    "BENZINGA_BULLS_BEARS_SCHEMA",
    "BENZINGA_CONSENSUS_SCHEMA",
    "BENZINGA_EARNINGS_SCHEMA",
    "BENZINGA_FIRMS_SCHEMA",
    "BENZINGA_GUIDANCE_SCHEMA",
    "BENZINGA_NEWS_SCHEMA",
    "BENZINGA_RATINGS_SCHEMA",
    "BENZINGA_TRANSCRIPTS_SCHEMA",
    "CONDITIONS_SCHEMA",
    "CURRENCY_CONVERSION_SCHEMA",
    "DAILY_SUMMARY_SCHEMA",
    "DIVIDENDS_SCHEMA",
    "ETF_GLOBAL_ANALYTICS_SCHEMA",
    "ETF_GLOBAL_CONSTITUENTS_SCHEMA",
    "ETF_GLOBAL_FUND_FLOWS_SCHEMA",
    "ETF_GLOBAL_PROFILES_SCHEMA",
    "ETF_GLOBAL_TAXONOMIES_SCHEMA",
    "EXCHANGES_SCHEMA",
    "FABLE_MERCHANT_AGGREGATES_SCHEMA",
    "FABLE_MERCHANT_HIERARCHY_SCHEMA",
    "FINANCIAL_STATEMENT_SCHEMA",
    "FMV_SCHEMA",
    "FUNDING_CONDITIONS_SCHEMA",
    "FUTURES_CONTRACTS_SCHEMA",
    "FUTURES_PRODUCTS_SCHEMA",
    "FUTURES_SCHEDULES_SCHEMA",
    "FUTURES_SNAPSHOT_SCHEMA",
    "HOLIDAYS_SCHEMA",
    "IMBALANCES_SCHEMA",
    "INDEX_VALUES_SCHEMA",
    "INDICATOR_SCHEMA",
    "INFLATION_EXPECTATIONS_SCHEMA",
    "INFLATION_SCHEMA",
    "IPOS_SCHEMA",
    "LABOR_MARKET_SCHEMA",
    "LULD_SCHEMA",
    "MARKET_STATUS_SCHEMA",
    "NEWS_SCHEMA",
    "OPTION_CHAIN_SCHEMA",
    "OPTION_CONTRACTS_SCHEMA",
    "QUOTES_SCHEMA",
    "RATIOS_SCHEMA",
    "RELATED_COMPANIES_SCHEMA",
    "SEC_13F_SCHEMA",
    "SEC_FILINGS_SCHEMA",
    "SEC_INSIDER_SCHEMA",
    "SHARE_FLOAT_SCHEMA",
    "SHORT_INTEREST_SCHEMA",
    "SHORT_VOLUME_SCHEMA",
    "SPLITS_SCHEMA",
    "STOCK_SNAPSHOT_SCHEMA",
    "TICKERS_SCHEMA",
    "TICKER_DETAILS_SCHEMA",
    "TICKER_EVENTS_SCHEMA",
    "TICKER_TYPES_SCHEMA",
    "TMX_CORPORATE_EVENTS_SCHEMA",
    "TRADES_SCHEMA",
    "TREASURY_YIELDS_SCHEMA",
]
