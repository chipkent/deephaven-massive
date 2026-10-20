"""Unified PascalCase PyArrow schemas for Federal Reserve & Macroeconomic datasets."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from deephaven_massive.engine.arrow_bridge import (
    DATE32_TYPE,
    SchemaSpec,
    flatten_dict,
)
from deephaven_massive.schemas.market import _first_present

if TYPE_CHECKING:
    from deephaven_massive.engine.symbols import SymbolNormalizer


def normalize_funding_conditions(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/fed/v1/funding-conditions` rows (SOFR, EFFR, OBFR, TGCR, IORB, repo, CP)."""
    flat = flatten_dict(row)
    flat.update(
        {
            "Date": _first_present(row, "Date", "date"),
            "Effr": _first_present(row, "Effr", "effr", "effective_federal_funds_rate"),
            "FedFundsTargetLow": _first_present(
                row, "FedFundsTargetLow", "fed_funds_target_low", "target_range_lower"
            ),
            "FedFundsTargetHigh": _first_present(
                row, "FedFundsTargetHigh", "fed_funds_target_high", "target_range_upper"
            ),
            "Iorb": _first_present(
                row, "Iorb", "iorb", "interest_on_reserve_balances"
            ),
            "Sofr": _first_present(row, "Sofr", "sofr"),
            "Sofr1stPercentile": _first_present(
                row, "Sofr1stPercentile", "sofr_1st_percentile", "sofr_p1"
            ),
            "Sofr25thPercentile": _first_present(
                row, "Sofr25thPercentile", "sofr_25th_percentile", "sofr_p25"
            ),
            "Sofr75thPercentile": _first_present(
                row, "Sofr75thPercentile", "sofr_75th_percentile", "sofr_p75"
            ),
            "Sofr99thPercentile": _first_present(
                row, "Sofr99thPercentile", "sofr_99th_percentile", "sofr_p99"
            ),
            "SofrVolume": _first_present(row, "SofrVolume", "sofr_volume"),
            "Obfr": _first_present(row, "Obfr", "obfr"),
            "Obfr1stPercentile": _first_present(
                row, "Obfr1stPercentile", "obfr_1st_percentile", "obfr_p1"
            ),
            "Obfr25thPercentile": _first_present(
                row, "Obfr25thPercentile", "obfr_25th_percentile", "obfr_p25"
            ),
            "Obfr75thPercentile": _first_present(
                row, "Obfr75thPercentile", "obfr_75th_percentile", "obfr_p75"
            ),
            "Obfr99thPercentile": _first_present(
                row, "Obfr99thPercentile", "obfr_99th_percentile", "obfr_p99"
            ),
            "ObfrVolume": _first_present(row, "ObfrVolume", "obfr_volume"),
            "Tgcr": _first_present(row, "Tgcr", "tgcr"),
            "Tgcr1stPercentile": _first_present(
                row, "Tgcr1stPercentile", "tgcr_1st_percentile", "tgcr_p1"
            ),
            "Tgcr25thPercentile": _first_present(
                row, "Tgcr25thPercentile", "tgcr_25th_percentile", "tgcr_p25"
            ),
            "Tgcr75thPercentile": _first_present(
                row, "Tgcr75thPercentile", "tgcr_75th_percentile", "tgcr_p75"
            ),
            "Tgcr99thPercentile": _first_present(
                row, "Tgcr99thPercentile", "tgcr_99th_percentile", "tgcr_p99"
            ),
            "TgcrVolume": _first_present(row, "TgcrVolume", "tgcr_volume"),
            "OvernightRepoVolume": _first_present(
                row, "OvernightRepoVolume", "overnight_repo_volume", "rp_overnight_volume"
            ),
            "OvernightReverseRepoVolume": _first_present(
                row,
                "OvernightReverseRepoVolume",
                "overnight_reverse_repo_volume",
                "rrp_overnight_volume",
            ),
            "CpRate90dFinancial": _first_present(
                row, "CpRate90dFinancial", "cp_rate_90d_financial"
            ),
            "CpRate90dNonfinancial": _first_present(
                row, "CpRate90dNonfinancial", "cp_rate_90d_nonfinancial"
            ),
        }
    )
    return flat


def normalize_treasury_yields(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/fed/v1/treasury-yields` rows."""
    return {
        "Date": _first_present(row, "Date", "date"),
        "Yield1Month": _first_present(row, "Yield1Month", "yield_1_month"),
        "Yield3Month": _first_present(row, "Yield3Month", "yield_3_month"),
        "Yield6Month": _first_present(row, "Yield6Month", "yield_6_month"),
        "Yield1Year": _first_present(row, "Yield1Year", "yield_1_year"),
        "Yield2Year": _first_present(row, "Yield2Year", "yield_2_year"),
        "Yield3Year": _first_present(row, "Yield3Year", "yield_3_year"),
        "Yield5Year": _first_present(row, "Yield5Year", "yield_5_year"),
        "Yield7Year": _first_present(row, "Yield7Year", "yield_7_year"),
        "Yield10Year": _first_present(row, "Yield10Year", "yield_10_year"),
        "Yield20Year": _first_present(row, "Yield20Year", "yield_20_year"),
        "Yield30Year": _first_present(row, "Yield30Year", "yield_30_year"),
    }


def normalize_inflation(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/fed/v1/inflation` rows."""
    return {
        "Date": _first_present(row, "Date", "date"),
        "Cpi": _first_present(row, "Cpi", "cpi"),
        "CpiCore": _first_present(row, "CpiCore", "cpi_core"),
        "CpiYearOverYear": _first_present(
            row, "CpiYearOverYear", "cpi_year_over_year"
        ),
        "Pce": _first_present(row, "Pce", "pce"),
        "PceCore": _first_present(row, "PceCore", "pce_core"),
        "PceSpending": _first_present(row, "PceSpending", "pce_spending"),
    }


def normalize_inflation_expectations(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/fed/v1/inflation-expectations` rows."""
    return {
        "Date": _first_present(row, "Date", "date"),
        "Market5Year": _first_present(row, "Market5Year", "market_5_year"),
        "Market10Year": _first_present(row, "Market10Year", "market_10_year"),
        "ForwardYears5To10": _first_present(
            row, "ForwardYears5To10", "forward_years_5_to_10"
        ),
        "Model1Year": _first_present(row, "Model1Year", "model_1_year"),
        "Model5Year": _first_present(row, "Model5Year", "model_5_year"),
        "Model10Year": _first_present(row, "Model10Year", "model_10_year"),
        "Model30Year": _first_present(row, "Model30Year", "model_30_year"),
    }


def normalize_labor_market(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/fed/v1/labor-market` rows."""
    return {
        "Date": _first_present(row, "Date", "date"),
        "UnemploymentRate": _first_present(
            row, "UnemploymentRate", "unemployment_rate"
        ),
        "LaborForceParticipationRate": _first_present(
            row, "LaborForceParticipationRate", "labor_force_participation_rate"
        ),
        "AvgHourlyEarnings": _first_present(
            row, "AvgHourlyEarnings", "avg_hourly_earnings"
        ),
        "JobOpenings": _first_present(row, "JobOpenings", "job_openings"),
    }


FUNDING_CONDITIONS_SCHEMA = SchemaSpec(
    name="FundingConditions",
    arrow_schema=pa.schema(
        [
            pa.field("Date", DATE32_TYPE),
            pa.field("Effr", pa.float64()),
            pa.field("FedFundsTargetLow", pa.float64()),
            pa.field("FedFundsTargetHigh", pa.float64()),
            pa.field("Iorb", pa.float64()),
            pa.field("Sofr", pa.float64()),
            pa.field("Sofr1stPercentile", pa.float64()),
            pa.field("Sofr25thPercentile", pa.float64()),
            pa.field("Sofr75thPercentile", pa.float64()),
            pa.field("Sofr99thPercentile", pa.float64()),
            pa.field("SofrVolume", pa.float64()),
            pa.field("Obfr", pa.float64()),
            pa.field("Obfr1stPercentile", pa.float64()),
            pa.field("Obfr25thPercentile", pa.float64()),
            pa.field("Obfr75thPercentile", pa.float64()),
            pa.field("Obfr99thPercentile", pa.float64()),
            pa.field("ObfrVolume", pa.float64()),
            pa.field("Tgcr", pa.float64()),
            pa.field("Tgcr1stPercentile", pa.float64()),
            pa.field("Tgcr25thPercentile", pa.float64()),
            pa.field("Tgcr75thPercentile", pa.float64()),
            pa.field("Tgcr99thPercentile", pa.float64()),
            pa.field("TgcrVolume", pa.float64()),
            pa.field("OvernightRepoVolume", pa.float64()),
            pa.field("OvernightReverseRepoVolume", pa.float64()),
            pa.field("CpRate90dFinancial", pa.float64()),
            pa.field("CpRate90dNonfinancial", pa.float64()),
        ]
    ),
    key_cols=("Date",),
    normalizer=normalize_funding_conditions,
    allow_dynamic_columns=True,
)

TREASURY_YIELDS_SCHEMA = SchemaSpec(
    name="TreasuryYields",
    arrow_schema=pa.schema(
        [
            pa.field("Date", DATE32_TYPE),
            pa.field("Yield1Month", pa.float64()),
            pa.field("Yield3Month", pa.float64()),
            pa.field("Yield6Month", pa.float64()),
            pa.field("Yield1Year", pa.float64()),
            pa.field("Yield2Year", pa.float64()),
            pa.field("Yield3Year", pa.float64()),
            pa.field("Yield5Year", pa.float64()),
            pa.field("Yield7Year", pa.float64()),
            pa.field("Yield10Year", pa.float64()),
            pa.field("Yield20Year", pa.float64()),
            pa.field("Yield30Year", pa.float64()),
        ]
    ),
    key_cols=("Date",),
    normalizer=normalize_treasury_yields,
)

INFLATION_SCHEMA = SchemaSpec(
    name="Inflation",
    arrow_schema=pa.schema(
        [
            pa.field("Date", DATE32_TYPE),
            pa.field("Cpi", pa.float64()),
            pa.field("CpiCore", pa.float64()),
            pa.field("CpiYearOverYear", pa.float64()),
            pa.field("Pce", pa.float64()),
            pa.field("PceCore", pa.float64()),
            pa.field("PceSpending", pa.float64()),
        ]
    ),
    key_cols=("Date",),
    normalizer=normalize_inflation,
)

INFLATION_EXPECTATIONS_SCHEMA = SchemaSpec(
    name="InflationExpectations",
    arrow_schema=pa.schema(
        [
            pa.field("Date", DATE32_TYPE),
            pa.field("Market5Year", pa.float64()),
            pa.field("Market10Year", pa.float64()),
            pa.field("ForwardYears5To10", pa.float64()),
            pa.field("Model1Year", pa.float64()),
            pa.field("Model5Year", pa.float64()),
            pa.field("Model10Year", pa.float64()),
            pa.field("Model30Year", pa.float64()),
        ]
    ),
    key_cols=("Date",),
    normalizer=normalize_inflation_expectations,
)

LABOR_MARKET_SCHEMA = SchemaSpec(
    name="LaborMarket",
    arrow_schema=pa.schema(
        [
            pa.field("Date", DATE32_TYPE),
            pa.field("UnemploymentRate", pa.float64()),
            pa.field("LaborForceParticipationRate", pa.float64()),
            pa.field("AvgHourlyEarnings", pa.float64()),
            pa.field("JobOpenings", pa.float64()),
        ]
    ),
    key_cols=("Date",),
    normalizer=normalize_labor_market,
)
