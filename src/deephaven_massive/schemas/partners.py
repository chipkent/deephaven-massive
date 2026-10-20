"""Unified PascalCase PyArrow schemas and normalizers for Partner & Alternative feeds (Benzinga, ETF Global, TMX)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from deephaven_massive.engine.arrow_bridge import (
    DATE32_TYPE,
    STRING_LIST_TYPE,
    UTC_NS_TYPE,
    SchemaSpec,
    flatten_dict,
)

if TYPE_CHECKING:
    from deephaven_massive.engine.symbols import SymbolNormalizer


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def _norm_symbol(
    row: Mapping[str, Any],
    norm: SymbolNormalizer | None,
    *keys: str,
) -> str | None:
    raw = _first_present(row, *keys)
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    return norm.canonical(s) if norm is not None else s.upper()


def _hybrid_partner_normalizer(
    anchor_Extractors: Mapping[str, tuple[str, ...]],
) -> Any:
    """Create a normalizer that extracts anchor keys and flattens remaining fields into PascalCase."""

    def _norm(
        row: Mapping[str, Any], norm: SymbolNormalizer | None = None
    ) -> dict[str, Any]:
        flat = flatten_dict(row)
        for col_name, candidate_keys in anchor_Extractors.items():
            if col_name == "Symbol":
                flat["Symbol"] = _norm_symbol(row, norm, *candidate_keys)
            else:
                val = _first_present(row, *candidate_keys)
                if val is not None:
                    flat[col_name] = val
        return flat

    return _norm


BENZINGA_RATINGS_SCHEMA = SchemaSpec(
    name="benzinga_ratings",
    arrow_schema=pa.schema(
        [
            pa.field("BenzingaId", pa.string()),
            pa.field("Symbol", pa.string()),
            pa.field("Date", DATE32_TYPE),
            pa.field("UpdatedTimestamp", UTC_NS_TYPE),
            pa.field("Firm", pa.string()),
            pa.field("Analyst", pa.string()),
            pa.field("RatingAction", pa.string()),
            pa.field("RatingCurrent", pa.string()),
            pa.field("RatingPrior", pa.string()),
            pa.field("PriceTargetAction", pa.string()),
            pa.field("PriceTargetCurrent", pa.float64()),
            pa.field("PriceTargetPrior", pa.float64()),
            pa.field("Currency", pa.string()),
        ]
    ),
    key_cols=("BenzingaId",),
    normalizer=_hybrid_partner_normalizer(
        {
            "BenzingaId": ("BenzingaId", "benzinga_id", "id"),
            "Symbol": ("Symbol", "ticker", "symbol"),
            "Date": ("Date", "date"),
            "UpdatedTimestamp": ("UpdatedTimestamp", "last_updated", "updated"),
            "Firm": ("Firm", "firm", "analyst_firm"),
            "Analyst": ("Analyst", "analyst", "analyst_name"),
            "RatingAction": ("RatingAction", "rating_action", "action_company"),
            "RatingCurrent": ("RatingCurrent", "rating_current", "rating"),
            "RatingPrior": ("RatingPrior", "rating_prior", "previous_rating"),
            "PriceTargetAction": ("PriceTargetAction", "price_target_action", "action_pt"),
            "PriceTargetCurrent": ("PriceTargetCurrent", "price_target_current", "pt_current", "price_target"),
            "PriceTargetPrior": ("PriceTargetPrior", "price_target_prior", "pt_prior"),
            "Currency": ("Currency", "currency"),
        }
    ),
    allow_dynamic_columns=True,
)

BENZINGA_EARNINGS_SCHEMA = SchemaSpec(
    name="benzinga_earnings",
    arrow_schema=pa.schema(
        [
            pa.field("BenzingaId", pa.string()),
            pa.field("Symbol", pa.string()),
            pa.field("Date", DATE32_TYPE),
            pa.field("UpdatedTimestamp", UTC_NS_TYPE),
            pa.field("FiscalYear", pa.int64()),
            pa.field("FiscalPeriod", pa.string()),
            pa.field("EpsActual", pa.float64()),
            pa.field("EpsEstimate", pa.float64()),
            pa.field("EpsSurprise", pa.float64()),
            pa.field("EpsSurprisePercent", pa.float64()),
            pa.field("RevenueActual", pa.float64()),
            pa.field("RevenueEstimate", pa.float64()),
            pa.field("RevenueSurprise", pa.float64()),
            pa.field("RevenueSurprisePercent", pa.float64()),
        ]
    ),
    key_cols=("BenzingaId",),
    normalizer=_hybrid_partner_normalizer(
        {
            "BenzingaId": ("BenzingaId", "benzinga_id", "id"),
            "Symbol": ("Symbol", "ticker", "symbol"),
            "Date": ("Date", "date"),
            "UpdatedTimestamp": ("UpdatedTimestamp", "last_updated", "updated"),
            "FiscalYear": ("FiscalYear", "fiscal_year"),
            "FiscalPeriod": ("FiscalPeriod", "fiscal_period", "period"),
            "EpsActual": ("EpsActual", "actual_eps", "eps_actual", "eps"),
            "EpsEstimate": ("EpsEstimate", "estimated_eps", "eps_estimate", "eps_est"),
            "EpsSurprise": ("EpsSurprise", "eps_surprise"),
            "EpsSurprisePercent": ("EpsSurprisePercent", "eps_surprise_percent"),
            "RevenueActual": ("RevenueActual", "actual_revenue", "revenue_actual", "revenue"),
            "RevenueEstimate": ("RevenueEstimate", "estimated_revenue", "revenue_estimate", "revenue_est"),
            "RevenueSurprise": ("RevenueSurprise", "revenue_surprise"),
            "RevenueSurprisePercent": ("RevenueSurprisePercent", "revenue_surprise_percent"),
        }
    ),
    allow_dynamic_columns=True,
)

BENZINGA_GUIDANCE_SCHEMA = SchemaSpec(
    name="benzinga_guidance",
    arrow_schema=pa.schema(
        [
            pa.field("BenzingaId", pa.string()),
            pa.field("Symbol", pa.string()),
            pa.field("Date", DATE32_TYPE),
            pa.field("UpdatedTimestamp", UTC_NS_TYPE),
            pa.field("FiscalYear", pa.int64()),
            pa.field("FiscalPeriod", pa.string()),
            pa.field("EpsGuidanceMin", pa.float64()),
            pa.field("EpsGuidanceMax", pa.float64()),
            pa.field("EpsGuidanceEst", pa.float64()),
            pa.field("RevenueGuidanceMin", pa.float64()),
            pa.field("RevenueGuidanceMax", pa.float64()),
            pa.field("RevenueGuidanceEst", pa.float64()),
        ]
    ),
    key_cols=("BenzingaId",),
    normalizer=_hybrid_partner_normalizer(
        {
            "BenzingaId": ("BenzingaId", "benzinga_id", "id"),
            "Symbol": ("Symbol", "ticker", "symbol"),
            "Date": ("Date", "date"),
            "UpdatedTimestamp": ("UpdatedTimestamp", "last_updated", "updated"),
            "FiscalYear": ("FiscalYear", "fiscal_year"),
            "FiscalPeriod": ("FiscalPeriod", "fiscal_period", "period"),
            "EpsGuidanceMin": ("EpsGuidanceMin", "eps_guidance_min", "min_eps_guidance"),
            "EpsGuidanceMax": ("EpsGuidanceMax", "eps_guidance_max", "max_eps_guidance"),
            "EpsGuidanceEst": ("EpsGuidanceEst", "eps_guidance_est", "est_eps_guidance"),
            "RevenueGuidanceMin": ("RevenueGuidanceMin", "revenue_guidance_min", "min_revenue_guidance"),
            "RevenueGuidanceMax": ("RevenueGuidanceMax", "revenue_guidance_max", "max_revenue_guidance"),
            "RevenueGuidanceEst": ("RevenueGuidanceEst", "revenue_guidance_est", "est_revenue_guidance"),
        }
    ),
    allow_dynamic_columns=True,
)

BENZINGA_CONSENSUS_SCHEMA = SchemaSpec(
    name="benzinga_consensus_ratings",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("ConsensusRating", pa.string()),
            pa.field("ConsensusRatingValue", pa.float64()),
            pa.field("ConsensusPriceTarget", pa.float64()),
            pa.field("HighPriceTarget", pa.float64()),
            pa.field("LowPriceTarget", pa.float64()),
            pa.field("TotalAnalysts", pa.int64()),
            pa.field("StrongBuyCount", pa.int64()),
            pa.field("BuyCount", pa.int64()),
            pa.field("HoldCount", pa.int64()),
            pa.field("SellCount", pa.int64()),
            pa.field("StrongSellCount", pa.int64()),
            pa.field("UpdatedTimestamp", UTC_NS_TYPE),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_hybrid_partner_normalizer(
        {
            "Symbol": ("Symbol", "ticker", "symbol"),
            "ConsensusRating": ("ConsensusRating", "consensus_rating"),
            "ConsensusRatingValue": ("ConsensusRatingValue", "consensus_rating_val", "consensus_rating_value"),
            "ConsensusPriceTarget": ("ConsensusPriceTarget", "consensus_price_target"),
            "HighPriceTarget": ("HighPriceTarget", "high_price_target"),
            "LowPriceTarget": ("LowPriceTarget", "low_price_target"),
            "TotalAnalysts": ("TotalAnalysts", "total_analyst_count", "total_analysts"),
            "StrongBuyCount": ("StrongBuyCount", "strong_buy_count", "strong_buy"),
            "BuyCount": ("BuyCount", "buy_count", "buy"),
            "HoldCount": ("HoldCount", "hold_count", "hold"),
            "SellCount": ("SellCount", "sell_count", "sell"),
            "StrongSellCount": ("StrongSellCount", "strong_sell_count", "strong_sell"),
            "UpdatedTimestamp": ("UpdatedTimestamp", "last_updated", "updated"),
        }
    ),
    allow_dynamic_columns=True,
)

BENZINGA_NEWS_SCHEMA = SchemaSpec(
    name="benzinga_news",
    arrow_schema=pa.schema(
        [
            pa.field("BenzingaId", pa.string()),
            pa.field("PublishedTimestamp", UTC_NS_TYPE),
            pa.field("UpdatedTimestamp", UTC_NS_TYPE),
            pa.field("Title", pa.string()),
            pa.field("Author", pa.string()),
            pa.field("Teaser", pa.string()),
            pa.field("Body", pa.string()),
            pa.field("Url", pa.string()),
            pa.field("Tickers", STRING_LIST_TYPE),
            pa.field("Channels", STRING_LIST_TYPE),
            pa.field("Tags", STRING_LIST_TYPE),
        ]
    ),
    key_cols=("BenzingaId",),
    normalizer=_hybrid_partner_normalizer(
        {
            "BenzingaId": ("BenzingaId", "benzinga_id", "id"),
            "PublishedTimestamp": ("PublishedTimestamp", "published", "created"),
            "UpdatedTimestamp": ("UpdatedTimestamp", "last_updated", "updated"),
            "Title": ("Title", "title"),
            "Author": ("Author", "author"),
            "Teaser": ("Teaser", "teaser"),
            "Body": ("Body", "body"),
            "Url": ("Url", "url"),
            "Tickers": ("Tickers", "tickers"),
            "Channels": ("Channels", "channels"),
            "Tags": ("Tags", "tags"),
        }
    ),
    allow_dynamic_columns=True,
)

BENZINGA_ANALYSTS_SCHEMA = SchemaSpec(
    name="benzinga_analysts",
    arrow_schema=pa.schema(
        [
            pa.field("AnalystId", pa.string()),
            pa.field("FullName", pa.string()),
            pa.field("FirmId", pa.string()),
            pa.field("FirmName", pa.string()),
            pa.field("SmartScore", pa.float64()),
            pa.field("SuccessRate", pa.float64()),
            pa.field("AverageReturn", pa.float64()),
            pa.field("TotalRatings", pa.int64()),
            pa.field("UpdatedTimestamp", UTC_NS_TYPE),
        ]
    ),
    key_cols=("AnalystId",),
    normalizer=_hybrid_partner_normalizer(
        {
            "AnalystId": ("AnalystId", "benzinga_id", "analyst_id", "id"),
            "FullName": ("FullName", "full_name", "name"),
            "FirmId": ("FirmId", "firm_id", "benzinga_firm_id"),
            "FirmName": ("FirmName", "firm_name", "firm"),
            "SmartScore": ("SmartScore", "smart_score", "overall_score"),
            "SuccessRate": ("SuccessRate", "success_rate", "overall_success_rate"),
            "AverageReturn": ("AverageReturn", "average_return", "overall_avg_return"),
            "TotalRatings": ("TotalRatings", "total_ratings", "total_ratings_count"),
            "UpdatedTimestamp": ("UpdatedTimestamp", "last_updated", "updated"),
        }
    ),
    allow_dynamic_columns=True,
)

BENZINGA_FIRMS_SCHEMA = SchemaSpec(
    name="benzinga_firms",
    arrow_schema=pa.schema(
        [
            pa.field("FirmId", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("Website", pa.string()),
            pa.field("UpdatedTimestamp", UTC_NS_TYPE),
        ]
    ),
    key_cols=("FirmId",),
    normalizer=_hybrid_partner_normalizer(
        {
            "FirmId": ("FirmId", "benzinga_id", "firm_id", "id"),
            "Name": ("Name", "name", "firm_name"),
            "Website": ("Website", "website", "homepage"),
            "UpdatedTimestamp": ("UpdatedTimestamp", "last_updated", "updated"),
        }
    ),
    allow_dynamic_columns=True,
)

BENZINGA_BULLS_BEARS_SCHEMA = SchemaSpec(
    name="benzinga_bulls_bears",
    arrow_schema=pa.schema(
        [
            pa.field("BenzingaId", pa.string()),
            pa.field("Symbol", pa.string()),
            pa.field("BullCase", pa.string()),
            pa.field("BearCase", pa.string()),
            pa.field("UpdatedTimestamp", UTC_NS_TYPE),
        ]
    ),
    key_cols=("Symbol", "BenzingaId"),
    normalizer=_hybrid_partner_normalizer(
        {
            "BenzingaId": ("BenzingaId", "benzinga_id", "id"),
            "Symbol": ("Symbol", "ticker", "symbol"),
            "BullCase": ("BullCase", "bull_case", "bulls_say"),
            "BearCase": ("BearCase", "bear_case", "bears_say"),
            "UpdatedTimestamp": ("UpdatedTimestamp", "last_updated", "updated"),
        }
    ),
    allow_dynamic_columns=True,
)

BENZINGA_TRANSCRIPTS_SCHEMA = SchemaSpec(
    name="benzinga_transcripts",
    arrow_schema=pa.schema(
        [
            pa.field("TranscriptId", pa.string()),
            pa.field("Symbol", pa.string()),
            pa.field("Date", DATE32_TYPE),
            pa.field("FiscalYear", pa.int64()),
            pa.field("FiscalPeriod", pa.string()),
            pa.field("Title", pa.string()),
            pa.field("TranscriptText", pa.string()),
            pa.field("UpdatedTimestamp", UTC_NS_TYPE),
        ]
    ),
    key_cols=("TranscriptId",),
    normalizer=_hybrid_partner_normalizer(
        {
            "TranscriptId": ("TranscriptId", "benzinga_id", "transcript_id", "id"),
            "Symbol": ("Symbol", "ticker", "symbol"),
            "Date": ("Date", "date"),
            "FiscalYear": ("FiscalYear", "fiscal_year"),
            "FiscalPeriod": ("FiscalPeriod", "fiscal_period", "period"),
            "Title": ("Title", "title", "call_title"),
            "TranscriptText": ("TranscriptText", "transcript", "body", "text"),
            "UpdatedTimestamp": ("UpdatedTimestamp", "last_updated", "updated"),
        }
    ),
    allow_dynamic_columns=True,
)

ETF_GLOBAL_CONSTITUENTS_SCHEMA = SchemaSpec(
    name="etf_global_constituents",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("ConstituentSymbol", pa.string()),
            pa.field("ConstituentName", pa.string()),
            pa.field("EffectiveDate", DATE32_TYPE),
            pa.field("Weight", pa.float64()),
            pa.field("MarketValue", pa.float64()),
            pa.field("SharesHeld", pa.float64()),
            pa.field("AssetClass", pa.string()),
            pa.field("SecurityType", pa.string()),
            pa.field("Currency", pa.string()),
        ]
    ),
    key_cols=("Symbol", "ConstituentSymbol", "EffectiveDate"),
    normalizer=_hybrid_partner_normalizer(
        {
            "Symbol": ("Symbol", "composite_ticker", "etf_ticker", "ticker"),
            "ConstituentSymbol": ("ConstituentSymbol", "constituent_ticker", "holding_ticker"),
            "ConstituentName": ("ConstituentName", "constituent_name", "holding_name"),
            "EffectiveDate": ("EffectiveDate", "effective_date", "as_of_date", "date"),
            "Weight": ("Weight", "weight", "weighting"),
            "MarketValue": ("MarketValue", "market_value"),
            "SharesHeld": ("SharesHeld", "shares_held", "shares"),
            "AssetClass": ("AssetClass", "asset_class"),
            "SecurityType": ("SecurityType", "security_type"),
            "Currency": ("Currency", "currency"),
        }
    ),
    allow_dynamic_columns=True,
)

ETF_GLOBAL_FUND_FLOWS_SCHEMA = SchemaSpec(
    name="etf_global_fund_flows",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("EffectiveDate", DATE32_TYPE),
            pa.field("FundFlows", pa.float64()),
            pa.field("Nav", pa.float64()),
            pa.field("Aum", pa.float64()),
            pa.field("SharesOutstanding", pa.float64()),
        ]
    ),
    key_cols=("Symbol", "EffectiveDate"),
    normalizer=_hybrid_partner_normalizer(
        {
            "Symbol": ("Symbol", "composite_ticker", "etf_ticker", "ticker"),
            "EffectiveDate": ("EffectiveDate", "effective_date", "as_of_date", "date"),
            "FundFlows": ("FundFlows", "fund_flows", "net_flows", "flows"),
            "Nav": ("Nav", "nav", "net_asset_value"),
            "Aum": ("Aum", "aum", "assets_under_management"),
            "SharesOutstanding": ("SharesOutstanding", "shares_outstanding", "shares"),
        }
    ),
    allow_dynamic_columns=True,
)

ETF_GLOBAL_ANALYTICS_SCHEMA = SchemaSpec(
    name="etf_global_analytics",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("EffectiveDate", DATE32_TYPE),
            pa.field("RewardScore", pa.float64()),
            pa.field("RiskScore", pa.float64()),
            pa.field("QuantScore", pa.float64()),
            pa.field("QuantGrade", pa.string()),
            pa.field("BehavioralScore", pa.float64()),
            pa.field("FundamentalScore", pa.float64()),
            pa.field("GlobalThemeScore", pa.float64()),
        ]
    ),
    key_cols=("Symbol", "EffectiveDate"),
    normalizer=_hybrid_partner_normalizer(
        {
            "Symbol": ("Symbol", "composite_ticker", "etf_ticker", "ticker"),
            "EffectiveDate": ("EffectiveDate", "effective_date", "as_of_date", "date"),
            "RewardScore": ("RewardScore", "reward_score"),
            "RiskScore": ("RiskScore", "risk_score", "total_risk_score"),
            "QuantScore": ("QuantScore", "quant_score", "quant_total_score"),
            "QuantGrade": ("QuantGrade", "quant_grade"),
            "BehavioralScore": ("BehavioralScore", "behavioral_score"),
            "FundamentalScore": ("FundamentalScore", "fundamental_score"),
            "GlobalThemeScore": ("GlobalThemeScore", "global_theme_score"),
        }
    ),
    allow_dynamic_columns=True,
)

ETF_GLOBAL_PROFILES_SCHEMA = SchemaSpec(
    name="etf_global_profiles",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("EffectiveDate", DATE32_TYPE),
            pa.field("Issuer", pa.string()),
            pa.field("Description", pa.string()),
            pa.field("InceptionDate", DATE32_TYPE),
            pa.field("NetExpenseRatio", pa.float64()),
            pa.field("Aum", pa.float64()),
            pa.field("BenchmarkIndex", pa.string()),
            pa.field("AssetClass", pa.string()),
            pa.field("Category", pa.string()),
        ]
    ),
    key_cols=("Symbol", "EffectiveDate"),
    normalizer=_hybrid_partner_normalizer(
        {
            "Symbol": ("Symbol", "composite_ticker", "etf_ticker", "ticker"),
            "EffectiveDate": ("EffectiveDate", "effective_date", "as_of_date", "date"),
            "Issuer": ("Issuer", "issuer", "advisor"),
            "Description": ("Description", "description", "fund_description"),
            "InceptionDate": ("InceptionDate", "inception_date"),
            "NetExpenseRatio": ("NetExpenseRatio", "net_expense_ratio", "expense_ratio"),
            "Aum": ("Aum", "aum", "assets_under_management"),
            "BenchmarkIndex": ("BenchmarkIndex", "benchmark_index", "primary_benchmark"),
            "AssetClass": ("AssetClass", "asset_class"),
            "Category": ("Category", "category", "focus"),
        }
    ),
    allow_dynamic_columns=True,
)

ETF_GLOBAL_TAXONOMIES_SCHEMA = SchemaSpec(
    name="etf_global_taxonomies",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("EffectiveDate", DATE32_TYPE),
            pa.field("AssetClass", pa.string()),
            pa.field("Category", pa.string()),
            pa.field("Focus", pa.string()),
            pa.field("DevelopmentClass", pa.string()),
            pa.field("Region", pa.string()),
            pa.field("Subindustry", pa.string()),
        ]
    ),
    key_cols=("Symbol", "EffectiveDate"),
    normalizer=_hybrid_partner_normalizer(
        {
            "Symbol": ("Symbol", "composite_ticker", "etf_ticker", "ticker"),
            "EffectiveDate": ("EffectiveDate", "effective_date", "as_of_date", "date"),
            "AssetClass": ("AssetClass", "asset_class"),
            "Category": ("Category", "category"),
            "Focus": ("Focus", "focus"),
            "DevelopmentClass": ("DevelopmentClass", "development_class"),
            "Region": ("Region", "region"),
            "Subindustry": ("Subindustry", "subindustry"),
        }
    ),
    allow_dynamic_columns=True,
)

TMX_CORPORATE_EVENTS_SCHEMA = SchemaSpec(
    name="tmx_corporate_events",
    arrow_schema=pa.schema(
        [
            pa.field("EventId", pa.string()),
            pa.field("Symbol", pa.string()),
            pa.field("CompanyName", pa.string()),
            pa.field("EventType", pa.string()),
            pa.field("EventDate", DATE32_TYPE),
            pa.field("EventTimestamp", UTC_NS_TYPE),
            pa.field("Status", pa.string()),
            pa.field("Description", pa.string()),
            pa.field("Url", pa.string()),
        ]
    ),
    key_cols=("EventId",),
    normalizer=_hybrid_partner_normalizer(
        {
            "EventId": ("EventId", "tmx_record_id", "event_id", "id"),
            "Symbol": ("Symbol", "ticker", "symbol"),
            "CompanyName": ("CompanyName", "company_name", "name"),
            "EventType": ("EventType", "type", "event_type"),
            "EventDate": ("EventDate", "date", "event_date"),
            "EventTimestamp": ("EventTimestamp", "timestamp", "event_timestamp", "last_updated"),
            "Status": ("Status", "status"),
            "Description": ("Description", "description", "title"),
            "Url": ("Url", "url", "webcast_url"),
        }
    ),
    allow_dynamic_columns=True,
)

FABLE_MERCHANT_AGGREGATES_SCHEMA = SchemaSpec(
    name="fable_eu_merchant_aggregates",
    arrow_schema=pa.schema(
        [
            pa.field("MerchantId", pa.string()),
            pa.field("MerchantName", pa.string()),
            pa.field("Symbol", pa.string()),
            pa.field("TransactionDate", DATE32_TYPE),
            pa.field("SpendAmount", pa.float64()),
            pa.field("TransactionCount", pa.int64()),
            pa.field("Currency", pa.string()),
        ]
    ),
    key_cols=("MerchantId", "TransactionDate"),
    normalizer=_hybrid_partner_normalizer(
        {
            "MerchantId": ("MerchantId", "merchant_id", "id"),
            "MerchantName": ("MerchantName", "merchant_name", "name"),
            "Symbol": ("Symbol", "ticker", "symbol"),
            "TransactionDate": ("TransactionDate", "transaction_date", "date"),
            "SpendAmount": ("SpendAmount", "spend_amount", "total_spend"),
            "TransactionCount": ("TransactionCount", "transaction_count", "transactions"),
            "Currency": ("Currency", "currency"),
        }
    ),
    allow_dynamic_columns=True,
)

FABLE_MERCHANT_HIERARCHY_SCHEMA = SchemaSpec(
    name="fable_eu_merchant_hierarchy",
    arrow_schema=pa.schema(
        [
            pa.field("MerchantId", pa.string()),
            pa.field("MerchantName", pa.string()),
            pa.field("ParentMerchantId", pa.string()),
            pa.field("Symbol", pa.string()),
            pa.field("Country", pa.string()),
            pa.field("Category", pa.string()),
        ]
    ),
    key_cols=("MerchantId",),
    normalizer=_hybrid_partner_normalizer(
        {
            "MerchantId": ("MerchantId", "merchant_id", "id"),
            "MerchantName": ("MerchantName", "merchant_name", "name"),
            "ParentMerchantId": ("ParentMerchantId", "parent_merchant_id", "parent_id"),
            "Symbol": ("Symbol", "ticker", "symbol"),
            "Country": ("Country", "country"),
            "Category": ("Category", "category", "sector"),
        }
    ),
    allow_dynamic_columns=True,
)
