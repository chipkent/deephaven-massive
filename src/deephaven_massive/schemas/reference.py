"""Unified PascalCase PyArrow schemas for Reference, Fundamentals, Short Data, Corporate Actions, and SEC Filings."""

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
from deephaven_massive.schemas.market import _first_present, _norm_symbol

if TYPE_CHECKING:
    from deephaven_massive.engine.symbols import SymbolNormalizer


def normalize_short_interest(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/stocks/v1/short-interest` rows."""
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "SettlementDate": _first_present(row, "SettlementDate", "settlement_date"),
        "ShortInterest": _first_present(row, "ShortInterest", "short_interest"),
        "AvgDailyVolume": _first_present(row, "AvgDailyVolume", "avg_daily_volume"),
        "DaysToCover": _first_present(row, "DaysToCover", "days_to_cover"),
    }


def normalize_short_volume(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/stocks/v1/short-volume` rows."""
    flat = flatten_dict(row)
    flat.update(
        {
            "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
            "Date": _first_present(row, "Date", "date"),
            "ShortVolume": _first_present(row, "ShortVolume", "short_volume"),
            "TotalVolume": _first_present(row, "TotalVolume", "total_volume"),
            "ShortVolumeRatio": _first_present(
                row, "ShortVolumeRatio", "short_volume_ratio"
            ),
            "ExemptVolume": _first_present(row, "ExemptVolume", "exempt_volume"),
            "NonExemptVolume": _first_present(
                row, "NonExemptVolume", "non_exempt_volume"
            ),
            "NasdaqCarteretShortVolume": _first_present(
                row,
                "NasdaqCarteretShortVolume",
                "nasdaq_carteret_short_volume",
            ),
            "NasdaqChicagoShortVolume": _first_present(
                row,
                "NasdaqChicagoShortVolume",
                "nasdaq_chicago_short_volume",
            ),
            "NyseShortVolume": _first_present(
                row, "NyseShortVolume", "nyse_short_volume"
            ),
            "ArcaShortVolume": _first_present(
                row, "ArcaShortVolume", "adf_short_volume", "arca_short_volume"
            ),
        }
    )
    return flat


def normalize_share_float(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/stocks/vX/float` rows."""
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "EffectiveDate": _first_present(row, "EffectiveDate", "effective_date", "date"),
        "FreeFloat": _first_present(row, "FreeFloat", "free_float"),
        "FreeFloatPercent": _first_present(
            row, "FreeFloatPercent", "free_float_percent"
        ),
        "OutstandingShares": _first_present(
            row, "OutstandingShares", "outstanding_shares", "share_class_shares_outstanding"
        ),
    }


def normalize_financial_statement(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize balance sheets, income statements, and cash flow statements (supporting both v1 and vX formats)."""
    flat = flatten_dict(row)
    fin = row.get("financials") if isinstance(row.get("financials"), Mapping) else {}
    for section in fin.values():
        flat.update(flatten_dict(section) if isinstance(section, Mapping) else {})

    tickers_val = row.get("tickers")
    first_ticker = (
        tickers_val[0]
        if isinstance(tickers_val, list) and tickers_val
        else _first_present(row, "Symbol", "ticker")
    )
    sym = norm.canonical(str(first_ticker)) if (first_ticker and norm) else (str(first_ticker) if first_ticker else None)

    flat.update(
        {
            "Symbol": sym,
            "Cik": _first_present(row, "Cik", "cik"),
            "CompanyName": _first_present(row, "CompanyName", "company_name"),
            "FilingDate": _first_present(row, "FilingDate", "filing_date"),
            "PeriodEnd": _first_present(
                row, "PeriodEnd", "period_of_report_date", "end_date"
            ),
            "FiscalPeriod": _first_present(row, "FiscalPeriod", "fiscal_period"),
            "FiscalYear": _first_present(row, "FiscalYear", "fiscal_year"),
            "Timeframe": _first_present(row, "Timeframe", "timeframe"),
            "Revenue": _first_present(flat, "Revenue", "Revenues", "TotalRevenue"),
            "NetIncome": _first_present(
                flat, "NetIncome", "NetIncomeLoss", "ConsolidatedNetIncomeLoss"
            ),
            "OperatingIncome": _first_present(
                flat, "OperatingIncome", "OperatingIncomeLoss"
            ),
            "DilutedEps": _first_present(
                flat, "DilutedEps", "DilutedEarningsPerShare"
            ),
            "TotalAssets": _first_present(flat, "TotalAssets", "Assets"),
            "TotalLiabilities": _first_present(flat, "TotalLiabilities", "Liabilities"),
            "TotalEquity": _first_present(
                flat, "TotalEquity", "Equity", "StockholdersEquity"
            ),
            "CashAndEquivalents": _first_present(
                flat, "CashAndEquivalents", "CashAndCashEquivalentsAtCarryingValue"
            ),
            "OperatingCashFlow": _first_present(
                flat,
                "OperatingCashFlow",
                "NetCashFlowFromOperatingActivities",
            ),
            "CapitalExpenditure": _first_present(
                flat, "CapitalExpenditure", "CapitalExpenditures"
            ),
            "FreeCashFlow": _first_present(flat, "FreeCashFlow"),
        }
    )
    return flat


def normalize_ratios(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/stocks/financials/v1/ratios` rows."""
    flat = flatten_dict(row)
    flat.update(
        {
            "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
            "Date": _first_present(row, "Date", "date", "period_end"),
            "PriceToEarnings": _first_present(
                row, "PriceToEarnings", "price_to_earnings", "pe_ratio"
            ),
            "PriceToBook": _first_present(
                row, "PriceToBook", "price_to_book", "pb_ratio"
            ),
            "PriceToSales": _first_present(
                row, "PriceToSales", "price_to_sales", "ps_ratio"
            ),
            "PriceToFreeCashFlow": _first_present(
                row, "PriceToFreeCashFlow", "price_to_free_cash_flow", "pfcf_ratio"
            ),
            "EnterpriseValue": _first_present(
                row, "EnterpriseValue", "enterprise_value", "ev"
            ),
            "EvToEbitda": _first_present(row, "EvToEbitda", "ev_to_ebitda"),
            "ReturnOnEquity": _first_present(
                row, "ReturnOnEquity", "return_on_equity", "roe"
            ),
            "ReturnOnAssets": _first_present(
                row, "ReturnOnAssets", "return_on_assets", "roa"
            ),
            "CurrentRatio": _first_present(row, "CurrentRatio", "current_ratio"),
            "QuickRatio": _first_present(row, "QuickRatio", "quick_ratio"),
            "DebtToEquity": _first_present(row, "DebtToEquity", "debt_to_equity"),
            "DividendYield": _first_present(row, "DividendYield", "dividend_yield"),
        }
    )
    return flat


def normalize_dividend(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize dividend declaration rows."""
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "ExDividendDate": _first_present(row, "ExDividendDate", "ex_dividend_date"),
        "RecordDate": _first_present(row, "RecordDate", "record_date"),
        "PayDate": _first_present(row, "PayDate", "pay_date"),
        "DeclarationDate": _first_present(row, "DeclarationDate", "declaration_date"),
        "CashAmount": _first_present(row, "CashAmount", "cash_amount"),
        "Currency": _first_present(row, "Currency", "currency"),
        "DividendType": _first_present(row, "DividendType", "dividend_type"),
        "Frequency": _first_present(row, "Frequency", "frequency"),
    }


def normalize_split(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize stock split rows."""
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "ExecutionDate": _first_present(row, "ExecutionDate", "execution_date"),
        "SplitFrom": _first_present(row, "SplitFrom", "split_from"),
        "SplitTo": _first_present(row, "SplitTo", "split_to"),
    }


def normalize_ipo(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/vX/reference/ipos` rows."""
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "IssuerName": _first_present(row, "IssuerName", "issuer_name"),
        "ListingDate": _first_present(row, "ListingDate", "listing_date"),
        "IpoStatus": _first_present(row, "IpoStatus", "ipo_status"),
        "FinalIssuePrice": _first_present(row, "FinalIssuePrice", "final_issue_price"),
        "LowestOfferPrice": _first_present(
            row, "LowestOfferPrice", "lowest_offer_price"
        ),
        "HighestOfferPrice": _first_present(
            row, "HighestOfferPrice", "highest_offer_price"
        ),
        "SharesOffered": _first_present(
            row, "SharesOffered", "total_shares_offered", "shares_outstanding"
        ),
        "PrimaryExchange": _first_present(row, "PrimaryExchange", "primary_exchange"),
        "SecurityType": _first_present(row, "SecurityType", "security_type"),
        "UsCode": _first_present(row, "UsCode", "us_code", "isin"),
    }


def normalize_ticker(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v3/reference/tickers` directory rows."""
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "Name": _first_present(row, "Name", "name"),
        "Market": _first_present(row, "Market", "market"),
        "Locale": _first_present(row, "Locale", "locale"),
        "PrimaryExchange": _first_present(row, "PrimaryExchange", "primary_exchange"),
        "Type": _first_present(row, "Type", "type"),
        "Active": _first_present(row, "Active", "active"),
        "CurrencyName": _first_present(
            row, "CurrencyName", "currency_name", "currency_symbol"
        ),
        "Cik": _first_present(row, "Cik", "cik"),
        "CompositeFigi": _first_present(row, "CompositeFigi", "composite_figi"),
        "ShareClassFigi": _first_present(row, "ShareClassFigi", "share_class_figi"),
        "LastUpdatedUtc": _first_present(row, "LastUpdatedUtc", "last_updated_utc"),
    }


def normalize_ticker_details(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v3/reference/tickers/{ticker}` detailed profile."""
    addr = row.get("address") if isinstance(row.get("address"), Mapping) else {}
    flat = flatten_dict(row)
    flat.update(
        {
            "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
            "Name": _first_present(row, "Name", "name"),
            "Market": _first_present(row, "Market", "market"),
            "Locale": _first_present(row, "Locale", "locale"),
            "PrimaryExchange": _first_present(
                row, "PrimaryExchange", "primary_exchange"
            ),
            "Type": _first_present(row, "Type", "type"),
            "Active": _first_present(row, "Active", "active"),
            "CurrencyName": _first_present(row, "CurrencyName", "currency_name"),
            "Cik": _first_present(row, "Cik", "cik"),
            "CompositeFigi": _first_present(row, "CompositeFigi", "composite_figi"),
            "MarketCap": _first_present(row, "MarketCap", "market_cap"),
            "PhoneNumber": _first_present(row, "PhoneNumber", "phone_number"),
            "Address1": _first_present(row, "Address1") or _first_present(addr, "address1"),
            "City": _first_present(row, "City") or _first_present(addr, "city"),
            "State": _first_present(row, "State") or _first_present(addr, "state"),
            "PostalCode": _first_present(row, "PostalCode")
            or _first_present(addr, "postal_code"),
            "Description": _first_present(row, "Description", "description"),
            "SicCode": _first_present(row, "SicCode", "sic_code"),
            "SicDescription": _first_present(
                row, "SicDescription", "sic_description"
            ),
            "HomepageUrl": _first_present(row, "HomepageUrl", "homepage_url"),
            "TotalEmployees": _first_present(row, "TotalEmployees", "total_employees"),
            "ListDate": _first_present(row, "ListDate", "list_date"),
            "WeightedSharesOutstanding": _first_present(
                row,
                "WeightedSharesOutstanding",
                "weighted_shares_outstanding",
            ),
        }
    )
    return flat


def normalize_ticker_event(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/vX/reference/tickers/{id}/events` rows."""
    tc = row.get("ticker_change") if isinstance(row.get("ticker_change"), Mapping) else {}
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "EventType": _first_present(row, "EventType", "type"),
        "Date": _first_present(row, "Date", "date"),
        "NewTicker": _first_present(row, "NewTicker") or _first_present(tc, "ticker"),
    }


def normalize_related_company(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v1/related-companies/{ticker}` rows."""
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "symbol"),
        "RelatedSymbol": _norm_symbol(row, norm, "RelatedSymbol", "ticker"),
    }


def normalize_ticker_type(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v3/reference/tickers/types` rows."""
    return {
        "Code": _first_present(row, "Code", "code"),
        "Description": _first_present(row, "Description", "description"),
        "AssetClass": _first_present(row, "AssetClass", "asset_class"),
        "Locale": _first_present(row, "Locale", "locale"),
    }


def normalize_option_contract(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v3/reference/options/contracts` rows."""
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "UnderlyingSymbol": _first_present(
            row, "UnderlyingSymbol", "underlying_ticker"
        ),
        "ContractType": _first_present(row, "ContractType", "contract_type"),
        "ExerciseStyle": _first_present(row, "ExerciseStyle", "exercise_style"),
        "ExpirationDate": _first_present(row, "ExpirationDate", "expiration_date"),
        "StrikePrice": _first_present(row, "StrikePrice", "strike_price"),
        "SharesPerContract": _first_present(
            row, "SharesPerContract", "shares_per_contract"
        ),
        "PrimaryExchange": _first_present(row, "PrimaryExchange", "primary_exchange"),
        "Cfi": _first_present(row, "Cfi", "cfi"),
    }


def normalize_futures_contract(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/futures/vX/contracts` rows."""
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "ProductCode": _first_present(row, "ProductCode", "product_code"),
        "Name": _first_present(row, "Name", "name"),
        "TradingVenue": _first_present(row, "TradingVenue", "trading_venue"),
        "Type": _first_present(row, "Type", "type"),
        "Active": _first_present(row, "Active", "active"),
        "FirstTradeDate": _first_present(row, "FirstTradeDate", "first_trade_date"),
        "LastTradeDate": _first_present(row, "LastTradeDate", "last_trade_date"),
        "SettlementDate": _first_present(row, "SettlementDate", "settlement_date"),
        "ExpirationDate": _first_present(row, "ExpirationDate", "expiration_date"),
        "TickSize": _first_present(row, "TickSize", "tick_size", "min_tick"),
    }


def normalize_futures_product(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/futures/vX/products` rows."""
    return {
        "ProductCode": _first_present(row, "ProductCode", "product_code"),
        "Name": _first_present(row, "Name", "name"),
        "TradingVenue": _first_present(row, "TradingVenue", "trading_venue"),
        "AssetClass": _first_present(row, "AssetClass", "asset_class"),
        "SubAssetClass": _first_present(row, "SubAssetClass", "sub_asset_class"),
        "UnitOfMeasure": _first_present(row, "UnitOfMeasure", "unit_of_measure"),
        "SettlementMethod": _first_present(
            row, "SettlementMethod", "settlement_method"
        ),
        "SettlementType": _first_present(row, "SettlementType", "settlement_type"),
        "PriceQuotation": _first_present(row, "PriceQuotation", "price_quotation"),
    }


def normalize_futures_schedule(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/futures/vX/schedules` rows."""
    return {
        "SessionEndDate": _first_present(row, "SessionEndDate", "session_end_date"),
        "ProductCode": _first_present(row, "ProductCode", "product_code"),
        "ProductName": _first_present(row, "ProductName", "product_name"),
        "TradingVenue": _first_present(row, "TradingVenue", "trading_venue"),
        "Event": _first_present(row, "Event", "event"),
        "Timestamp": _first_present(row, "Timestamp", "timestamp"),
    }


def normalize_sec_filing(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize SEC EDGAR index, 10-K sections, 8-K text, risk factors, and disclosures."""
    flat = flatten_dict(row)
    flat.update(
        {
            "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
            "Cik": _first_present(row, "Cik", "cik"),
            "AccessionNumber": _first_present(
                row, "AccessionNumber", "accession_number"
            ),
            "FormType": _first_present(row, "FormType", "form_type", "type"),
            "FilingDate": _first_present(row, "FilingDate", "filing_date"),
            "ReportDate": _first_present(
                row, "ReportDate", "report_date", "period_end"
            ),
            "FilingUrl": _first_present(row, "FilingUrl", "filing_url", "url"),
            "Section": _first_present(
                row, "Section", "section", "category", "taxonomy"
            ),
            "Title": _first_present(row, "Title", "title", "item", "topic"),
            "Text": _first_present(row, "Text", "text", "content", "description"),
        }
    )
    return flat


def normalize_sec_13f(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/stocks/filings/13F/v1/holdings` rows."""
    flat = flatten_dict(row)
    flat.update(
        {
            "Cik": _first_present(row, "Cik", "cik"),
            "FilerName": _first_present(
                row, "FilerName", "filer_name", "company_name"
            ),
            "PeriodOfReport": _first_present(
                row, "PeriodOfReport", "period_of_report", "report_date"
            ),
            "FilingDate": _first_present(row, "FilingDate", "filing_date"),
            "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
            "SecurityName": _first_present(
                row, "SecurityName", "security_name", "name_of_issuer"
            ),
            "Cusip": _first_present(row, "Cusip", "cusip"),
            "Value": _first_present(row, "Value", "value"),
            "Shares": _first_present(row, "Shares", "shares", "ssh_prnamt"),
            "Discretion": _first_present(
                row, "Discretion", "investment_discretion", "discretion"
            ),
            "PutCall": _first_present(row, "PutCall", "put_call"),
        }
    )
    return flat


def normalize_sec_insider(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize Form 3 and Form 4 insider ownership and transaction rows."""
    flat = flatten_dict(row)
    flat.update(
        {
            "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
            "Cik": _first_present(row, "Cik", "cik", "issuer_cik"),
            "InsiderName": _first_present(
                row, "InsiderName", "insider_name", "reporting_owner_name"
            ),
            "InsiderTitle": _first_present(
                row, "InsiderTitle", "insider_title", "officer_title"
            ),
            "AccessionNumber": _first_present(
                row, "AccessionNumber", "accession_number"
            ),
            "FormType": _first_present(row, "FormType", "form_type"),
            "FilingDate": _first_present(row, "FilingDate", "filing_date"),
            "TransactionDate": _first_present(
                row, "TransactionDate", "transaction_date"
            ),
            "TransactionCode": _first_present(
                row, "TransactionCode", "transaction_code"
            ),
            "Shares": _first_present(
                row, "Shares", "shares", "transaction_shares", "shares_owned"
            ),
            "PricePerShare": _first_present(
                row, "PricePerShare", "price_per_share", "transaction_price_per_share"
            ),
            "SharesOwnedAfter": _first_present(
                row, "SharesOwnedAfter", "shares_owned_following_transaction"
            ),
        }
    )
    return flat


def normalize_news(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v2/reference/news` rows with publisher and sentiment insights."""
    pub = row.get("publisher") if isinstance(row.get("publisher"), Mapping) else {}
    insights = row.get("insights") if isinstance(row.get("insights"), list) else []
    first_insight = (
        insights[0] if insights and isinstance(insights[0], Mapping) else {}
    )
    return {
        "ArticleId": _first_present(row, "ArticleId", "id"),
        "PublishedUtc": _first_present(row, "PublishedUtc", "published_utc", "published"),
        "Title": _first_present(row, "Title", "title"),
        "Author": _first_present(row, "Author", "author"),
        "PublisherName": _first_present(row, "PublisherName") or _first_present(pub, "name"),
        "ArticleUrl": _first_present(row, "ArticleUrl", "article_url", "url"),
        "Symbols": _first_present(row, "Symbols", "tickers"),
        "Description": _first_present(row, "Description", "description"),
        "Keywords": _first_present(row, "Keywords", "keywords"),
        "Sentiment": _first_present(row, "Sentiment")
        or _first_present(first_insight, "sentiment"),
        "SentimentReasoning": _first_present(row, "SentimentReasoning")
        or _first_present(first_insight, "sentiment_reasoning"),
    }


def normalize_exchange(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v3/reference/exchanges` and `/futures/vX/exchanges` rows."""
    return {
        "ExchangeId": _first_present(row, "ExchangeId", "id"),
        "Type": _first_present(row, "Type", "type"),
        "AssetClass": _first_present(row, "AssetClass", "asset_class"),
        "Locale": _first_present(row, "Locale", "locale"),
        "Name": _first_present(row, "Name", "name"),
        "Acronym": _first_present(row, "Acronym", "acronym"),
        "Mic": _first_present(row, "Mic", "mic"),
        "OperatingMic": _first_present(row, "OperatingMic", "operating_mic"),
        "ParticipantId": _first_present(row, "ParticipantId", "participant_id"),
        "Url": _first_present(row, "Url", "url"),
    }


def normalize_condition(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v3/reference/conditions` rows."""
    return {
        "ConditionId": _first_present(row, "ConditionId", "id"),
        "Type": _first_present(row, "Type", "type"),
        "Name": _first_present(row, "Name", "name"),
        "AssetClass": _first_present(row, "AssetClass", "asset_class"),
        "DataTypes": _first_present(row, "DataTypes", "data_types"),
        "Legacy": _first_present(row, "Legacy", "legacy"),
    }


def normalize_holiday(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v1/marketstatus/upcoming` rows."""
    return {
        "Date": _first_present(row, "Date", "date"),
        "Exchange": _first_present(row, "Exchange", "exchange"),
        "Name": _first_present(row, "Name", "name"),
        "Status": _first_present(row, "Status", "status"),
        "OpenTime": _first_present(row, "OpenTime", "open"),
        "CloseTime": _first_present(row, "CloseTime", "close"),
    }


def normalize_market_status(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize `/v1/marketstatus/now` rows."""
    exchanges = row.get("exchanges") if isinstance(row.get("exchanges"), Mapping) else {}
    currencies = (
        row.get("currencies") if isinstance(row.get("currencies"), Mapping) else {}
    )
    return {
        "ServerTime": _first_present(row, "ServerTime", "serverTime", "server_time"),
        "Market": _first_present(row, "Market", "market"),
        "EarlyHours": _first_present(row, "EarlyHours", "earlyHours", "early_hours"),
        "AfterHours": _first_present(row, "AfterHours", "afterHours", "after_hours"),
        "NyseStatus": _first_present(row, "NyseStatus") or _first_present(exchanges, "nyse"),
        "NasdaqStatus": _first_present(row, "NasdaqStatus") or _first_present(exchanges, "nasdaq"),
        "OtcStatus": _first_present(row, "OtcStatus") or _first_present(exchanges, "otc"),
        "CryptoStatus": _first_present(row, "CryptoStatus") or _first_present(currencies, "crypto"),
        "FxStatus": _first_present(row, "FxStatus") or _first_present(currencies, "fx"),
    }


SHORT_INTEREST_SCHEMA = SchemaSpec(
    name="ShortInterest",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("SettlementDate", DATE32_TYPE),
            pa.field("ShortInterest", pa.int64()),
            pa.field("AvgDailyVolume", pa.float64()),
            pa.field("DaysToCover", pa.float64()),
        ]
    ),
    key_cols=("Symbol", "SettlementDate"),
    normalizer=normalize_short_interest,
)

SHORT_VOLUME_SCHEMA = SchemaSpec(
    name="ShortVolume",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Date", DATE32_TYPE),
            pa.field("ShortVolume", pa.float64()),
            pa.field("TotalVolume", pa.float64()),
            pa.field("ShortVolumeRatio", pa.float64()),
            pa.field("ExemptVolume", pa.float64()),
            pa.field("NonExemptVolume", pa.float64()),
            pa.field("NasdaqCarteretShortVolume", pa.float64()),
            pa.field("NasdaqChicagoShortVolume", pa.float64()),
            pa.field("NyseShortVolume", pa.float64()),
            pa.field("ArcaShortVolume", pa.float64()),
        ]
    ),
    key_cols=("Symbol", "Date"),
    normalizer=normalize_short_volume,
    allow_dynamic_columns=True,
)

SHARE_FLOAT_SCHEMA = SchemaSpec(
    name="ShareFloat",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("EffectiveDate", DATE32_TYPE),
            pa.field("FreeFloat", pa.float64()),
            pa.field("FreeFloatPercent", pa.float64()),
            pa.field("OutstandingShares", pa.float64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=normalize_share_float,
)

FINANCIAL_STATEMENT_SCHEMA = SchemaSpec(
    name="FinancialStatements",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Cik", pa.string()),
            pa.field("CompanyName", pa.string()),
            pa.field("FilingDate", DATE32_TYPE),
            pa.field("PeriodEnd", DATE32_TYPE),
            pa.field("FiscalPeriod", pa.string()),
            pa.field("FiscalYear", pa.string()),
            pa.field("Timeframe", pa.string()),
            pa.field("Revenue", pa.float64()),
            pa.field("NetIncome", pa.float64()),
            pa.field("OperatingIncome", pa.float64()),
            pa.field("DilutedEps", pa.float64()),
            pa.field("TotalAssets", pa.float64()),
            pa.field("TotalLiabilities", pa.float64()),
            pa.field("TotalEquity", pa.float64()),
            pa.field("CashAndEquivalents", pa.float64()),
            pa.field("OperatingCashFlow", pa.float64()),
            pa.field("CapitalExpenditure", pa.float64()),
            pa.field("FreeCashFlow", pa.float64()),
        ]
    ),
    key_cols=("Symbol", "PeriodEnd", "FiscalPeriod"),
    normalizer=normalize_financial_statement,
    allow_dynamic_columns=True,
)

RATIOS_SCHEMA = SchemaSpec(
    name="Ratios",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Date", DATE32_TYPE),
            pa.field("PriceToEarnings", pa.float64()),
            pa.field("PriceToBook", pa.float64()),
            pa.field("PriceToSales", pa.float64()),
            pa.field("PriceToFreeCashFlow", pa.float64()),
            pa.field("EnterpriseValue", pa.float64()),
            pa.field("EvToEbitda", pa.float64()),
            pa.field("ReturnOnEquity", pa.float64()),
            pa.field("ReturnOnAssets", pa.float64()),
            pa.field("CurrentRatio", pa.float64()),
            pa.field("QuickRatio", pa.float64()),
            pa.field("DebtToEquity", pa.float64()),
            pa.field("DividendYield", pa.float64()),
        ]
    ),
    key_cols=("Symbol", "Date"),
    normalizer=normalize_ratios,
    allow_dynamic_columns=True,
)

DIVIDENDS_SCHEMA = SchemaSpec(
    name="Dividends",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("ExDividendDate", DATE32_TYPE),
            pa.field("RecordDate", DATE32_TYPE),
            pa.field("PayDate", DATE32_TYPE),
            pa.field("DeclarationDate", DATE32_TYPE),
            pa.field("CashAmount", pa.float64()),
            pa.field("Currency", pa.string()),
            pa.field("DividendType", pa.string()),
            pa.field("Frequency", pa.int32()),
        ]
    ),
    key_cols=("Symbol", "ExDividendDate"),
    normalizer=normalize_dividend,
)

SPLITS_SCHEMA = SchemaSpec(
    name="Splits",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("ExecutionDate", DATE32_TYPE),
            pa.field("SplitFrom", pa.float64()),
            pa.field("SplitTo", pa.float64()),
        ]
    ),
    key_cols=("Symbol", "ExecutionDate"),
    normalizer=normalize_split,
)

IPOS_SCHEMA = SchemaSpec(
    name="Ipos",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("IssuerName", pa.string()),
            pa.field("ListingDate", DATE32_TYPE),
            pa.field("IpoStatus", pa.string()),
            pa.field("FinalIssuePrice", pa.float64()),
            pa.field("LowestOfferPrice", pa.float64()),
            pa.field("HighestOfferPrice", pa.float64()),
            pa.field("SharesOffered", pa.float64()),
            pa.field("PrimaryExchange", pa.string()),
            pa.field("SecurityType", pa.string()),
            pa.field("UsCode", pa.string()),
        ]
    ),
    key_cols=("Symbol", "ListingDate"),
    normalizer=normalize_ipo,
)

TICKERS_SCHEMA = SchemaSpec(
    name="Tickers",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("Market", pa.string()),
            pa.field("Locale", pa.string()),
            pa.field("PrimaryExchange", pa.string()),
            pa.field("Type", pa.string()),
            pa.field("Active", pa.bool_()),
            pa.field("CurrencyName", pa.string()),
            pa.field("Cik", pa.string()),
            pa.field("CompositeFigi", pa.string()),
            pa.field("ShareClassFigi", pa.string()),
            pa.field("LastUpdatedUtc", UTC_NS_TYPE),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=normalize_ticker,
)

TICKER_DETAILS_SCHEMA = SchemaSpec(
    name="TickerDetails",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("Market", pa.string()),
            pa.field("Locale", pa.string()),
            pa.field("PrimaryExchange", pa.string()),
            pa.field("Type", pa.string()),
            pa.field("Active", pa.bool_()),
            pa.field("CurrencyName", pa.string()),
            pa.field("Cik", pa.string()),
            pa.field("CompositeFigi", pa.string()),
            pa.field("MarketCap", pa.float64()),
            pa.field("PhoneNumber", pa.string()),
            pa.field("Address1", pa.string()),
            pa.field("City", pa.string()),
            pa.field("State", pa.string()),
            pa.field("PostalCode", pa.string()),
            pa.field("Description", pa.string()),
            pa.field("SicCode", pa.string()),
            pa.field("SicDescription", pa.string()),
            pa.field("HomepageUrl", pa.string()),
            pa.field("TotalEmployees", pa.int64()),
            pa.field("ListDate", DATE32_TYPE),
            pa.field("WeightedSharesOutstanding", pa.float64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=normalize_ticker_details,
    allow_dynamic_columns=True,
)

TICKER_EVENTS_SCHEMA = SchemaSpec(
    name="TickerEvents",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("EventType", pa.string()),
            pa.field("Date", DATE32_TYPE),
            pa.field("NewTicker", pa.string()),
        ]
    ),
    key_cols=("Symbol", "Date"),
    normalizer=normalize_ticker_event,
)

RELATED_COMPANIES_SCHEMA = SchemaSpec(
    name="RelatedCompanies",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("RelatedSymbol", pa.string()),
        ]
    ),
    key_cols=("Symbol", "RelatedSymbol"),
    normalizer=normalize_related_company,
)

TICKER_TYPES_SCHEMA = SchemaSpec(
    name="TickerTypes",
    arrow_schema=pa.schema(
        [
            pa.field("Code", pa.string()),
            pa.field("Description", pa.string()),
            pa.field("AssetClass", pa.string()),
            pa.field("Locale", pa.string()),
        ]
    ),
    key_cols=("Code",),
    normalizer=normalize_ticker_type,
)

OPTION_CONTRACTS_SCHEMA = SchemaSpec(
    name="OptionContracts",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("UnderlyingSymbol", pa.string()),
            pa.field("ContractType", pa.string()),
            pa.field("ExerciseStyle", pa.string()),
            pa.field("ExpirationDate", DATE32_TYPE),
            pa.field("StrikePrice", pa.float64()),
            pa.field("SharesPerContract", pa.float64()),
            pa.field("PrimaryExchange", pa.string()),
            pa.field("Cfi", pa.string()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=normalize_option_contract,
)

FUTURES_CONTRACTS_SCHEMA = SchemaSpec(
    name="FuturesContracts",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("ProductCode", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("TradingVenue", pa.string()),
            pa.field("Type", pa.string()),
            pa.field("Active", pa.bool_()),
            pa.field("FirstTradeDate", DATE32_TYPE),
            pa.field("LastTradeDate", DATE32_TYPE),
            pa.field("SettlementDate", DATE32_TYPE),
            pa.field("ExpirationDate", DATE32_TYPE),
            pa.field("TickSize", pa.float64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=normalize_futures_contract,
)

FUTURES_PRODUCTS_SCHEMA = SchemaSpec(
    name="FuturesProducts",
    arrow_schema=pa.schema(
        [
            pa.field("ProductCode", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("TradingVenue", pa.string()),
            pa.field("AssetClass", pa.string()),
            pa.field("SubAssetClass", pa.string()),
            pa.field("UnitOfMeasure", pa.string()),
            pa.field("SettlementMethod", pa.string()),
            pa.field("SettlementType", pa.string()),
            pa.field("PriceQuotation", pa.string()),
        ]
    ),
    key_cols=("ProductCode",),
    normalizer=normalize_futures_product,
)

FUTURES_SCHEDULES_SCHEMA = SchemaSpec(
    name="FuturesSchedules",
    arrow_schema=pa.schema(
        [
            pa.field("SessionEndDate", DATE32_TYPE),
            pa.field("ProductCode", pa.string()),
            pa.field("ProductName", pa.string()),
            pa.field("TradingVenue", pa.string()),
            pa.field("Event", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
        ]
    ),
    key_cols=("ProductCode", "Event", "Timestamp"),
    normalizer=normalize_futures_schedule,
)

SEC_FILINGS_SCHEMA = SchemaSpec(
    name="SecFilings",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Cik", pa.string()),
            pa.field("AccessionNumber", pa.string()),
            pa.field("FormType", pa.string()),
            pa.field("FilingDate", DATE32_TYPE),
            pa.field("ReportDate", DATE32_TYPE),
            pa.field("FilingUrl", pa.string()),
            pa.field("Section", pa.string()),
            pa.field("Title", pa.string()),
            pa.field("Text", pa.string()),
        ]
    ),
    key_cols=("AccessionNumber",),
    normalizer=normalize_sec_filing,
    allow_dynamic_columns=True,
)

SEC_13F_SCHEMA = SchemaSpec(
    name="Sec13FHoldings",
    arrow_schema=pa.schema(
        [
            pa.field("Cik", pa.string()),
            pa.field("FilerName", pa.string()),
            pa.field("PeriodOfReport", DATE32_TYPE),
            pa.field("FilingDate", DATE32_TYPE),
            pa.field("Symbol", pa.string()),
            pa.field("SecurityName", pa.string()),
            pa.field("Cusip", pa.string()),
            pa.field("Value", pa.float64()),
            pa.field("Shares", pa.float64()),
            pa.field("Discretion", pa.string()),
            pa.field("PutCall", pa.string()),
        ]
    ),
    key_cols=("Cik", "Cusip", "PeriodOfReport"),
    normalizer=normalize_sec_13f,
    allow_dynamic_columns=True,
)

SEC_INSIDER_SCHEMA = SchemaSpec(
    name="SecInsiderTransactions",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Cik", pa.string()),
            pa.field("InsiderName", pa.string()),
            pa.field("InsiderTitle", pa.string()),
            pa.field("AccessionNumber", pa.string()),
            pa.field("FormType", pa.string()),
            pa.field("FilingDate", DATE32_TYPE),
            pa.field("TransactionDate", DATE32_TYPE),
            pa.field("TransactionCode", pa.string()),
            pa.field("Shares", pa.float64()),
            pa.field("PricePerShare", pa.float64()),
            pa.field("SharesOwnedAfter", pa.float64()),
        ]
    ),
    key_cols=("AccessionNumber", "InsiderName"),
    normalizer=normalize_sec_insider,
    allow_dynamic_columns=True,
)

NEWS_SCHEMA = SchemaSpec(
    name="News",
    arrow_schema=pa.schema(
        [
            pa.field("ArticleId", pa.string()),
            pa.field("PublishedUtc", UTC_NS_TYPE),
            pa.field("Title", pa.string()),
            pa.field("Author", pa.string()),
            pa.field("PublisherName", pa.string()),
            pa.field("ArticleUrl", pa.string()),
            pa.field("Symbols", STRING_LIST_TYPE),
            pa.field("Description", pa.string()),
            pa.field("Keywords", STRING_LIST_TYPE),
            pa.field("Sentiment", pa.string()),
            pa.field("SentimentReasoning", pa.string()),
        ]
    ),
    key_cols=("ArticleId",),
    normalizer=normalize_news,
)

EXCHANGES_SCHEMA = SchemaSpec(
    name="Exchanges",
    arrow_schema=pa.schema(
        [
            pa.field("ExchangeId", pa.int32()),
            pa.field("Type", pa.string()),
            pa.field("AssetClass", pa.string()),
            pa.field("Locale", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("Acronym", pa.string()),
            pa.field("Mic", pa.string()),
            pa.field("OperatingMic", pa.string()),
            pa.field("ParticipantId", pa.string()),
            pa.field("Url", pa.string()),
        ]
    ),
    key_cols=("ExchangeId",),
    normalizer=normalize_exchange,
)

CONDITIONS_SCHEMA = SchemaSpec(
    name="Conditions",
    arrow_schema=pa.schema(
        [
            pa.field("ConditionId", pa.int32()),
            pa.field("Type", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("AssetClass", pa.string()),
            pa.field("DataTypes", STRING_LIST_TYPE),
            pa.field("Legacy", pa.bool_()),
        ]
    ),
    key_cols=("ConditionId",),
    normalizer=normalize_condition,
)

HOLIDAYS_SCHEMA = SchemaSpec(
    name="MarketHolidays",
    arrow_schema=pa.schema(
        [
            pa.field("Date", DATE32_TYPE),
            pa.field("Exchange", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("Status", pa.string()),
            pa.field("OpenTime", UTC_NS_TYPE),
            pa.field("CloseTime", UTC_NS_TYPE),
        ]
    ),
    key_cols=("Date", "Exchange"),
    normalizer=normalize_holiday,
)

MARKET_STATUS_SCHEMA = SchemaSpec(
    name="MarketStatus",
    arrow_schema=pa.schema(
        [
            pa.field("ServerTime", UTC_NS_TYPE),
            pa.field("Market", pa.string()),
            pa.field("EarlyHours", pa.bool_()),
            pa.field("AfterHours", pa.bool_()),
            pa.field("NyseStatus", pa.string()),
            pa.field("NasdaqStatus", pa.string()),
            pa.field("OtcStatus", pa.string()),
            pa.field("CryptoStatus", pa.string()),
            pa.field("FxStatus", pa.string()),
        ]
    ),
    key_cols=("Market",),
    normalizer=normalize_market_status,
)
