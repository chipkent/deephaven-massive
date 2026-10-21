"""Unified PascalCase PyArrow schemas and normalizers for market data feeds.

This module defines the canonical PyArrow schemas (`SchemaSpec`) and row
normalizers for high-frequency and historical market data across all asset
classes—including trades, NBBO quotes, OHLCV aggregate bars, Limit Up-Limit Down
(LULD) bands, net order imbalances (NOI), Fair Market Value (FMV), index values,
cross-asset snapshots, daily summaries, currency conversions, and technical
indicators.

Every normalizer reconciles structural differences across Massive.com's three
transport surfaces:
* **WebSocket events**: Short single-letter keys (e.g., ``sym``, ``p``, ``s``,
  ``t``, ``bp``, ``ap``).
* **REST API responses**: Descriptive ``snake_case`` or ``camelCase`` keys and
  deeply nested snapshot objects (e.g., ``last_trade``, ``last_quote``,
  ``greeks``).
* **S3 Flat Files**: Tabular CSV headers (e.g., ``sip_timestamp``,
  ``window_start``, ``bid_price``).
"""

from __future__ import annotations

import calendar
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from deephaven_massive.engine.arrow_bridge import (
    DATE32_TYPE,
    INT32_LIST_TYPE,
    UTC_NS_TYPE,
    SchemaSpec,
    to_utc_ns,
)

if TYPE_CHECKING:
    from deephaven_massive.engine.symbols import SymbolNormalizer


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    """Return the first non-``None`` value found in ``row`` across ``keys``.

    Args:
        row: Source dictionary or mapping to inspect.
        *keys: Candidate dictionary keys in descending priority order.

    Returns:
        Any: The value associated with the first matching key whose value is not
        ``None``, or ``None`` if no key matches.
    """
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def _coalesce(*vals: Any) -> Any:
    """Return the first value in ``vals`` that is not ``None``."""
    for v in vals:
        if v is not None:
            return v
    return None


def _submap(row: Mapping[str, Any], *keys: str) -> Mapping[str, Any]:
    """Return the first nested ``Mapping`` found in ``row`` across ``keys``, or ``{}``."""
    val = _first_present(row, *keys)
    return val if isinstance(val, Mapping) else {}


def _norm_symbol(
    row: Mapping[str, Any],
    norm: SymbolNormalizer | None,
    *keys: str,
) -> str | None:
    """Extract and canonicalize a ticker or instrument symbol from ``row``.

    Args:
        row: Source dictionary or mapping to inspect.
        norm: Optional ``SymbolNormalizer`` for stripping wire prefixes and
            formatting canonical symbols.
        *keys: Candidate symbol keys in priority order.

    Returns:
        str | None: Canonical uppercase symbol string, or ``None`` if absent or
        blank.
    """
    raw = _first_present(row, *keys)
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    return norm.canonical(s) if norm is not None else s.upper()


def _normalize_trade(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize a WebSocket, REST, Snapshot, or S3 Flat File trade row.

    Extracts trade execution price, size, exchange ID, condition codes, sequence
    number, tape, and nanosecond SIP/participant/TRF timestamps—automatically
    unwrapping nested ``last_trade`` or ``lastTrade`` sub-objects when invoked
    on a snapshot payload.

    Args:
        row: Raw trade or snapshot dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching ``TRADES_SCHEMA``.
    """
    # Support snapshot rows containing nested `last_trade` or `lastTrade`
    nested_lt = row.get("last_trade") or row.get("lastTrade")
    src = nested_lt if isinstance(nested_lt, Mapping) else row
    sym = _norm_symbol(row, norm, "Symbol", "sym", "pair", "ticker", "T", "S") or _norm_symbol(
        src, norm, "Symbol", "sym", "pair", "ticker", "T", "S"
    )
    return {
        "Symbol": sym,
        "Timestamp": _first_present(
            src, "Timestamp", "sip_timestamp", "t", "timestamp", "participant_timestamp"
        ),
        "ParticipantTimestamp": _first_present(
            src, "ParticipantTimestamp", "participant_timestamp", "y"
        ),
        "TrfTimestamp": _first_present(src, "TrfTimestamp", "trf_timestamp", "trft"),
        "Price": _first_present(src, "Price", "price", "p"),
        "Size": _first_present(src, "Size", "size", "s"),
        "Exchange": _first_present(src, "Exchange", "exchange", "x"),
        "TradeId": _first_present(src, "TradeId", "id", "i"),
        "Conditions": _first_present(src, "Conditions", "conditions", "c"),
        "Correction": _first_present(src, "Correction", "correction", "e"),
        "SequenceNumber": _first_present(src, "SequenceNumber", "sequence_number", "q"),
        "TrfId": _first_present(src, "TrfId", "trf_id", "trfi"),
        "Tape": _first_present(src, "Tape", "tape", "z"),
    }


def _normalize_quote(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize a WebSocket, REST, Snapshot, or S3 Flat File quote row.

    Extracts top-of-book bid/ask prices, sizes, exchange IDs, condition and
    indicator arrays, sequence numbers, and timestamps across equities, options,
    futures, forex (where ``p`` carries the currency pair on WebSocket frames),
    and crypto.

    Args:
        row: Raw quote or snapshot dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching ``QUOTES_SCHEMA``.
    """
    nested_lq = row.get("last_quote") or row.get("lastQuote") or row.get("last")
    src = nested_lq if isinstance(nested_lq, Mapping) else row
    # Note: In Forex WS (`C.*`), `p` is the currency pair string (e.g. "EUR/USD")
    sym = _norm_symbol(row, norm, "Symbol", "sym", "pair", "ticker", "T", "S")
    if sym is None and isinstance(row.get("p"), str):
        sym = _norm_symbol(row, norm, "p")
    if sym is None:
        sym = _norm_symbol(src, norm, "Symbol", "sym", "pair", "ticker")
    return {
        "Symbol": sym,
        "Timestamp": _first_present(
            src,
            "Timestamp",
            "sip_timestamp",
            "t",
            "timestamp",
            "last_updated",
            "participant_timestamp",
        ),
        "ParticipantTimestamp": _first_present(
            src, "ParticipantTimestamp", "participant_timestamp", "y", "exchange_timestamp"
        ),
        "BidPrice": _coalesce(
            _first_present(src, "BidPrice", "bid_price", "bp", "b", "bid"),
            src.get("p") if not isinstance(src.get("p"), str) else None,
        ),
        "BidSize": _first_present(src, "BidSize", "bid_size", "bs", "s", "bidSize"),
        "BidExchange": _first_present(src, "BidExchange", "bid_exchange", "bx", "x"),
        "AskPrice": _first_present(src, "AskPrice", "ask_price", "ap", "P", "a", "ask"),
        "AskSize": _first_present(src, "AskSize", "ask_size", "as", "S", "askSize"),
        "AskExchange": _first_present(src, "AskExchange", "ask_exchange", "ax"),
        "Conditions": _first_present(src, "Conditions", "conditions", "c"),
        "Indicators": _first_present(src, "Indicators", "indicators", "i"),
        "SequenceNumber": _first_present(src, "SequenceNumber", "sequence_number", "q"),
        "Tape": _first_present(src, "Tape", "tape", "z"),
    }


_TIMESPAN_NS: dict[str, int] = {
    "second": 1_000_000_000,
    "sec": 1_000_000_000,
    "s": 1_000_000_000,
    "minute": 60_000_000_000,
    "min": 60_000_000_000,
    "m": 60_000_000_000,
    "hour": 3_600_000_000_000,
    "hr": 3_600_000_000_000,
    "h": 3_600_000_000_000,
    "day": 86_400_000_000_000,
    "d": 86_400_000_000_000,
    "session": 86_400_000_000_000,
    "week": 7 * 86_400_000_000_000,
    "w": 7 * 86_400_000_000_000,
    "month": 30 * 86_400_000_000_000,
    "quarter": 90 * 86_400_000_000_000,
    "year": 365 * 86_400_000_000_000,
}


_CALENDAR_MONTH_FACTORS: dict[str, int] = {
    "month": 1,
    "quarter": 3,
    "year": 12,
}


def _add_calendar_months_ns(start_ns: int, months: int) -> int:
    """Advance a UTC nanosecond epoch timestamp by ``months`` calendar months."""
    sec, rem_ns = divmod(start_ns, 1_000_000_000)
    dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    total_months = (dt.year * 12 + (dt.month - 1)) + months
    target_year, target_month_zero = divmod(total_months, 12)
    target_month = target_month_zero + 1
    max_day = calendar.monthrange(target_year, target_month)[1]
    target_dt = dt.replace(
        year=target_year,
        month=target_month,
        day=min(dt.day, max_day),
    )
    return int(target_dt.timestamp()) * 1_000_000_000 + rem_ns


def _compute_bar_span_ns(multiplier: int = 1, timespan: str = "minute") -> int:
    """Compute the duration of an aggregate bar in nanoseconds.

    Args:
        multiplier: Number of base ``timespan`` units per bar (e.g., ``5`` for
            5-minute bars).
        timespan: Base interval unit name (e.g., ``"second"``, ``"minute"``,
            ``"hour"``, ``"day"``, ``"week"``, ``"month"``, ``"quarter"``,
            ``"year"``). Unknown values default to 1 minute.

    Returns:
        int: Total bar duration in nanoseconds.
    """
    unit_ns = _TIMESPAN_NS.get(timespan.strip().lower(), 60_000_000_000)
    return int(multiplier) * unit_ns


def _normalize_bar(row: Mapping[str, Any], norm: SymbolNormalizer | None = None) -> dict[str, Any]:
    """Normalize a WebSocket, REST aggregate, Snapshot, or S3 Flat File OHLCV bar row.

    Unifies bar start and end timestamps (`Timestamp` and `EndTimestamp`),
    synthesizing `EndTimestamp` from `_span_ns` or `(multiplier, timespan)` when
    REST or S3 payloads provide only the window start time. Also unwraps nested
    ``min``, ``day``, or ``session`` bar sub-objects from snapshot payloads.

    Args:
        row: Raw aggregate bar or snapshot dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching ``BARS_SCHEMA``.
    """
    span_ns = _first_present(row, "_span_ns", "span_ns")
    if span_ns is not None and int(span_ns) >= 86_400_000_000_000:
        nested_bar = row.get("day") or row.get("session") or row.get("min")
    else:
        nested_bar = row.get("min") or row.get("day") or row.get("session")
    src = (
        nested_bar
        if isinstance(nested_bar, Mapping) and "o" not in row and "open" not in row
        else row
    )
    sym = _norm_symbol(row, norm, "Symbol", "sym", "pair", "ticker", "T")
    start_raw = _first_present(
        src, "WindowStart", "Timestamp", "window_start", "t", "s", "timestamp", "date"
    )
    end_raw = _first_present(src, "EndTimestamp", "e", "end_timestamp")
    start_ns = to_utc_ns(start_raw)
    if end_raw is not None:
        end_ns = to_utc_ns(end_raw)
    elif start_ns is not None:
        raw_timespan = str(row.get("timespan", "")).strip().lower()
        month_factor = _CALENDAR_MONTH_FACTORS.get(raw_timespan)
        if month_factor is not None:
            mult = int(row.get("multiplier", 1))
            end_ns = _add_calendar_months_ns(start_ns, mult * month_factor)
        else:
            if span_ns is None and ("timespan" in row or "multiplier" in row):
                span_ns = _compute_bar_span_ns(
                    int(row.get("multiplier", 1)),
                    raw_timespan or "minute",
                )
            end_ns = start_ns + int(span_ns) if span_ns is not None else start_ns
    else:
        end_ns = None
    return {
        "Symbol": sym,
        "Timestamp": start_ns,
        "EndTimestamp": end_ns,
        "Open": _first_present(src, "Open", "o", "open"),
        "High": _first_present(src, "High", "h", "high"),
        "Low": _first_present(src, "Low", "l", "low"),
        "Close": _first_present(src, "Close", "c", "close"),
        "Volume": _first_present(src, "Volume", "v", "volume"),
        "Vwap": _first_present(src, "Vwap", "vw", "vwap"),
        "AccumulatedVolume": _first_present(src, "AccumulatedVolume", "av", "accumulated_volume"),
        "OfficialOpen": _first_present(src, "OfficialOpen", "op", "official_open"),
        "Transactions": _first_present(src, "Transactions", "n", "transactions"),
        "IsOtc": _first_present(src, "IsOtc", "otc", "is_otc"),
    }


def _normalize_luld(row: Mapping[str, Any], norm: SymbolNormalizer | None = None) -> dict[str, Any]:
    """Normalize a Limit Up-Limit Down (``LULD.*``) price band event.

    Args:
        row: Raw LULD WebSocket event dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching ``LULD_SCHEMA``.
    """
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "T", "sym", "ticker"),
        "Timestamp": _first_present(row, "Timestamp", "t", "timestamp"),
        "HighPrice": _first_present(row, "HighPrice", "h", "high_price"),
        "LowPrice": _first_present(row, "LowPrice", "l", "low_price"),
        "Indicators": _first_present(row, "Indicators", "i", "indicators"),
        "SequenceNumber": _first_present(row, "SequenceNumber", "q", "sequence_number"),
        "Tape": _first_present(row, "Tape", "z", "tape"),
    }


def _normalize_imbalance(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize a Net Order Imbalance (``NOI.*``) auction event.

    Args:
        row: Raw NOI WebSocket event dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching ``IMBALANCES_SCHEMA``.
    """
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "T", "sym", "ticker"),
        "Timestamp": _first_present(row, "Timestamp", "t", "timestamp"),
        "AuctionTime": _first_present(row, "AuctionTime", "at", "auction_time"),
        "AuctionType": _first_present(row, "AuctionType", "a", "auction_type"),
        "PairedShares": _first_present(row, "PairedShares", "p", "paired_shares"),
        "ImbalanceShares": _first_present(row, "ImbalanceShares", "i", "imbalance_shares"),
        "ReferencePrice": _first_present(row, "ReferencePrice", "b", "reference_price"),
        "IndicativeMatchPrice": _first_present(
            row, "IndicativeMatchPrice", "im", "indicative_match_price"
        ),
        "UpperCollar": _first_present(row, "UpperCollar", "uc", "upper_collar"),
        "LowerCollar": _first_present(row, "LowerCollar", "lc", "lower_collar"),
        "Exchange": _first_present(row, "Exchange", "x", "exchange"),
        "SequenceNumber": _first_present(row, "SequenceNumber", "q", "sequence_number"),
    }


def _normalize_fmv(row: Mapping[str, Any], norm: SymbolNormalizer | None = None) -> dict[str, Any]:
    """Normalize a Fair Market Value (``FMV.*``) real-time pricing event.

    Args:
        row: Raw FMV WebSocket event dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching ``FMV_SCHEMA``.
    """
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "sym", "ticker", "T"),
        "Timestamp": _first_present(row, "Timestamp", "t", "timestamp"),
        "FairMarketValue": _first_present(row, "FairMarketValue", "fmv", "val"),
    }


def _normalize_index_value(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize an Index Value (``V.I:*``, ``/v3/snapshot/indices``, or S3 ``values_v1``) row.

    Args:
        row: Raw index value event, snapshot, or CSV row dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching
        ``INDEX_VALUES_SCHEMA``.
    """
    session = _submap(row, "session")
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "T", "sym", "ticker"),
        "Timestamp": _first_present(
            row,
            "Timestamp",
            "t",
            "timestamp",
            "last_updated",
            "lastUpdated",
            "updated",
        )
        or _first_present(session, "last_updated", "lastUpdated", "updated"),
        "Value": _first_present(row, "Value", "val", "value", "c")
        or _first_present(session, "close", "price"),
    }


def _normalize_stock_snapshot(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Flatten a nested Stock, Forex, Crypto, or Universal Snapshot into PascalCase columns.

    Unpacks nested ``day``, ``prevDay``, ``lastTrade``, ``lastQuote``, ``min``,
    and ``session`` dictionaries from REST snapshot payloads into a single flat
    row suitable for tabular analysis in Deephaven.

    Args:
        row: Raw nested snapshot dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching
        ``STOCK_SNAPSHOT_SCHEMA``.
    """
    day = _submap(row, "day")
    prev_map = _submap(row, "prevDay", "prev_day")
    lt_map = _submap(row, "lastTrade", "last_trade")
    lq_map = _submap(row, "lastQuote", "last_quote")
    min_bar = _submap(row, "min")
    sess = _submap(row, "session")

    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker", "sym", "T"),
        "Timestamp": _coalesce(
            _first_present(row, "Timestamp", "updated", "last_updated"),
            _first_present(lt_map, "t", "sip_timestamp"),
            _first_present(lq_map, "t", "sip_timestamp"),
        ),
        "DayOpen": _coalesce(
            _first_present(row, "DayOpen"),
            _first_present(day, "o", "open"),
            _first_present(sess, "open"),
        ),
        "DayHigh": _coalesce(
            _first_present(row, "DayHigh"),
            _first_present(day, "h", "high"),
            _first_present(sess, "high"),
        ),
        "DayLow": _coalesce(
            _first_present(row, "DayLow"),
            _first_present(day, "l", "low"),
            _first_present(sess, "low"),
        ),
        "DayClose": _coalesce(
            _first_present(row, "DayClose"),
            _first_present(day, "c", "close"),
            _first_present(sess, "close"),
            _first_present(row, "value"),
        ),
        "DayVolume": _coalesce(
            _first_present(row, "DayVolume"),
            _first_present(day, "v", "volume"),
            _first_present(sess, "volume"),
        ),
        "DayVwap": _coalesce(
            _first_present(row, "DayVwap"),
            _first_present(day, "vw", "vwap"),
        ),
        "PrevDayClose": _coalesce(
            _first_present(row, "PrevDayClose"),
            _first_present(prev_map, "c", "close"),
            _first_present(sess, "previous_close"),
        ),
        "PrevDayVolume": _coalesce(
            _first_present(row, "PrevDayVolume"),
            _first_present(prev_map, "v", "volume"),
        ),
        "Change": _coalesce(
            _first_present(row, "Change", "todaysChange", "todays_change"),
            _first_present(sess, "change"),
        ),
        "ChangePercent": _coalesce(
            _first_present(row, "ChangePercent", "todaysChangePerc", "todays_change_perc"),
            _first_present(sess, "change_percent"),
        ),
        "LastTradePrice": _coalesce(
            _first_present(row, "LastTradePrice"),
            _first_present(lt_map, "p", "price"),
        ),
        "LastTradeSize": _coalesce(
            _first_present(row, "LastTradeSize"),
            _first_present(lt_map, "s", "size"),
        ),
        "LastTradeTimestamp": _coalesce(
            _first_present(row, "LastTradeTimestamp"),
            _first_present(lt_map, "t", "sip_timestamp", "participant_timestamp"),
        ),
        "BidPrice": _coalesce(
            _first_present(row, "BidPrice"),
            _first_present(lq_map, "p", "bp", "b", "bid", "bid_price"),
        ),
        "BidSize": _coalesce(
            _first_present(row, "BidSize"),
            _first_present(lq_map, "s", "bs", "bid_size"),
        ),
        "AskPrice": _coalesce(
            _first_present(row, "AskPrice"),
            _first_present(lq_map, "P", "ap", "a", "ask", "ask_price"),
        ),
        "AskSize": _coalesce(
            _first_present(row, "AskSize"),
            _first_present(lq_map, "S", "as", "ask_size"),
        ),
        "QuoteTimestamp": _coalesce(
            _first_present(row, "QuoteTimestamp"),
            _first_present(lq_map, "t", "sip_timestamp", "last_updated"),
        ),
        "MinOpen": _coalesce(_first_present(row, "MinOpen"), _first_present(min_bar, "o", "open")),
        "MinHigh": _coalesce(_first_present(row, "MinHigh"), _first_present(min_bar, "h", "high")),
        "MinLow": _coalesce(_first_present(row, "MinLow"), _first_present(min_bar, "l", "low")),
        "MinClose": _coalesce(
            _first_present(row, "MinClose"), _first_present(min_bar, "c", "close")
        ),
        "MinVolume": _coalesce(
            _first_present(row, "MinVolume"), _first_present(min_bar, "v", "volume")
        ),
        "MinVwap": _coalesce(_first_present(row, "MinVwap"), _first_present(min_bar, "vw", "vwap")),
        "FairMarketValue": _first_present(row, "FairMarketValue", "fmv"),
    }


def _normalize_option_chain_snapshot(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Flatten an Option Chain or Option Contract Snapshot into PascalCase columns.

    Unpacks nested contract ``details``, option ``greeks`` (Delta, Gamma, Theta,
    Vega), ``underlying_asset`` metrics, ``last_quote`` NBBO/midpoint,
    ``last_trade``, and ``day`` OHLCV into ``OPTION_CHAIN_SCHEMA``.

    Args:
        row: Raw option contract snapshot dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching
        ``OPTION_CHAIN_SCHEMA``.
    """
    details = _submap(row, "details")
    greeks = _submap(row, "greeks")
    underlying = _submap(row, "underlying_asset")
    day = _submap(row, "day")
    lq = _submap(row, "last_quote")
    lt = _submap(row, "last_trade")

    raw_sym = _coalesce(_first_present(row, "Symbol", "ticker"), _first_present(details, "ticker"))
    sym = (
        norm.canonical(str(raw_sym)) if (raw_sym and norm) else (str(raw_sym) if raw_sym else None)
    )

    return {
        "Symbol": sym,
        "UnderlyingSymbol": _coalesce(
            _first_present(row, "UnderlyingSymbol"),
            _first_present(underlying, "ticker"),
        ),
        "UnderlyingPrice": _coalesce(
            _first_present(row, "UnderlyingPrice"),
            _first_present(underlying, "price", "value"),
        ),
        "ExpirationDate": _coalesce(
            _first_present(row, "ExpirationDate"),
            _first_present(details, "expiration_date"),
        ),
        "StrikePrice": _coalesce(
            _first_present(row, "StrikePrice"),
            _first_present(details, "strike_price"),
        ),
        "ContractType": _coalesce(
            _first_present(row, "ContractType"),
            _first_present(details, "contract_type"),
        ),
        "ExerciseStyle": _coalesce(
            _first_present(row, "ExerciseStyle"),
            _first_present(details, "exercise_style"),
        ),
        "SharesPerContract": _coalesce(
            _first_present(row, "SharesPerContract"),
            _first_present(details, "shares_per_contract"),
        ),
        "BreakEvenPrice": _first_present(row, "BreakEvenPrice", "break_even_price"),
        "ImpliedVolatility": _first_present(row, "ImpliedVolatility", "implied_volatility"),
        "OpenInterest": _first_present(row, "OpenInterest", "open_interest"),
        "Delta": _coalesce(_first_present(row, "Delta"), _first_present(greeks, "delta")),
        "Gamma": _coalesce(_first_present(row, "Gamma"), _first_present(greeks, "gamma")),
        "Theta": _coalesce(_first_present(row, "Theta"), _first_present(greeks, "theta")),
        "Vega": _coalesce(_first_present(row, "Vega"), _first_present(greeks, "vega")),
        "BidPrice": _coalesce(
            _first_present(row, "BidPrice"), _first_present(lq, "bid", "bid_price")
        ),
        "BidSize": _coalesce(_first_present(row, "BidSize"), _first_present(lq, "bid_size")),
        "AskPrice": _coalesce(
            _first_present(row, "AskPrice"), _first_present(lq, "ask", "ask_price")
        ),
        "AskSize": _coalesce(_first_present(row, "AskSize"), _first_present(lq, "ask_size")),
        "Midpoint": _coalesce(_first_present(row, "Midpoint"), _first_present(lq, "midpoint")),
        "QuoteTimestamp": _coalesce(
            _first_present(row, "QuoteTimestamp"),
            _first_present(lq, "last_updated", "sip_timestamp"),
        ),
        "LastTradePrice": _coalesce(
            _first_present(row, "LastTradePrice"), _first_present(lt, "price")
        ),
        "LastTradeSize": _coalesce(
            _first_present(row, "LastTradeSize"), _first_present(lt, "size")
        ),
        "LastTradeTimestamp": _coalesce(
            _first_present(row, "LastTradeTimestamp"),
            _first_present(lt, "sip_timestamp"),
        ),
        "DayOpen": _coalesce(_first_present(row, "DayOpen"), _first_present(day, "open", "o")),
        "DayHigh": _coalesce(_first_present(row, "DayHigh"), _first_present(day, "high", "h")),
        "DayLow": _coalesce(_first_present(row, "DayLow"), _first_present(day, "low", "l")),
        "DayClose": _coalesce(_first_present(row, "DayClose"), _first_present(day, "close", "c")),
        "DayVolume": _coalesce(
            _first_present(row, "DayVolume"), _first_present(day, "volume", "v")
        ),
        "DayVwap": _coalesce(_first_present(row, "DayVwap"), _first_present(day, "vwap", "vw")),
        "PrevDayClose": _coalesce(
            _first_present(row, "PrevDayClose"),
            _first_present(day, "previous_close"),
        ),
        "Change": _coalesce(_first_present(row, "Change"), _first_present(day, "change")),
        "ChangePercent": _coalesce(
            _first_present(row, "ChangePercent"),
            _first_present(day, "change_percent"),
        ),
        "FairMarketValue": _first_present(row, "FairMarketValue", "fmv"),
    }


def _normalize_futures_snapshot(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Flatten a Futures Snapshot into PascalCase columns.

    Unpacks nested ``details``, ``last_trade``, ``last_quote``, ``session``, and
    ``last_minute`` sub-objects into a flat dictionary aligned with
    ``FUTURES_SNAPSHOT_SCHEMA``.

    Args:
        row: Raw futures contract snapshot dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching
        ``FUTURES_SNAPSHOT_SCHEMA``.
    """
    details = _submap(row, "details")
    lt = _submap(row, "last_trade")
    lq = _submap(row, "last_quote")
    sess = _submap(row, "session")
    min_bar = _submap(row, "last_minute")

    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker"),
        "ProductCode": _coalesce(
            _first_present(row, "ProductCode", "product_code"),
            _first_present(details, "product_code"),
        ),
        "Name": _coalesce(_first_present(row, "Name", "name"), _first_present(details, "name")),
        "Timestamp": _coalesce(
            _first_present(row, "Timestamp", "last_updated"),
            _first_present(lt, "timestamp"),
            _first_present(lq, "timestamp"),
        ),
        "LastTradePrice": _coalesce(
            _first_present(row, "LastTradePrice"), _first_present(lt, "price")
        ),
        "LastTradeSize": _coalesce(
            _first_present(row, "LastTradeSize"), _first_present(lt, "size")
        ),
        "BidPrice": _coalesce(
            _first_present(row, "BidPrice"), _first_present(lq, "bid", "bid_price")
        ),
        "BidSize": _coalesce(_first_present(row, "BidSize"), _first_present(lq, "bid_size")),
        "AskPrice": _coalesce(
            _first_present(row, "AskPrice"), _first_present(lq, "ask", "ask_price")
        ),
        "AskSize": _coalesce(_first_present(row, "AskSize"), _first_present(lq, "ask_size")),
        "SessionOpen": _coalesce(
            _first_present(row, "SessionOpen"),
            _first_present(sess, "open"),
            _first_present(min_bar, "open"),
        ),
        "SessionHigh": _coalesce(
            _first_present(row, "SessionHigh"),
            _first_present(sess, "high"),
            _first_present(min_bar, "high"),
        ),
        "SessionLow": _coalesce(
            _first_present(row, "SessionLow"),
            _first_present(sess, "low"),
            _first_present(min_bar, "low"),
        ),
        "SessionClose": _coalesce(
            _first_present(row, "SessionClose"),
            _first_present(sess, "close"),
            _first_present(min_bar, "close"),
        ),
        "SessionVolume": _coalesce(
            _first_present(row, "SessionVolume"),
            _first_present(sess, "volume"),
            _first_present(min_bar, "volume"),
        ),
        "SettlementPrice": _coalesce(
            _first_present(row, "SettlementPrice"),
            _first_present(sess, "settlement_price"),
        ),
        "PreviousSettlement": _coalesce(
            _first_present(row, "PreviousSettlement"),
            _first_present(sess, "previous_settlement"),
        ),
        "Change": _coalesce(_first_present(row, "Change"), _first_present(sess, "change")),
        "ChangePercent": _coalesce(
            _first_present(row, "ChangePercent"),
            _first_present(sess, "change_percent"),
        ),
        "OpenInterest": _coalesce(
            _first_present(row, "OpenInterest"),
            _first_present(details, "open_interest"),
        ),
    }


def _normalize_daily_summary(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize a grouped daily or single-ticker open/close summary row.

    Args:
        row: Raw grouped daily bar or daily open/close summary dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching
        ``DAILY_SUMMARY_SCHEMA``.
    """
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "T", "symbol", "ticker"),
        "Date": _first_present(row, "Date", "from", "date", "t"),
        "Open": _first_present(row, "Open", "open", "o"),
        "High": _first_present(row, "High", "high", "h"),
        "Low": _first_present(row, "Low", "low", "l"),
        "Close": _first_present(row, "Close", "close", "c"),
        "Volume": _first_present(row, "Volume", "volume", "v"),
        "Vwap": _first_present(row, "Vwap", "vwap", "vw"),
        "PreMarket": _first_present(row, "PreMarket", "preMarket", "pre_market"),
        "AfterHours": _first_present(row, "AfterHours", "afterHours", "after_hours"),
        "Transactions": _first_present(row, "Transactions", "n", "transactions"),
        "IsOtc": _first_present(row, "IsOtc", "otc"),
    }


def _normalize_currency_conversion(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize a ``/v1/conversion/{from}/{to}`` real-time FX conversion response.

    Args:
        row: Raw currency conversion response dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching
        ``CURRENCY_CONVERSION_SCHEMA``.
    """
    last_q = _submap(row, "last")
    from_curr = _first_present(row, "FromCurrency", "from")
    to_curr = _first_present(row, "ToCurrency", "to")
    raw_sym = _first_present(row, "Symbol", "symbol") or (
        f"{from_curr}/{to_curr}" if from_curr and to_curr else None
    )
    sym = norm.canonical(str(raw_sym)) if (raw_sym and norm) else raw_sym
    return {
        "Symbol": sym,
        "Timestamp": _coalesce(
            _first_present(row, "Timestamp"), _first_present(last_q, "timestamp")
        ),
        "FromCurrency": from_curr,
        "ToCurrency": to_curr,
        "InitialAmount": _first_present(row, "InitialAmount", "initialAmount", "initial_amount"),
        "ConvertedAmount": _first_present(row, "ConvertedAmount", "converted"),
        "BidPrice": _coalesce(_first_present(row, "BidPrice"), _first_present(last_q, "bid")),
        "AskPrice": _coalesce(_first_present(row, "AskPrice"), _first_present(last_q, "ask")),
    }


def _normalize_indicator(
    row: Mapping[str, Any], norm: SymbolNormalizer | None = None
) -> dict[str, Any]:
    """Normalize an SMA, EMA, MACD, or RSI technical indicator value row.

    Args:
        row: Raw technical indicator value dictionary.
        norm: Optional symbol normalizer used to canonicalize ``Symbol``.

    Returns:
        dict[str, Any]: Normalized row dictionary matching ``INDICATOR_SCHEMA``.
    """
    return {
        "Symbol": _norm_symbol(row, norm, "Symbol", "ticker", "sym"),
        "Timestamp": _first_present(row, "Timestamp", "timestamp", "t"),
        "Value": _first_present(row, "Value", "value"),
        "Signal": _first_present(row, "Signal", "signal"),
        "Histogram": _first_present(row, "Histogram", "histogram"),
    }


TRADES_SCHEMA = SchemaSpec(
    name="Trades",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("ParticipantTimestamp", UTC_NS_TYPE),
            pa.field("TrfTimestamp", UTC_NS_TYPE),
            pa.field("Price", pa.float64()),
            pa.field("Size", pa.float64()),
            pa.field("Exchange", pa.int32()),
            pa.field("TradeId", pa.string()),
            pa.field("Conditions", INT32_LIST_TYPE),
            pa.field("Correction", pa.int32()),
            pa.field("SequenceNumber", pa.int64()),
            pa.field("TrfId", pa.int32()),
            pa.field("Tape", pa.int32()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_trade,
)

QUOTES_SCHEMA = SchemaSpec(
    name="Quotes",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("ParticipantTimestamp", UTC_NS_TYPE),
            pa.field("BidPrice", pa.float64()),
            pa.field("BidSize", pa.float64()),
            pa.field("BidExchange", pa.int32()),
            pa.field("AskPrice", pa.float64()),
            pa.field("AskSize", pa.float64()),
            pa.field("AskExchange", pa.int32()),
            pa.field("Conditions", INT32_LIST_TYPE),
            pa.field("Indicators", INT32_LIST_TYPE),
            pa.field("SequenceNumber", pa.int64()),
            pa.field("Tape", pa.int32()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_quote,
)

BARS_SCHEMA = SchemaSpec(
    name="Bars",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("EndTimestamp", UTC_NS_TYPE),
            pa.field("Open", pa.float64()),
            pa.field("High", pa.float64()),
            pa.field("Low", pa.float64()),
            pa.field("Close", pa.float64()),
            pa.field("Volume", pa.float64()),
            pa.field("Vwap", pa.float64()),
            pa.field("AccumulatedVolume", pa.float64()),
            pa.field("OfficialOpen", pa.float64()),
            pa.field("Transactions", pa.int64()),
            pa.field("IsOtc", pa.bool_()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_bar,
)

LULD_SCHEMA = SchemaSpec(
    name="Luld",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("HighPrice", pa.float64()),
            pa.field("LowPrice", pa.float64()),
            pa.field("Indicators", INT32_LIST_TYPE),
            pa.field("SequenceNumber", pa.int64()),
            pa.field("Tape", pa.int32()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_luld,
)

IMBALANCES_SCHEMA = SchemaSpec(
    name="Imbalances",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("AuctionTime", pa.int32()),
            pa.field("AuctionType", pa.string()),
            pa.field("PairedShares", pa.float64()),
            pa.field("ImbalanceShares", pa.float64()),
            pa.field("ReferencePrice", pa.float64()),
            pa.field("IndicativeMatchPrice", pa.float64()),
            pa.field("UpperCollar", pa.float64()),
            pa.field("LowerCollar", pa.float64()),
            pa.field("Exchange", pa.int32()),
            pa.field("SequenceNumber", pa.int64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_imbalance,
)

FMV_SCHEMA = SchemaSpec(
    name="Fmv",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("FairMarketValue", pa.float64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_fmv,
)

INDEX_VALUES_SCHEMA = SchemaSpec(
    name="IndexValues",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("Value", pa.float64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_index_value,
)

STOCK_SNAPSHOT_SCHEMA = SchemaSpec(
    name="StockSnapshots",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("DayOpen", pa.float64()),
            pa.field("DayHigh", pa.float64()),
            pa.field("DayLow", pa.float64()),
            pa.field("DayClose", pa.float64()),
            pa.field("DayVolume", pa.float64()),
            pa.field("DayVwap", pa.float64()),
            pa.field("PrevDayClose", pa.float64()),
            pa.field("PrevDayVolume", pa.float64()),
            pa.field("Change", pa.float64()),
            pa.field("ChangePercent", pa.float64()),
            pa.field("LastTradePrice", pa.float64()),
            pa.field("LastTradeSize", pa.float64()),
            pa.field("LastTradeTimestamp", UTC_NS_TYPE),
            pa.field("BidPrice", pa.float64()),
            pa.field("BidSize", pa.float64()),
            pa.field("AskPrice", pa.float64()),
            pa.field("AskSize", pa.float64()),
            pa.field("QuoteTimestamp", UTC_NS_TYPE),
            pa.field("MinOpen", pa.float64()),
            pa.field("MinHigh", pa.float64()),
            pa.field("MinLow", pa.float64()),
            pa.field("MinClose", pa.float64()),
            pa.field("MinVolume", pa.float64()),
            pa.field("MinVwap", pa.float64()),
            pa.field("FairMarketValue", pa.float64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_stock_snapshot,
)

OPTION_CHAIN_SCHEMA = SchemaSpec(
    name="OptionChainSnapshot",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("UnderlyingSymbol", pa.string()),
            pa.field("UnderlyingPrice", pa.float64()),
            pa.field("ExpirationDate", DATE32_TYPE),
            pa.field("StrikePrice", pa.float64()),
            pa.field("ContractType", pa.string()),
            pa.field("ExerciseStyle", pa.string()),
            pa.field("SharesPerContract", pa.float64()),
            pa.field("BreakEvenPrice", pa.float64()),
            pa.field("ImpliedVolatility", pa.float64()),
            pa.field("OpenInterest", pa.float64()),
            pa.field("Delta", pa.float64()),
            pa.field("Gamma", pa.float64()),
            pa.field("Theta", pa.float64()),
            pa.field("Vega", pa.float64()),
            pa.field("BidPrice", pa.float64()),
            pa.field("BidSize", pa.float64()),
            pa.field("AskPrice", pa.float64()),
            pa.field("AskSize", pa.float64()),
            pa.field("Midpoint", pa.float64()),
            pa.field("QuoteTimestamp", UTC_NS_TYPE),
            pa.field("LastTradePrice", pa.float64()),
            pa.field("LastTradeSize", pa.float64()),
            pa.field("LastTradeTimestamp", UTC_NS_TYPE),
            pa.field("DayOpen", pa.float64()),
            pa.field("DayHigh", pa.float64()),
            pa.field("DayLow", pa.float64()),
            pa.field("DayClose", pa.float64()),
            pa.field("DayVolume", pa.float64()),
            pa.field("DayVwap", pa.float64()),
            pa.field("PrevDayClose", pa.float64()),
            pa.field("Change", pa.float64()),
            pa.field("ChangePercent", pa.float64()),
            pa.field("FairMarketValue", pa.float64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_option_chain_snapshot,
)

FUTURES_SNAPSHOT_SCHEMA = SchemaSpec(
    name="FuturesSnapshots",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("ProductCode", pa.string()),
            pa.field("Name", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("LastTradePrice", pa.float64()),
            pa.field("LastTradeSize", pa.float64()),
            pa.field("BidPrice", pa.float64()),
            pa.field("BidSize", pa.float64()),
            pa.field("AskPrice", pa.float64()),
            pa.field("AskSize", pa.float64()),
            pa.field("SessionOpen", pa.float64()),
            pa.field("SessionHigh", pa.float64()),
            pa.field("SessionLow", pa.float64()),
            pa.field("SessionClose", pa.float64()),
            pa.field("SessionVolume", pa.float64()),
            pa.field("SettlementPrice", pa.float64()),
            pa.field("PreviousSettlement", pa.float64()),
            pa.field("Change", pa.float64()),
            pa.field("ChangePercent", pa.float64()),
            pa.field("OpenInterest", pa.float64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_futures_snapshot,
)

DAILY_SUMMARY_SCHEMA = SchemaSpec(
    name="DailySummary",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Date", DATE32_TYPE),
            pa.field("Open", pa.float64()),
            pa.field("High", pa.float64()),
            pa.field("Low", pa.float64()),
            pa.field("Close", pa.float64()),
            pa.field("Volume", pa.float64()),
            pa.field("Vwap", pa.float64()),
            pa.field("PreMarket", pa.float64()),
            pa.field("AfterHours", pa.float64()),
            pa.field("Transactions", pa.int64()),
            pa.field("IsOtc", pa.bool_()),
        ]
    ),
    key_cols=("Symbol", "Date"),
    normalizer=_normalize_daily_summary,
)

CURRENCY_CONVERSION_SCHEMA = SchemaSpec(
    name="CurrencyConversion",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("FromCurrency", pa.string()),
            pa.field("ToCurrency", pa.string()),
            pa.field("InitialAmount", pa.float64()),
            pa.field("ConvertedAmount", pa.float64()),
            pa.field("BidPrice", pa.float64()),
            pa.field("AskPrice", pa.float64()),
        ]
    ),
    key_cols=("Symbol",),
    normalizer=_normalize_currency_conversion,
)

INDICATOR_SCHEMA = SchemaSpec(
    name="TechnicalIndicator",
    arrow_schema=pa.schema(
        [
            pa.field("Symbol", pa.string()),
            pa.field("Timestamp", UTC_NS_TYPE),
            pa.field("Value", pa.float64()),
            pa.field("Signal", pa.float64()),
            pa.field("Histogram", pa.float64()),
        ]
    ),
    key_cols=("Symbol", "Timestamp"),
    normalizer=_normalize_indicator,
)
