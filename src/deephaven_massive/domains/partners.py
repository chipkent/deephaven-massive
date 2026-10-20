"""Partner & Alternative Data domain clients (`m.partners.benzinga`, `m.partners.etf_global`, `m.partners.tmx`)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import BaseDomainClient, format_date_param
from deephaven_massive.engine.arrow_bridge import SchemaSpec
from deephaven_massive.engine.builder import FeedBuilder
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
    TMX_CORPORATE_EVENTS_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession


class _PartnerSubClient(BaseDomainClient):
    """Base helper for partner sub-clients supporting `.snapshot()`, `.poll()`, and `.historical()`."""

    def __init__(self, session: MassiveSession) -> None:
        super().__init__(session, domain="stocks")

    def _partner_feed(
        self,
        name: str,
        path: str,
        schema_spec: SchemaSpec,
        params: Mapping[str, Any] | None = None,
        *,
        date_param: str = "date",
    ) -> FeedBuilder:
        base_params = {k: v for k, v in (params or {}).items() if v is not None}

        def _hist(
            start: Any = None,
            end: Any = None,
            limit: int | None = None,
            source: str = "auto",
        ) -> Table:
            q = dict(base_params)
            q[f"{date_param}.gte"] = format_date_param(start)
            q[f"{date_param}.lte"] = format_date_param(end)
            return self.fetch_chunked_table(path, schema_spec, q, limit=limit)

        return self.build_feed(
            name=name,
            schema_spec=schema_spec,
            snapshot_fn=lambda: self.fetch_rest_rows(path, base_params),
            historical_fn=_hist,
        )


class BenzingaClient(_PartnerSubClient):
    """Benzinga analyst ratings, earnings, guidance, consensus, news, and call transcripts (`m.partners.benzinga`)."""

    def ratings(self, symbol: str | None = None) -> FeedBuilder:
        """Analyst upgrades, downgrades, and price target actions (`/benzinga/v1/ratings`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "benzinga_ratings",
            "/benzinga/v1/ratings",
            BENZINGA_RATINGS_SCHEMA,
            {"ticker": canon},
        )

    def earnings(self, symbol: str | None = None) -> FeedBuilder:
        """Historical and upcoming EPS & revenue actuals vs estimates (`/benzinga/v1/earnings`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "benzinga_earnings",
            "/benzinga/v1/earnings",
            BENZINGA_EARNINGS_SCHEMA,
            {"ticker": canon},
        )

    def guidance(self, symbol: str | None = None) -> FeedBuilder:
        """Corporate management EPS and revenue guidance (`/benzinga/v1/guidance`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "benzinga_guidance",
            "/benzinga/v1/guidance",
            BENZINGA_GUIDANCE_SCHEMA,
            {"ticker": canon},
        )

    def consensus_ratings(self, symbol: str | None = None) -> FeedBuilder:
        """Aggregated consensus ratings and price targets (`/benzinga/v1/consensus-ratings`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "benzinga_consensus_ratings",
            "/benzinga/v1/consensus-ratings",
            BENZINGA_CONSENSUS_SCHEMA,
            {"ticker": canon},
        )

    def news(self, symbol: str | None = None) -> FeedBuilder:
        """Real-time Benzinga news articles (`/benzinga/v2/news`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "benzinga_news",
            "/benzinga/v2/news",
            BENZINGA_NEWS_SCHEMA,
            {"tickers": canon},
            date_param="published",
        )

    def analysts(self, analyst_id: str | None = None) -> FeedBuilder:
        """Individual analyst directory and accuracy metrics (`/benzinga/v1/analysts`)."""
        return self._partner_feed(
            "benzinga_analysts",
            "/benzinga/v1/analysts",
            BENZINGA_ANALYSTS_SCHEMA,
            {"benzinga_id": analyst_id},
        )

    def firms(self, firm_id: str | None = None) -> FeedBuilder:
        """Research firm directory (`/benzinga/v1/firms`)."""
        return self._partner_feed(
            "benzinga_firms",
            "/benzinga/v1/firms",
            BENZINGA_FIRMS_SCHEMA,
            {"benzinga_id": firm_id},
        )

    def bulls_say_bears_say(self, symbol: str | None = None) -> FeedBuilder:
        """Curated bull case and bear case summaries (`/benzinga/v1/bulls-bears-say`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "benzinga_bulls_bears",
            "/benzinga/v1/bulls-bears-say",
            BENZINGA_BULLS_BEARS_SCHEMA,
            {"ticker": canon},
        )

    def transcripts(self, symbol: str | None = None) -> FeedBuilder:
        """Full earnings call transcripts (`/benzinga/v1/transcripts`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "benzinga_transcripts",
            "/benzinga/v1/transcripts",
            BENZINGA_TRANSCRIPTS_SCHEMA,
            {"ticker": canon},
        )


class EtfGlobalClient(_PartnerSubClient):
    """ETF Global constituents, fund flows, analytics, profiles, and taxonomies (`m.partners.etf_global`)."""

    def constituents(self, symbol: str | None = None) -> FeedBuilder:
        """Daily ETF underlying holdings and weights (`/etf-global/v1/constituents`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "etf_global_constituents",
            "/etf-global/v1/constituents",
            ETF_GLOBAL_CONSTITUENTS_SCHEMA,
            {"composite_ticker": canon},
            date_param="effective_date",
        )

    def fund_flows(self, symbol: str | None = None) -> FeedBuilder:
        """Daily ETF creation/redemption net dollar flows and NAV (`/etf-global/v1/fund-flows`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "etf_global_fund_flows",
            "/etf-global/v1/fund-flows",
            ETF_GLOBAL_FUND_FLOWS_SCHEMA,
            {"composite_ticker": canon},
            date_param="effective_date",
        )

    def analytics(self, symbol: str | None = None) -> FeedBuilder:
        """ETF Global quantitative risk, reward, and behavioral scores (`/etf-global/v1/analytics`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "etf_global_analytics",
            "/etf-global/v1/analytics",
            ETF_GLOBAL_ANALYTICS_SCHEMA,
            {"composite_ticker": canon},
            date_param="effective_date",
        )

    def profiles(self, symbol: str | None = None) -> FeedBuilder:
        """ETF expense ratios, AUM, issuer, and benchmark metadata (`/etf-global/v1/profiles`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "etf_global_profiles",
            "/etf-global/v1/profiles",
            ETF_GLOBAL_PROFILES_SCHEMA,
            {"composite_ticker": canon},
            date_param="effective_date",
        )

    def taxonomies(self, symbol: str | None = None) -> FeedBuilder:
        """Asset class, category, region, and focus classifications (`/etf-global/v1/taxonomies`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "etf_global_taxonomies",
            "/etf-global/v1/taxonomies",
            ETF_GLOBAL_TAXONOMIES_SCHEMA,
            {"composite_ticker": canon},
            date_param="effective_date",
        )


class TmxClient(_PartnerSubClient):
    """TMX Wall Street Horizon corporate events calendar (`m.partners.tmx`)."""

    def corporate_events(self, symbol: str | None = None) -> FeedBuilder:
        """Conferences, shareholder meetings, dividend dates, and splits (`/tmx/v1/corporate-events`)."""
        canon = self.symbol_normalizer.canonical(symbol) if symbol else None  # type: ignore[union-attr]
        return self._partner_feed(
            "tmx_corporate_events",
            "/tmx/v1/corporate-events",
            TMX_CORPORATE_EVENTS_SCHEMA,
            {"ticker": canon},
            date_param="date",
        )


class PartnersClient:
    """Container for Partner & Alternative Data clients (`benzinga`, `etf_global`, `tmx`)."""

    def __init__(self, session: MassiveSession) -> None:
        self.session = session
        self.benzinga = BenzingaClient(session)
        self.etf_global = EtfGlobalClient(session)
        self.tmx = TmxClient(session)
