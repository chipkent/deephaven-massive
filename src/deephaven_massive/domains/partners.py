"""Partner & Alternative Data domain clients (`m.partners.benzinga`, `m.partners.etf_global`, `m.partners.tmx`, `m.partners.fable`)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import (
    BaseDomainClient,
    format_date_param,
    normalize_filter_params,
)
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
    FABLE_MERCHANT_AGGREGATES_SCHEMA,
    FABLE_MERCHANT_HIERARCHY_SCHEMA,
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
        symbol: str | None = None,
        tickers: str | Iterable[str] | None = None,
        ticker_param: str = "ticker",
        filters: Mapping[str, Any] | None = None,
        date_param: str = "date",
    ) -> FeedBuilder:
        base_params = normalize_filter_params(
            params,
            filters,
            symbol=symbol,
            tickers=tickers,
            normalizer=self.symbol_normalizer,
            ticker_param=ticker_param,
        )

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

    def ratings(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Analyst upgrades, downgrades, and price target actions (`/benzinga/v1/ratings`)."""
        return self._partner_feed(
            "benzinga_ratings",
            "/benzinga/v1/ratings",
            BENZINGA_RATINGS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
        )

    def analyst_insights(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Analyst research notes and narrative insights (`/benzinga/v1/analyst-insights`)."""
        return self._partner_feed(
            "benzinga_analyst_insights",
            "/benzinga/v1/analyst-insights",
            BENZINGA_RATINGS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
        )

    def earnings(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Historical and upcoming EPS & revenue actuals vs estimates (`/benzinga/v1/earnings`)."""
        return self._partner_feed(
            "benzinga_earnings",
            "/benzinga/v1/earnings",
            BENZINGA_EARNINGS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
        )

    def guidance(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Corporate management EPS and revenue guidance (`/benzinga/v1/guidance`)."""
        return self._partner_feed(
            "benzinga_guidance",
            "/benzinga/v1/guidance",
            BENZINGA_GUIDANCE_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
        )

    def consensus_ratings(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Aggregated consensus ratings and price targets (`/benzinga/v1/consensus-ratings`)."""
        return self._partner_feed(
            "benzinga_consensus_ratings",
            "/benzinga/v1/consensus-ratings",
            BENZINGA_CONSENSUS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
        )

    def news(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Real-time Benzinga news articles (`/benzinga/v2/news`)."""
        return self._partner_feed(
            "benzinga_news",
            "/benzinga/v2/news",
            BENZINGA_NEWS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            ticker_param="tickers",
            filters=filters,
            date_param="published",
        )

    def analysts(
        self, analyst_id: str | None = None, **filters: Any
    ) -> FeedBuilder:
        """Individual analyst directory and accuracy metrics (`/benzinga/v1/analysts`)."""
        return self._partner_feed(
            "benzinga_analysts",
            "/benzinga/v1/analysts",
            BENZINGA_ANALYSTS_SCHEMA,
            {"benzinga_id": analyst_id},
            filters=filters,
        )

    def firms(
        self, firm_id: str | None = None, **filters: Any
    ) -> FeedBuilder:
        """Research firm directory (`/benzinga/v1/firms`)."""
        return self._partner_feed(
            "benzinga_firms",
            "/benzinga/v1/firms",
            BENZINGA_FIRMS_SCHEMA,
            {"benzinga_id": firm_id},
            filters=filters,
        )

    def bulls_say_bears_say(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Curated bull case and bear case summaries (`/benzinga/v1/bulls-bears-say`)."""
        return self._partner_feed(
            "benzinga_bulls_bears",
            "/benzinga/v1/bulls-bears-say",
            BENZINGA_BULLS_BEARS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
        )

    bulls_bears_say = bulls_say_bears_say

    def transcripts(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Full earnings call transcripts (`/benzinga/v1/transcripts`)."""
        return self._partner_feed(
            "benzinga_transcripts",
            "/benzinga/v1/transcripts",
            BENZINGA_TRANSCRIPTS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
        )


class EtfGlobalClient(_PartnerSubClient):
    """ETF Global constituents, fund flows, analytics, profiles, and taxonomies (`m.partners.etf_global`)."""

    def constituents(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Daily ETF underlying holdings and weights (`/etf-global/v1/constituents`)."""
        return self._partner_feed(
            "etf_global_constituents",
            "/etf-global/v1/constituents",
            ETF_GLOBAL_CONSTITUENTS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            ticker_param="composite_ticker",
            filters=filters,
            date_param="effective_date",
        )

    def fund_flows(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Daily ETF creation/redemption net dollar flows and NAV (`/etf-global/v1/fund-flows`)."""
        return self._partner_feed(
            "etf_global_fund_flows",
            "/etf-global/v1/fund-flows",
            ETF_GLOBAL_FUND_FLOWS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            ticker_param="composite_ticker",
            filters=filters,
            date_param="effective_date",
        )

    def analytics(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """ETF Global quantitative risk, reward, and behavioral scores (`/etf-global/v1/analytics`)."""
        return self._partner_feed(
            "etf_global_analytics",
            "/etf-global/v1/analytics",
            ETF_GLOBAL_ANALYTICS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            ticker_param="composite_ticker",
            filters=filters,
            date_param="effective_date",
        )

    def profiles(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """ETF expense ratios, AUM, issuer, and benchmark metadata (`/etf-global/v1/profiles`)."""
        return self._partner_feed(
            "etf_global_profiles",
            "/etf-global/v1/profiles",
            ETF_GLOBAL_PROFILES_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            ticker_param="composite_ticker",
            filters=filters,
            date_param="effective_date",
        )

    def taxonomies(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Asset class, category, region, and focus classifications (`/etf-global/v1/taxonomies`)."""
        return self._partner_feed(
            "etf_global_taxonomies",
            "/etf-global/v1/taxonomies",
            ETF_GLOBAL_TAXONOMIES_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            ticker_param="composite_ticker",
            filters=filters,
            date_param="effective_date",
        )


class TmxClient(_PartnerSubClient):
    """TMX Wall Street Horizon corporate events calendar (`m.partners.tmx`)."""

    def corporate_events(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Conferences, shareholder meetings, dividend dates, and splits (`/tmx/v1/corporate-events`)."""
        return self._partner_feed(
            "tmx_corporate_events",
            "/tmx/v1/corporate-events",
            TMX_CORPORATE_EVENTS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="date",
        )


class FableClient(_PartnerSubClient):
    """Fable Data European consumer transaction aggregates and merchant hierarchy (`m.partners.fable`)."""

    def eu_merchant_aggregates(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """European consumer transaction merchant spend aggregates (`/fable/v1/eu/merchant-aggregates`)."""
        return self._partner_feed(
            "fable_eu_merchant_aggregates",
            "/fable/v1/eu/merchant-aggregates",
            FABLE_MERCHANT_AGGREGATES_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="transaction_date",
        )

    def eu_merchant_hierarchy(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """European merchant-to-corporate parent hierarchy (`/fable/v1/eu/merchant-hierarchy`)."""
        return self._partner_feed(
            "fable_eu_merchant_hierarchy",
            "/fable/v1/eu/merchant-hierarchy",
            FABLE_MERCHANT_HIERARCHY_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
            date_param="effective_date",
        )


class PartnersClient:
    """Container for Partner & Alternative Data clients (`benzinga`, `etf_global`, `tmx`, `fable`)."""

    def __init__(self, session: MassiveSession) -> None:
        self.session = session
        self.benzinga = BenzingaClient(session)
        self.etf_global = EtfGlobalClient(session)
        self.tmx = TmxClient(session)
        self.fable = FableClient(session)
        # Flat convenience delegates matching Plan §5.8
        self.benzinga_analyst_insights = self.benzinga.analyst_insights
        self.benzinga_analysts = self.benzinga.analysts
        self.benzinga_bulls_bears = self.benzinga.bulls_say_bears_say
        self.benzinga_consensus_ratings = self.benzinga.consensus_ratings
        self.benzinga_earnings = self.benzinga.earnings
        self.benzinga_firms = self.benzinga.firms
        self.benzinga_guidance = self.benzinga.guidance
        self.benzinga_news = self.benzinga.news
        self.benzinga_ratings = self.benzinga.ratings
        self.benzinga_transcripts = self.benzinga.transcripts
        self.etf_analytics = self.etf_global.analytics
        self.etf_constituents = self.etf_global.constituents
        self.etf_fund_flows = self.etf_global.fund_flows
        self.etf_profiles = self.etf_global.profiles
        self.etf_taxonomies = self.etf_global.taxonomies
        self.tmx_corporate_events = self.tmx.corporate_events
