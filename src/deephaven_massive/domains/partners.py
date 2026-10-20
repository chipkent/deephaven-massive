"""Partner & Alternative Data domain clients (`m.partners`).

This module implements :class:`PartnersClient` and its four specialized vendor
sub-clients:

- :class:`BenzingaClient` (`m.partners.benzinga`): Real-time analyst ratings,
  narrative insights, earnings actuals vs. estimates, corporate guidance,
  consensus ratings, news, analyst/firm directories, bull/bear cases, and
  earnings call transcripts.
- :class:`EtfGlobalClient` (`m.partners.etf_global`): Daily ETF underlying
  constituents, creation/redemption fund flows, quantitative risk/reward
  analytics, fund profiles, and asset-class taxonomies.
- :class:`TmxClient` (`m.partners.tmx`): Wall Street Horizon corporate events
  calendar (conferences, shareholder meetings, dividends, splits).
- :class:`FableClient` (`m.partners.fable`): European consumer transaction
  merchant spend aggregates and merchant-to-corporate parent hierarchy.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import BaseDomainClient
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
    """Base helper for Partner & Alternative Data sub-clients.

    Inherits from :class:`~deephaven_massive.domains.base.BaseDomainClient`
    with US equity ticker normalization and exposes `_partner_feed` for
    configuring `.snapshot()`, `.poll()`, and `.historical()` REST collection
    feeds.
    """

    def __init__(self, session: MassiveSession) -> None:
        """Initialize the partner sub-client.

        Args:
            session: Active :class:`~deephaven_massive.session.MassiveSession`.
        """
        super().__init__(session, domain="stocks")

    _partner_feed = BaseDomainClient._rest_collection_feed


class BenzingaClient(_PartnerSubClient):
    """Domain client for Benzinga analyst, earnings, guidance, and news feeds (`m.partners.benzinga`).

    Every method returns a :class:`~deephaven_massive.engine.builder.FeedBuilder`
    supporting `.snapshot()`, `.poll()`, and `.historical()`.
    """

    def ratings(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Configure a Benzinga analyst upgrades, downgrades, and price-target actions feed (`/benzinga/v1/ratings`).

        Args:
            symbol: Optional single equity ticker filter (e.g., `"AAPL"`).
            tickers: Optional ticker or iterable of tickers for multi-symbol
                filtering (`ticker.any_of`).
            **filters: Additional Massive REST query parameters (e.g.,
                `date__gte="2025-01-01"`, `rating_action="Upgrades"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_RATINGS_SCHEMA`.
        """
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
        """Configure a Benzinga analyst research notes and narrative insights feed (`/benzinga/v1/analyst-insights`).

        Args:
            symbol: Optional single equity ticker filter.
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_RATINGS_SCHEMA`.
        """
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
        """Configure a Benzinga EPS and revenue actuals vs. consensus estimates feed (`/benzinga/v1/earnings`).

        Args:
            symbol: Optional single equity ticker filter.
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters (e.g.,
                `date__gte="2025-01-01"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_EARNINGS_SCHEMA`.
        """
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
        """Configure a Benzinga corporate management EPS and revenue guidance feed (`/benzinga/v1/guidance`).

        Args:
            symbol: Optional single equity ticker filter.
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_GUIDANCE_SCHEMA`.
        """
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
        """Configure a Benzinga aggregated consensus ratings and price targets feed (`/benzinga/v1/consensus-ratings`).

        Args:
            symbol: Optional single equity ticker filter.
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_CONSENSUS_SCHEMA`.
        """
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
        """Configure a real-time Benzinga financial news articles feed (`/benzinga/v2/news`).

        Args:
            symbol: Optional single equity ticker filter.
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters (e.g.,
                `published__gte="2025-01-01"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_NEWS_SCHEMA`.
        """
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
        """Configure a Benzinga wall-street analyst directory and performance metrics feed (`/benzinga/v1/analysts`).

        Args:
            analyst_id: Optional Benzinga analyst identifier filter.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_ANALYSTS_SCHEMA`.
        """
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
        """Configure a Benzinga research firm directory feed (`/benzinga/v1/firms`).

        Args:
            firm_id: Optional Benzinga research firm identifier filter.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_FIRMS_SCHEMA`.
        """
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
        """Configure a Benzinga curated bull-case and bear-case investment thesis feed (`/benzinga/v1/bulls-bears-say`).

        Args:
            symbol: Optional single equity ticker filter.
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_BULLS_BEARS_SCHEMA`.
        """
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
        """Configure a Benzinga full earnings-call transcripts feed (`/benzinga/v1/transcripts`).

        Args:
            symbol: Optional single equity ticker filter.
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `BENZINGA_TRANSCRIPTS_SCHEMA`.
        """
        return self._partner_feed(
            "benzinga_transcripts",
            "/benzinga/v1/transcripts",
            BENZINGA_TRANSCRIPTS_SCHEMA,
            symbol=symbol,
            tickers=tickers,
            filters=filters,
        )


class EtfGlobalClient(_PartnerSubClient):
    """Domain client for ETF Global holdings, fund flows, analytics, profiles, and taxonomies (`m.partners.etf_global`)."""

    def constituents(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Configure an ETF Global daily underlying constituents and weights feed (`/etf-global/v1/constituents`).

        Args:
            symbol: Optional composite ETF ticker filter (e.g., `"SPY"`).
            tickers: Optional ETF ticker or iterable of ETF tickers.
            **filters: Additional Massive REST query parameters (e.g.,
                `effective_date="2025-01-15"`, `constituent_ticker="AAPL"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `ETF_GLOBAL_CONSTITUENTS_SCHEMA`.
        """
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
        """Configure an ETF Global daily creation/redemption net dollar flows and NAV feed (`/etf-global/v1/fund-flows`).

        Args:
            symbol: Optional composite ETF ticker filter (e.g., `"SPY"`).
            tickers: Optional ETF ticker or iterable of ETF tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `ETF_GLOBAL_FUND_FLOWS_SCHEMA`.
        """
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
        """Configure an ETF Global quantitative risk, reward, and behavioral scores feed (`/etf-global/v1/analytics`).

        Args:
            symbol: Optional composite ETF ticker filter (e.g., `"SPY"`).
            tickers: Optional ETF ticker or iterable of ETF tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `ETF_GLOBAL_ANALYTICS_SCHEMA`.
        """
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
        """Configure an ETF Global expense ratios, AUM, issuer, and benchmark metadata feed (`/etf-global/v1/profiles`).

        Args:
            symbol: Optional composite ETF ticker filter (e.g., `"SPY"`).
            tickers: Optional ETF ticker or iterable of ETF tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `ETF_GLOBAL_PROFILES_SCHEMA`.
        """
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
        """Configure an ETF Global asset-class, category, region, and focus classifications feed (`/etf-global/v1/taxonomies`).

        Args:
            symbol: Optional composite ETF ticker filter (e.g., `"SPY"`).
            tickers: Optional ETF ticker or iterable of ETF tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `ETF_GLOBAL_TAXONOMIES_SCHEMA`.
        """
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
    """Domain client for TMX Wall Street Horizon corporate events calendar (`m.partners.tmx`)."""

    def corporate_events(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Configure a TMX corporate events calendar feed (`/tmx/v1/corporate-events`).

        Args:
            symbol: Optional equity ticker filter (e.g., `"AAPL"`).
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters (e.g.,
                `date__gte="2025-01-01"`, `type="earnings_conference_call"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with `TMX_CORPORATE_EVENTS_SCHEMA`.
        """
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
    """Domain client for Fable Data European consumer transaction feeds (`m.partners.fable`)."""

    def eu_merchant_aggregates(
        self,
        symbol: str | None = None,
        *,
        tickers: str | Iterable[str] | None = None,
        **filters: Any,
    ) -> FeedBuilder:
        """Configure a Fable Data European consumer transaction merchant spend aggregates feed (`/fable/v1/eu/merchant-aggregates`).

        Args:
            symbol: Optional listed parent equity ticker filter.
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters (e.g.,
                `transaction_date__gte="2025-01-01"`, `country="GB"`).

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with
            `FABLE_MERCHANT_AGGREGATES_SCHEMA`.
        """
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
        """Configure a Fable Data European merchant-to-corporate parent hierarchy feed (`/fable/v1/eu/merchant-hierarchy`).

        Args:
            symbol: Optional listed parent equity ticker filter.
            tickers: Optional ticker or iterable of tickers.
            **filters: Additional Massive REST query parameters.

        Returns:
            FeedBuilder: Configured feed builder supporting `.snapshot()`,
            `.poll()`, and `.historical()` with
            `FABLE_MERCHANT_HIERARCHY_SCHEMA`.
        """
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
    """Namespace container for Partner & Alternative Data clients (`m.partners`).

    Exposes both structured vendor sub-clients (`m.partners.benzinga`,
    `m.partners.etf_global`, `m.partners.tmx`, `m.partners.fable`) and flat
    convenience methods (`m.partners.benzinga_ratings()`,
    `m.partners.etf_fund_flows()`, `m.partners.tmx_corporate_events()`, etc.).

    Attributes:
        session: Parent :class:`~deephaven_massive.session.MassiveSession`.
        benzinga: :class:`BenzingaClient` sub-client.
        etf_global: :class:`EtfGlobalClient` sub-client.
        tmx: :class:`TmxClient` sub-client.
        fable: :class:`FableClient` sub-client.
    """

    def __init__(self, session: MassiveSession) -> None:
        """Initialize the Partner & Alternative Data container client.

        Args:
            session: Active :class:`~deephaven_massive.session.MassiveSession`.
        """
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
