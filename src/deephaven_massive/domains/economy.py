"""Macroeconomic & Federal Reserve domain client (`m.economy`)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import BaseDomainClient
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.economy import (
    FUNDING_CONDITIONS_SCHEMA,
    INFLATION_EXPECTATIONS_SCHEMA,
    INFLATION_SCHEMA,
    LABOR_MARKET_SCHEMA,
    TREASURY_YIELDS_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven_massive.session import MassiveSession


class EconomyClient(BaseDomainClient):
    """Domain client for Federal Reserve & Macroeconomic data (`m.economy`)."""

    def __init__(self, session: MassiveSession) -> None:
        super().__init__(session, domain=None)

    def funding_conditions(self, **filters: Any) -> FeedBuilder:
        """SOFR, EFFR, OBFR, TGCR, BGCR, and Fed Funds target range (`/fed/v1/funding-conditions`)."""
        return self._rest_collection_feed(
            "economy_funding_conditions",
            "/fed/v1/funding-conditions",
            FUNDING_CONDITIONS_SCHEMA,
            filters=filters,
        )

    def treasury_yields(self, **filters: Any) -> FeedBuilder:
        """Daily US Treasury yield curve (`1M` through `30Y` nominal + TIPS real yields, `/fed/v1/treasury-yields`)."""
        return self._rest_collection_feed(
            "economy_treasury_yields",
            "/fed/v1/treasury-yields",
            TREASURY_YIELDS_SCHEMA,
            filters=filters,
        )

    def inflation(self, **filters: Any) -> FeedBuilder:
        """Headline & Core CPI, PCE, and PPI (`/fed/v1/inflation`)."""
        return self._rest_collection_feed(
            "economy_inflation",
            "/fed/v1/inflation",
            INFLATION_SCHEMA,
            filters=filters,
        )

    def inflation_expectations(self, **filters: Any) -> FeedBuilder:
        """TIPS breakeven inflation, Cleveland Fed model, and Michigan survey (`/fed/v1/inflation-expectations`)."""
        return self._rest_collection_feed(
            "economy_inflation_expectations",
            "/fed/v1/inflation-expectations",
            INFLATION_EXPECTATIONS_SCHEMA,
            filters=filters,
        )

    def labor_market(self, **filters: Any) -> FeedBuilder:
        """Unemployment rate, nonfarm payrolls, jobless claims, LFPR, and hourly earnings (`/fed/v1/labor-market`)."""
        return self._rest_collection_feed(
            "economy_labor_market",
            "/fed/v1/labor-market",
            LABOR_MARKET_SCHEMA,
            filters=filters,
        )
