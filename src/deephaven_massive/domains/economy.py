"""Macroeconomic and Federal Reserve domain client (``m.economy``).

This module provides :class:`EconomyClient`, which exposes Federal Reserve and
U.S. Treasury macroeconomic datasets via the fluent :class:`FeedBuilder` API.
Every method supports ``.snapshot()``, ``.historical(start=..., end=...)``, and
``.poll(interval=...)``.
"""

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
    from datetime import date

    from deephaven_massive.session import MassiveSession


class EconomyClient(BaseDomainClient):
    """Domain client for Federal Reserve and U.S. Treasury macroeconomic data (``m.economy``).

    Provides access to daily money-market funding conditions, constant-maturity
    Treasury yield curves, realized inflation indices, market/survey inflation
    expectations, and labor-market indicators.
    """

    def __init__(self, session: MassiveSession) -> None:
        """Initialize the macroeconomic domain client.

        Args:
            session: Owning :class:`MassiveSession` instance.
        """
        super().__init__(session, domain=None)

    def funding_conditions(self, date: str | date | None = None, **filters: Any) -> FeedBuilder:
        """Build a feed for overnight funding rates, repo volumes, and commercial paper spreads.

        Wraps ``/fed/v1/funding-conditions`` covering SOFR, EFFR, OBFR, TGCR,
        BGCR, IORB, and the Federal Reserve target range.

        Args:
            date: Optional observation date (``"YYYY-MM-DD"`` or :class:`datetime.date`).
            **filters: Optional REST query filters (e.g., ``date_gte="2025-01-01"``).

        Returns:
            FeedBuilder: Builder supporting ``.snapshot()``, ``.historical()``,
            and ``.poll()`` with ``FUNDING_CONDITIONS_SCHEMA``.
        """
        return self._rest_collection_feed(
            "economy_funding_conditions",
            "/fed/v1/funding-conditions",
            FUNDING_CONDITIONS_SCHEMA,
            {"date": date},
            filters=filters,
        )

    fed_funds_rate = funding_conditions

    def treasury_yields(self, date: str | date | None = None, **filters: Any) -> FeedBuilder:
        """Build a feed for daily U.S. Treasury par yield curve rates (``1M`` through ``30Y``).

        Wraps ``/fed/v1/treasury-yields``.

        Args:
            date: Optional observation date (``"YYYY-MM-DD"`` or :class:`datetime.date`).
            **filters: Optional REST query filters (e.g., ``date_gte="2024-01-01"``).

        Returns:
            FeedBuilder: Builder supporting ``.snapshot()``, ``.historical()``,
            and ``.poll()`` with ``TREASURY_YIELDS_SCHEMA``.
        """
        return self._rest_collection_feed(
            "economy_treasury_yields",
            "/fed/v1/treasury-yields",
            TREASURY_YIELDS_SCHEMA,
            {"date": date},
            filters=filters,
        )

    def inflation(self, date: str | date | None = None, **filters: Any) -> FeedBuilder:
        """Build a feed for Headline and Core CPI and PCE inflation metrics.

        Wraps ``/fed/v1/inflation``.

        Args:
            date: Optional observation date (``"YYYY-MM-DD"`` or :class:`datetime.date`).
            **filters: Optional REST query filters (e.g., ``date_gte="2020-01-01"``).

        Returns:
            FeedBuilder: Builder supporting ``.snapshot()``, ``.historical()``,
            and ``.poll()`` with ``INFLATION_SCHEMA``.
        """
        return self._rest_collection_feed(
            "economy_inflation",
            "/fed/v1/inflation",
            INFLATION_SCHEMA,
            {"date": date},
            filters=filters,
        )

    def inflation_expectations(self, date: str | date | None = None, **filters: Any) -> FeedBuilder:
        """Build a feed for TIPS breakeven inflation, Cleveland Fed model, and NY Fed survey expectations.

        Wraps ``/fed/v1/inflation-expectations``.

        Args:
            date: Optional observation date (``"YYYY-MM-DD"`` or :class:`datetime.date`).
            **filters: Optional REST query filters (e.g., ``date_gte="2020-01-01"``).

        Returns:
            FeedBuilder: Builder supporting ``.snapshot()``, ``.historical()``,
            and ``.poll()`` with ``INFLATION_EXPECTATIONS_SCHEMA``.
        """
        return self._rest_collection_feed(
            "economy_inflation_expectations",
            "/fed/v1/inflation-expectations",
            INFLATION_EXPECTATIONS_SCHEMA,
            {"date": date},
            filters=filters,
        )

    def labor_market(self, date: str | date | None = None, **filters: Any) -> FeedBuilder:
        """Build a feed for unemployment rate, labor force participation, job openings, and jobless claims.

        Wraps ``/fed/v1/labor-market``.

        Args:
            date: Optional observation date (``"YYYY-MM-DD"`` or :class:`datetime.date`).
            **filters: Optional REST query filters (e.g., ``date_gte="2020-01-01"``).

        Returns:
            FeedBuilder: Builder supporting ``.snapshot()``, ``.historical()``,
            and ``.poll()`` with ``LABOR_MARKET_SCHEMA``.
        """
        return self._rest_collection_feed(
            "economy_labor_market",
            "/fed/v1/labor-market",
            LABOR_MARKET_SCHEMA,
            {"date": date},
            filters=filters,
        )
