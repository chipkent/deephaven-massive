"""Macroeconomic & Federal Reserve domain client (`m.economy`)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from deephaven_massive.domains.base import BaseDomainClient, format_date_param
from deephaven_massive.engine.arrow_bridge import SchemaSpec
from deephaven_massive.engine.builder import FeedBuilder
from deephaven_massive.schemas.economy import (
    FUNDING_CONDITIONS_SCHEMA,
    INFLATION_EXPECTATIONS_SCHEMA,
    INFLATION_SCHEMA,
    LABOR_MARKET_SCHEMA,
    TREASURY_YIELDS_SCHEMA,
)

if TYPE_CHECKING:
    from deephaven.table import Table
    from deephaven_massive.session import MassiveSession


class EconomyClient(BaseDomainClient):
    """Domain client for Federal Reserve & Macroeconomic data (`m.economy`)."""

    def __init__(self, session: MassiveSession) -> None:
        super().__init__(session, domain=None)

    def _econ_feed(
        self, name: str, path: str, schema_spec: SchemaSpec
    ) -> FeedBuilder:
        def _hist(
            start: Any = None,
            end: Any = None,
            limit: int | None = None,
            source: str = "auto",
        ) -> Table:
            params: dict[str, Any] = {
                "date.gte": format_date_param(start),
                "date.lte": format_date_param(end),
            }
            return self.fetch_chunked_table(
                path, schema_spec, params, limit=limit
            )

        return self.build_feed(
            name=name,
            schema_spec=schema_spec,
            snapshot_fn=lambda: self.fetch_rest_rows(path),
            historical_fn=_hist,
        )

    def funding_conditions(self) -> FeedBuilder:
        """SOFR, EFFR, OBFR, TGCR, BGCR, and Fed Funds target range (`/fed/v1/funding`)."""
        return self._econ_feed(
            "economy_funding_conditions",
            "/fed/v1/funding",
            FUNDING_CONDITIONS_SCHEMA,
        )

    def treasury_yields(self) -> FeedBuilder:
        """Daily US Treasury yield curve (`1M` through `30Y` nominal + TIPS real yields, `/fed/v1/treasury-yields`)."""
        return self._econ_feed(
            "economy_treasury_yields",
            "/fed/v1/treasury-yields",
            TREASURY_YIELDS_SCHEMA,
        )

    def inflation(self) -> FeedBuilder:
        """Headline & Core CPI, PCE, and PPI (`/fed/v1/inflation`)."""
        return self._econ_feed(
            "economy_inflation",
            "/fed/v1/inflation",
            INFLATION_SCHEMA,
        )

    def inflation_expectations(self) -> FeedBuilder:
        """TIPS breakeven inflation, Cleveland Fed model, and Michigan survey (`/fed/v1/inflation-expectations`)."""
        return self._econ_feed(
            "economy_inflation_expectations",
            "/fed/v1/inflation-expectations",
            INFLATION_EXPECTATIONS_SCHEMA,
        )

    def labor_market(self) -> FeedBuilder:
        """Unemployment rate, nonfarm payrolls, jobless claims, LFPR, and hourly earnings (`/fed/v1/labor-market`)."""
        return self._econ_feed(
            "economy_labor_market",
            "/fed/v1/labor-market",
            LABOR_MARKET_SCHEMA,
        )
