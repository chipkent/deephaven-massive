"""The `m.partners` namespace: datasets from Massive's data partners.

Partner datasets stay nested by vendor, one class each:

- `m.partners.benzinga` (`_Benzinga`): analysts, analyst insights, ratings, consensus
  ratings, bulls-and-bears summaries, guidance, earnings, firms, and news;
- `m.partners.etf_global` (`_EtfGlobal`): ETF analytics, constituents, fund flows, profiles,
  and taxonomies;
- `m.partners.tmx` (`_Tmx`): corporate events;
- `m.partners.fable` (`_Fable`): European consumer-spending merchant aggregates and hierarchy.

This module owns the partners' `DatasetSpec`s, exported as `DATASETS` and composed into the
catalog by `catalog.datasets()`.

Status: no datasets are declared yet. `DATASETS` is empty and the vendor classes have no dataset
methods.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._namespaces.base import Namespace

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._tables.core import SessionCore

__all__ = ["DATASETS", "_Benzinga", "_EtfGlobal", "_Fable", "_Partners", "_Tmx"]

logger = logging.getLogger(__name__)

DATASETS: Final[tuple[DatasetSpec, ...]] = ()
"""Every partner dataset, one `DatasetSpec` each (for example `partners.benzinga.news`)."""


class _Benzinga(Namespace):
    """`m.partners.benzinga`: Benzinga's analyst, earnings, guidance, and news datasets.

    Created once per session by `_Partners`; holds no state of its own.
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.benzinga"


class _EtfGlobal(Namespace):
    """`m.partners.etf_global`: ETF Global's fund analytics, holdings, and flows.

    Created once per session by `_Partners`; holds no state of its own.
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.etf_global"


class _Tmx(Namespace):
    """`m.partners.tmx`: TMX corporate events.

    Created once per session by `_Partners`; holds no state of its own.
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.tmx"


class _Fable(Namespace):
    """`m.partners.fable`: Fable's European consumer-spending datasets.

    Created once per session by `_Partners`; holds no state of its own.
    """

    __slots__ = ()
    _path: ClassVar[str] = "partners.fable"


class _Partners(Namespace):
    """`m.partners`: one attribute per data vendor.

    Created once by `MassiveSession`, together with its four vendor namespaces. Holds no
    mutable state, so it is safe to use from any thread.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()
            m.partners.benzinga  # <MassiveSession #1 partners.benzinga>, the Benzinga datasets
    """

    __slots__ = ("_benzinga", "_etf_global", "_fable", "_tmx")
    _path: ClassVar[str] = "partners"

    def __init__(self, core: SessionCore) -> None:
        """Create the four vendor namespaces.

        Args:
            core: The session's internal core.
        """
        super().__init__(core)
        self._benzinga = _Benzinga(core)
        self._etf_global = _EtfGlobal(core)
        self._tmx = _Tmx(core)
        self._fable = _Fable(core)

    @property
    def benzinga(self) -> _Benzinga:
        """The Benzinga datasets."""
        return self._benzinga

    @property
    def etf_global(self) -> _EtfGlobal:
        """The ETF Global datasets."""
        return self._etf_global

    @property
    def tmx(self) -> _Tmx:
        """The TMX datasets."""
        return self._tmx

    @property
    def fable(self) -> _Fable:
        """The Fable consumer-spending datasets."""
        return self._fable
