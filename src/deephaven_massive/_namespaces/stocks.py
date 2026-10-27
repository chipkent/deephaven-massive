"""The `m.stocks` namespace: US equities.

Datasets: trades, quotes, bars, LULD bands, net order imbalances, fair market value,
snapshots, movers, daily open/close, dividends, splits, IPOs, short interest and volume, share
float, and the nested `financials` and `filings` namespaces.

Each dataset is one factory method on `_Stocks` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the catalog by
`datasets()` in `_datasets/catalog.py`.

Status: no datasets are declared yet. `DATASETS` is empty and `_Stocks` has no dataset methods.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._namespaces.base import Namespace

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec

__all__ = ["DATASETS", "_Stocks"]

logger = logging.getLogger(__name__)

DATASETS: Final[tuple[DatasetSpec, ...]] = ()
"""Every dataset of `m.stocks`, one `DatasetSpec` each (for example `stocks.trades`)."""


class _Stocks(Namespace):
    """`m.stocks`: the US equities datasets of a session.

    Created once by `MassiveSession` and reached as `m.stocks`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.
    """

    __slots__ = ()
    _path: ClassVar[str] = "stocks"
