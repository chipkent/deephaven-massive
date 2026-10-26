"""The `m.forex` namespace: foreign exchange.

Datasets: quotes, bars, fair market value, snapshots, movers, and currency
conversion.

Each dataset is one factory method on `_Forex` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the catalog by
`_catalog.datasets()`.

Status: no datasets are declared yet. `DATASETS` is empty and `_Forex` has no dataset methods.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._domains.base import Namespace

if TYPE_CHECKING:
    from deephaven_massive._catalog import DatasetSpec

__all__ = ["DATASETS", "_Forex"]

logger = logging.getLogger(__name__)

DATASETS: Final[tuple[DatasetSpec, ...]] = ()
"""Every dataset of `m.forex`, one `DatasetSpec` each (for example `forex.quotes`)."""


class _Forex(Namespace):
    """`m.forex`: the foreign exchange datasets of a session.

    Created once by `MassiveSession` and reached as `m.forex`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.
    """

    __slots__ = ()
    _path: ClassVar[str] = "forex"
