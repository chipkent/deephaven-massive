"""The `m.reference` namespace: cross-asset reference data (DESIGN § 8.7).

Datasets: tickers, ticker details, types, and events, related companies, news,
exchanges, condition codes, market status, and market holidays.

Each dataset is one factory method on `_Reference` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the catalog by
`_catalog.datasets()`.

Status: the datasets are declared by the namespace's own work package. Until it lands,
`DATASETS` is empty and `_Reference` has no dataset methods.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

from deephaven_massive._domains.base import Namespace

if TYPE_CHECKING:
    from deephaven_massive._catalog import DatasetSpec

__all__ = ["DATASETS", "_Reference"]

logger = logging.getLogger(__name__)

DATASETS: Final[tuple[DatasetSpec, ...]] = ()
"""Every dataset of `m.reference`, one `DatasetSpec` each (for example `reference.news`)."""


class _Reference(Namespace):
    """`m.reference`: the cross-asset reference data datasets of a session.

    Created once by `MassiveSession` and reached as `m.reference`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.
    """

    __slots__ = ()
