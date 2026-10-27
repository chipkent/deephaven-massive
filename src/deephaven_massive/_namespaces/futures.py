"""The `m.futures` namespace: US futures (CME, CBOT, NYMEX, COMEX).

Datasets: trades, quotes, bars, snapshots, contracts, products, exchanges,
market status, and schedules.

Each dataset is one factory method on `_Futures` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset
catalog (`CATALOG` in `_session.py`).

Status: no datasets are declared yet. `DATASETS` is empty and `_Futures` has no dataset methods.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._namespaces.base import Namespace

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec

__all__ = ["DATASETS", "_Futures"]

logger = logging.getLogger(__name__)

DATASETS: Final[tuple[DatasetSpec, ...]] = ()
"""Every dataset of `m.futures`, one `DatasetSpec` each (for example `futures.trades`)."""


class _Futures(Namespace):
    """`m.futures`: the US futures (CME, CBOT, NYMEX, COMEX) datasets of a session.

    Created once by `MassiveSession` and reached as `m.futures`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.
    """

    __slots__ = ()
    _path: ClassVar[str] = "futures"
