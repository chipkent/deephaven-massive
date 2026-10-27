"""The `m.indices` namespace: indices.

Datasets: index values, bars, snapshots, and daily open/close.

Each dataset is one factory method on `_Indices` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the dataset
catalog (`CATALOG` in `_session.py`).

Status: no datasets are declared yet. `DATASETS` is empty and `_Indices` has no dataset methods.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._namespaces.base import Namespace

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec

__all__ = ["DATASETS", "_Indices"]

logger = logging.getLogger(__name__)

DATASETS: Final[tuple[DatasetSpec, ...]] = ()
"""Every dataset of `m.indices`, one `DatasetSpec` each (for example `indices.values`)."""


class _Indices(Namespace):
    """`m.indices`: the indices datasets of a session.

    Created once by `MassiveSession` and reached as `m.indices`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.
    """

    __slots__ = ()
    _path: ClassVar[str] = "indices"
