"""The `m.options` namespace: US options (OPRA).

Datasets: trades, quotes, bars, fair market value, option chains, contract
snapshots, contract reference data, and daily open/close.

Each dataset is one factory method on `_Options` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the catalog by
`datasets()` in `_datasets/catalog.py`.

Status: no datasets are declared yet. `DATASETS` is empty and `_Options` has no dataset methods.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._namespaces.base import Namespace

if TYPE_CHECKING:
    from deephaven_massive._datasets.catalog import DatasetSpec

__all__ = ["DATASETS", "_Options"]

logger = logging.getLogger(__name__)

DATASETS: Final[tuple[DatasetSpec, ...]] = ()
"""Every dataset of `m.options`, one `DatasetSpec` each (for example `options.trades`)."""


class _Options(Namespace):
    """`m.options`: the US options (OPRA) datasets of a session.

    Created once by `MassiveSession` and reached as `m.options`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.
    """

    __slots__ = ()
    _path: ClassVar[str] = "options"
