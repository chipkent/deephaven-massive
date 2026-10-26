"""The `m.crypto` namespace: crypto pairs.

Datasets: trades, quotes, bars, fair market value, snapshots, movers, and daily
open/close.

Each dataset is one factory method on `_Crypto` that validates its arguments and returns a query;
nothing touches the network until a verb is called on the query. This module owns the
namespace's `DatasetSpec`s, exported as `DATASETS` and composed into the catalog by
`_catalog.datasets()`.

Status: no datasets are declared yet. `DATASETS` is empty and `_Crypto` has no dataset methods.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final

from deephaven_massive._namespaces.base import Namespace

if TYPE_CHECKING:
    from deephaven_massive._catalog import DatasetSpec

__all__ = ["DATASETS", "_Crypto"]

logger = logging.getLogger(__name__)

DATASETS: Final[tuple[DatasetSpec, ...]] = ()
"""Every dataset of `m.crypto`, one `DatasetSpec` each (for example `crypto.trades`)."""


class _Crypto(Namespace):
    """`m.crypto`: the crypto pairs datasets of a session.

    Created once by `MassiveSession` and reached as `m.crypto`. It holds no state of its own,
    so it is safe to use from any thread; each method builds a query and never blocks.
    """

    __slots__ = ()
    _path: ClassVar[str] = "crypto"
