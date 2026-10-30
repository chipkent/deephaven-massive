"""`dm.datasets()`: the static table that lists every dataset the library serves.

The process's catalog (`CATALOG` in `_session.py`) holds one `DatasetSpec` per dataset, built
from the namespace modules when the library is imported. This module turns it into one row per
dataset, so a user can browse, filter, and search what exists from the Deephaven console before
creating a session, or even before an API key is set: nothing here reads the configuration or
the network. What a dataset's columns mean is its query's `columns()` and the column reference
that each row links.
"""

from __future__ import annotations

import logging
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from deephaven import dtypes, new_table
from deephaven.column import InputColumn, string_col
from deephaven.jcompat import j_hashmap
from deephaven.table import Table

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.column_reference import call_path, section_url
from deephaven_massive._query import verbs
from deephaven_massive._session import CATALOG

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = ["DATASET_LIST_DESCRIPTIONS", "datasets"]

logger = logging.getLogger(__name__)

DATASET_LIST_DESCRIPTIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "Dataset": "The dataset's name: its namespace and method, such as stocks.trades.",
        "Call": "How a session m reaches the dataset, such as m.stocks.trades.",
        "Summary": "What the dataset holds, in one sentence.",
        "Verbs": (
            "The verbs its queries have, such as fetch and poll. Bars stream only at some "
            "timespans, as their docstring says."
        ),
        "Transports": "Where its rows come from: REST, flat files, WebSocket.",
        "Documentation": "The web address of its section in the column reference.",
    }
)
"""The description of each column of the `datasets()` table, shown as its tooltip."""


def datasets() -> Table:
    """List every dataset the library serves, one row each, as a static table.

    Use it to find a dataset without reading the namespaces one by one: filter it in the
    console, or open a row's `Documentation` link for its columns. It reads only the dataset
    catalog, which exists once the library is imported, so it needs no session and no API key
    and makes no network call.

    Returns:
        A static table, in the order the namespaces declare their datasets, with the columns
        `Dataset` (String, such as `stocks.trades`), `Call` (String, how a session `m` reaches
        it, such as `m.stocks.trades`), `Summary` (String, one sentence), `Verbs` (String[],
        the verbs its queries have, in the order `fetch`, `poll`, `blink`, `append_only`,
        `ring`, `last_by`, `history`), `Transports` (String[], the transports that serve it,
        spelled `REST`, `flat files`, and `WebSocket`, as `columns()` spells them), and
        `Documentation` (String, the web address of its section of the column reference).
        Each column carries a description, shown as its tooltip in the web UI.

    The table is created on the calling thread, in the liveness scope at the top of its stack,
    as any static table the caller makes is.

    Example:
        ::

            import deephaven_massive as dm

            catalog = dm.datasets()
            streams = catalog.where("Arrays.asList(Verbs).contains(`blink`)")
            news = catalog.where("Summary.toLowerCase().contains(`news`)")
    """
    specs = tuple(CATALOG.by_name.values())
    table = new_table(
        [
            string_col("Dataset", [spec.name for spec in specs]),
            string_col("Call", [call_path(spec) for spec in specs]),
            string_col("Summary", [spec.summary for spec in specs]),
            _string_arrays("Verbs", [verbs(spec) for spec in specs]),
            _string_arrays(
                "Transports",
                [
                    tuple(
                        label
                        for transport, label in TRANSPORT_LABELS.items()
                        if any(source is transport for source, _ in spec.sources)
                    )
                    for spec in specs
                ],
            ),
            string_col("Documentation", [section_url(spec) for spec in specs]),
        ]
    )
    described = table.j_table.withColumnDescriptions(j_hashmap(dict(DATASET_LIST_DESCRIPTIONS)))
    return Table(j_table=described)


def _string_arrays(name: str, rows: Sequence[Sequence[str]]) -> InputColumn:
    """Build a `String[]` column of a new table.

    Args:
        name: The column's name.
        rows: Each row's strings.

    Returns:
        The column, for `new_table`.
    """
    return InputColumn(
        name,
        dtypes.string_array,
        input_data=[dtypes.array(dtypes.string, list(row)) for row in rows],
    )
