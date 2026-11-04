"""The list of datasets: `dm.datasets()`, and how each dataset is called.

The process's catalog (`CATALOG` in `_session.py`) holds one `DatasetSpec` per dataset, built
from the namespace modules when the library is imported. This module turns it into one row per
dataset, so a user can browse, filter, and search what exists from the Deephaven console before
creating a session, or even before an API key is set: nothing here reads the configuration or
the network. What a dataset's columns mean is its query's `columns()` and the dataset reference
page that each row links.

`dataset_calls` pairs each dataset with the session method that serves it, for the generated
dataset reference: the method's signature as a user writes it, the verbs its queries have, and
each argument's line from the method's docstring.
The dataset layer renders the pages but may not import the methods, so the reference command
and its parity test build these here and pass them down.
"""

from __future__ import annotations

import enum
import inspect
import re
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from deephaven import dtypes, new_table
from deephaven.column import InputColumn, string_col
from deephaven.jcompat import j_hashmap
from deephaven.table import Table

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.column_reference import DatasetCall, call_path, page_url
from deephaven_massive._namespaces.query_building import Namespace
from deephaven_massive._query import verbs
from deephaven_massive._session import CATALOG, NAMESPACE_MODULES, MassiveSession

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence
    from collections.abc import Set as AbstractSet

__all__ = ["DATASET_LIST_DESCRIPTIONS", "dataset_calls", "datasets"]


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
        "Documentation": "The web address of its page in the dataset reference.",
    }
)
"""The description of each column of the `datasets()` table, shown as its tooltip."""


def datasets() -> Table:
    """List every dataset the library serves, one row each, as a static table.

    Use it to find a dataset without reading the namespaces one by one: filter it in the
    console, or open a row's `Documentation` link for its columns and plans. It reads only the
    dataset catalog, which exists once the library is imported, so it needs no session and no
    API key and makes no network call.

    Returns:
        A static table, in the order the namespaces declare their datasets, with the columns
        `Dataset` (String, such as `stocks.trades`), `Call` (String, how a session `m` reaches
        it, such as `m.stocks.trades`), `Summary` (String, one sentence), `Verbs` (String[],
        the verbs its queries have, in the order `fetch`, `poll`, `blink`, `append_only`,
        `ring`, `last_by`, `history`), `Transports` (String[], where the library reads it from,
        spelled `REST`, `flat files`, and `WebSocket`, as `columns()` spells them), and
        `Documentation` (String, the web address of its page of the dataset reference).
        Each column carries a description, shown as its tooltip in the Deephaven IDE.

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
            string_col("Documentation", [page_url(spec) for spec in specs]),
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


_AS_OF: Final = "as_of"
"""The shared argument for the day a reference list is read as of, which sets `date`."""

_ARGS_SECTION: Final = re.compile(r"^Args:\n((?:[ \t].*\n|\n)*)", re.MULTILINE)
"""A docstring's `Args` section: the `Args:` line and every indented or blank line after it."""

_ARGS_ENTRY: Final = re.compile(r"^    (\w+): (.*)$")
"""One argument's first line in an `Args` section: its name, then the start of its text."""


def dataset_calls() -> Mapping[str, DatasetCall]:
    """Return how each dataset is called: its method's signature, verbs, and documented arguments.

    Walks every namespace class of the namespace modules, whose public methods are named by
    their path from the session (`stocks.bars`, which `Namespace` sets as each method's
    qualified name), and the session's own dataset methods, such as `m.snapshots`. Reads only
    the classes; needs no session, key, or network.

    Returns:
        A read-only mapping from each dataset's name, such as `"stocks.bars"`, to its
        `DatasetCall`, for every dataset of the catalog. `DatasetCall.typed` holds the wire
        parameter each argument sets (`_wire_name`).

    Raises:
        LookupError: If a catalog dataset has no method, or a method no dataset.
    """
    methods = dict(_dataset_methods())
    names = set(CATALOG.by_name)
    if names != set(methods):
        raise LookupError(
            f"datasets without a method: {sorted(names - set(methods))}; "
            f"methods without a dataset: {sorted(set(methods) - names)}"
        )
    return MappingProxyType({name: _call(name, methods[name]) for name in CATALOG.by_name})


def _dataset_methods() -> Iterator[tuple[str, Callable[..., object]]]:
    """Yield every dataset method with its dataset's name.

    Yields:
        `(dataset name, method)` pairs: each public function of each namespace class of the
        namespace modules, named by its qualified name (its path, such as `stocks.bars`),
        then the session's public methods that name a dataset of the catalog.
    """
    for module in NAMESPACE_MODULES:
        for owner in vars(module).values():
            if (
                isinstance(owner, type)
                and issubclass(owner, Namespace)
                and owner.__module__ == module.__name__
            ):
                for name, member in vars(owner).items():
                    if not name.startswith("_") and inspect.isfunction(member):
                        yield member.__qualname__, member
    for name, member in vars(MassiveSession).items():
        if name in CATALOG.by_name and inspect.isfunction(member):
            yield name, member


def _call(name: str, method: Callable[..., object]) -> DatasetCall:
    """Describe how one dataset is called.

    Args:
        name: The dataset's name.
        method: Its method; its first parameter is `self`.

    Returns:
        Its signature as written from a session `m`, its verbs, and its arguments' wire names.
    """
    parameters = list(inspect.signature(method).parameters.values())[1:]
    parts: list[str] = []
    keyword = False
    for param in parameters:
        if param.kind is inspect.Parameter.KEYWORD_ONLY and not keyword:
            parts.append("*")
            keyword = True
        if param.default is inspect.Parameter.empty:
            parts.append(param.name)
        else:
            parts.append(f"{param.name}={_default(param.default)}")
    spec = CATALOG.by_name[name]
    wire = {
        param.name.split(".")[0]
        for path in spec.rest_paths
        for param in CATALOG.rest_path(path).params
    }
    typed = [_wire_name(param.name, wire) for param in parameters]
    return DatasetCall(
        signature=f"m.{name}({', '.join(parts)})",
        verbs=verbs(spec),
        arguments=tuple(param.name for param in parameters),
        typed=tuple(typed),
        descriptions=tuple(_argument_lines(method).get(param.name, "") for param in parameters),
    )


def _argument_lines(method: Callable[..., object]) -> dict[str, str]:
    """Read each argument's entry from the `Args` section of a method's docstring.

    Args:
        method: A dataset method, whose docstring follows the Google style: an `Args:` line,
            then one entry per argument, `name: text`, indented one level, its text running on
            over lines indented further.

    Returns:
        Each entry's text by argument name, its lines joined by single spaces, such as
        `{"start": "The first ex-dividend date."}`; empty for a method without an `Args`
        section.
    """
    section = _ARGS_SECTION.search(inspect.getdoc(method) or "")
    lines: dict[str, list[str]] = {}
    current: list[str] = []
    for line in (section[1] if section else "").splitlines():
        entry = _ARGS_ENTRY.match(line)
        if entry:
            current = lines.setdefault(entry[1], [])
            current.append(entry[2])
        elif line.strip():
            current.append(line.strip())
    return {name: " ".join(parts) for name, parts in lines.items()}


def _wire_name(argument: str, wire: AbstractSet[str]) -> str:
    """Return the wire parameter an argument of a dataset method sets.

    Args:
        argument: The argument's name, such as `"ticker_type"`.
        wire: The base names of the parameters of the dataset's REST paths.

    Returns:
        The argument itself when a path has a parameter of that name, or when nothing below
        applies; `"date"` for `as_of` (the day a reference list is read as of); otherwise its
        last word when a path has a parameter of that name (`ticker_type` sets `type`,
        `condition_id` sets `id`); otherwise the one path parameter that starts with the
        argument and an underscore (`underlying` sets `underlying_ticker`).
    """
    if argument in wire:
        return argument
    if argument == _AS_OF:
        return "date"
    last = argument.rsplit("_", 1)[-1]
    if last in wire:
        return last
    longer = [name for name in wire if name.startswith(f"{argument}_")]
    return longer[0] if len(longer) == 1 else argument


def _default(value: object) -> str:
    """Write an argument's default as a user would type it.

    Args:
        value: The default.

    Returns:
        `dm.<Enum>.<MEMBER>` for an enum member of the library, such as `dm.Order.ASC`;
        `frozenset(dm.<Enum>)` for a set of every member of one, such as every futures venue;
        and the `repr` of anything else, such as `None`, `True`, or `1`.
    """
    if isinstance(value, enum.Enum):
        return f"dm.{type(value).__name__}.{value.name}"
    if isinstance(value, frozenset) and value:
        kind = type(next(iter(value)))
        if issubclass(kind, enum.Enum) and value == frozenset(kind):
            return f"frozenset(dm.{kind.__name__})"
    return repr(value)
