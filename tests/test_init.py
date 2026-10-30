"""Tests for the public surface of `deephaven_massive` and its lazy resolution."""

import ast
import importlib.metadata
import inspect
import json
import subprocess
import sys
from enum import StrEnum
from types import ModuleType

import pytest

import deephaven_massive as dm
from deephaven_massive import EntitlementError, MassiveError

EXPECTED_EXPORTS = [
    "MassiveSession",
    "MassiveConfig",
    "MassiveError",
    "AuthenticationError",
    "EntitlementError",
    "ErrorCause",
    "ALL",
    "AssetClass",
    "Timespan",
    "PollMode",
    "HistorySource",
    "FuturesVenue",
    "Direction",
    "ContractType",
    "SeriesType",
    "Timeframe",
    "Order",
    "Market",
    "Locale",
    "ConditionDataType",
    "Sip",
    "Subscription",
    "TickerType",
    "SymbolConvention",
    "OnError",
    "canonical_symbol",
    "canonical_symbol_stock",
    "canonical_symbol_stock_yahoo",
    "canonical_symbol_stock_nasdaq",
    "canonical_symbol_stock_cms",
    "canonical_symbol_option",
    "canonical_symbol_future",
    "canonical_symbol_future_spread",
    "canonical_symbol_index",
    "canonical_symbol_forex",
    "canonical_symbol_crypto",
    "canonicalize_symbols",
    "datasets",
    "disable_debug_logging",
    "enable_debug_logging",
    "LogOutput",
    "__version__",
]


def _type_checking_imports(module: ModuleType) -> set[str]:
    """Return the names a module imports inside its `if TYPE_CHECKING:` block."""
    tree = ast.parse(inspect.getsource(module))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.If) and ast.unparse(node.test) == "TYPE_CHECKING":
            for statement in node.body:
                assert isinstance(statement, ast.ImportFrom)
                names.update(alias.asname or alias.name for alias in statement.names)
    return names


def test_version_is_the_installed_distribution_version() -> None:
    assert dm.__version__ == importlib.metadata.version("deephaven-massive")


def test_all_is_exactly_the_public_api() -> None:
    assert len(EXPECTED_EXPORTS) == 42
    assert sorted(dm.__all__) == sorted(EXPECTED_EXPORTS)


def test_every_lazy_name_resolves_to_its_defining_module() -> None:
    lazy = set(dm.__all__) - {"__version__"}
    assert set(dm._EXPORTS) == lazy
    for name, module in dm._EXPORTS.items():
        assert getattr(dm, name) is getattr(importlib.import_module(module), name), name


def test_no_lazy_name_is_bound_in_the_module_namespace() -> None:
    # Bound names would bypass `__getattr__`; the names resolve on every access instead.
    for name in dm.__all__:
        getattr(dm, name)
    assert set(vars(dm)) & set(dm._EXPORTS) == set()


@pytest.mark.parametrize("name", ["TYPE_CHECKING", "annotations", "importlib", "import_module"])
def test_helper_names_are_not_reachable(name: str) -> None:
    with pytest.raises(AttributeError):
        getattr(dm, name)


def test_no_public_names_outside_all() -> None:
    # Every public name resolves lazily, so the namespace itself holds no public name at all;
    # anything else would be reachable as `dm.<name>` beyond the exports. The one exception is
    # the public submodule `queries`, which Python binds here once anything imports it.
    assert {name for name in vars(dm) if not name.startswith("_")} <= {"queries"}


def test_type_checking_imports_mirror_all() -> None:
    assert _type_checking_imports(dm) == set(dm.__all__) - {"__version__"}


def test_an_unknown_name_raises_attribute_error() -> None:
    with pytest.raises(AttributeError, match="module 'deephaven_massive' has no attribute 'nope'"):
        _ = dm.nope  # type: ignore[attr-defined]  # the missing attribute is the point


def test_a_private_module_is_not_a_lazy_name() -> None:
    # `_EXPORTS` maps public names only; a private name is never imported on request.
    with pytest.raises(AttributeError):
        _ = dm._no_such_module  # type: ignore[attr-defined]  # the missing attribute is the point


def test_dir_lists_exactly_the_public_names() -> None:
    assert dir(dm) == sorted(EXPECTED_EXPORTS)


def test_from_import_resolves_lazily() -> None:
    # The module-level `from deephaven_massive import ...` above went through `__getattr__`.
    assert EntitlementError is dm.EntitlementError
    assert issubclass(EntitlementError, MassiveError)


def test_public_names_come_from_private_modules() -> None:
    public = [getattr(dm, name) for name in dm.__all__ if name not in {"ALL", "__version__"}]
    for obj in public:
        assert obj.__module__.startswith("deephaven_massive._"), obj


def test_nineteen_enums_are_exported() -> None:
    enums = [
        n
        for n in dm.__all__
        if isinstance(getattr(dm, n), type) and issubclass(getattr(dm, n), StrEnum)
    ]
    assert len(enums) == 19


def test_exception_hierarchy() -> None:
    assert issubclass(dm.AuthenticationError, dm.MassiveError)
    assert issubclass(dm.EntitlementError, dm.MassiveError)
    assert issubclass(dm.MassiveError, Exception)


def _modules_loaded_by(*names: str) -> list[str]:
    """Import the package in a fresh interpreter (no JVM) and read `names` from it.

    Args:
        *names: Public names to read after the import.

    Returns:
        The `deephaven_massive` submodules loaded afterwards, sorted.
    """
    # The script is a literal and the names travel as data on stdin; `-P` keeps the working
    # directory off `sys.path`.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import json, sys\n"
                "import deephaven_massive as dm\n"
                "for name in json.load(sys.stdin):\n"
                "    getattr(dm, name)\n"
                "loaded = [m for m in sys.modules if m.startswith('deephaven_massive.')]\n"
                "print(json.dumps(sorted(loaded)))"
            ),
        ],
        input=json.dumps(names),
        capture_output=True,
        text=True,
        check=True,
    )
    loaded: list[str] = json.loads(result.stdout)
    return loaded


def test_importing_the_package_imports_no_private_module() -> None:
    assert _modules_loaded_by() == []


def test_reading_a_name_imports_only_its_defining_module() -> None:
    assert _modules_loaded_by("Timespan") == ["deephaven_massive._enums"]


def test_the_logging_switch_needs_no_other_module() -> None:
    # It must work before a Deephaven server exists, so it loads nothing that needs the JVM:
    # only itself and the wire package's lazy interface, for the snapshot date.
    assert _modules_loaded_by("enable_debug_logging", "disable_debug_logging", "LogOutput") == [
        "deephaven_massive._debug_log",
        "deephaven_massive._massive",
    ]
