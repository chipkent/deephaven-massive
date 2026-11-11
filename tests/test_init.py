"""Tests for the public surface of `deephaven_massive` and its lazy resolution."""

import ast
import builtins
import functools
import importlib
import importlib.metadata
import inspect
import json
import subprocess
import sys
import typing
from collections.abc import Callable
from enum import Enum, StrEnum
from types import ModuleType
from typing import ParamSpec, ParamSpecArgs, ParamSpecKwargs, TypeVar

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
    "Reread",
    "FuturesVenue",
    "Direction",
    "ContractType",
    "SeriesType",
    "Timeframe",
    "Order",
    "Market",
    "Locale",
    "ConditionDataType",
    "ConnectionTaken",
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
    assert len(EXPECTED_EXPORTS) == 44
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
    # anything else would be reachable as `dm.<name>` beyond the exports. The exceptions are
    # the public submodules `queries` and `namespaces`, which Python binds here once anything
    # imports them.
    assert {name for name in vars(dm) if not name.startswith("_")} <= {"queries", "namespaces"}


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


def test_twenty_one_enums_are_exported() -> None:
    enums = [
        n
        for n in dm.__all__
        if isinstance(getattr(dm, n), type) and issubclass(getattr(dm, n), StrEnum)
    ]
    assert len(enums) == 21


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


# The public modules, whose `__all__` is the whole public surface.
_PUBLIC_MODULES = ("deephaven_massive", "deephaven_massive.queries", "deephaven_massive.namespaces")

# The dunder methods whose signatures users see in `help()` and API references.
_PUBLIC_DUNDERS = frozenset({"__init__", "__call__", "__enter__", "__exit__", "__iter__"})


def _in_package(module: str | None) -> bool:
    """Say whether a module name is the package or one of its modules.

    Returns:
        True for `deephaven_massive` and its modules.
    """
    return module is not None and (
        module == "deephaven_massive" or module.startswith("deephaven_massive.")
    )


class _PublicApiWalk:
    """Walk everything reachable from the public modules and record each private type named.

    A type is private when the package defines it and no public module exports it: a class or
    function whose `__module__` is a package module, or an alias a package module assigns at
    its top level. The walk covers each public name; each member of each class reached,
    inherited ones included: methods, dunders, properties, cached properties, class-level
    values, and annotated attributes; and each signature and overload, with its parameters,
    defaults, and return. A class or alias reached through an annotation is walked in turn, so
    a sub-namespace returned by a property is checked like a top-level one. A constructor
    parameter whose name starts with an underscore is internal (a namespace's session core),
    so its type is not checked; on any other method, such a parameter is checked.
    """

    def __init__(self, modules: tuple[ModuleType, ...]) -> None:
        self.public: dict[int, str] = {}
        for module in modules:
            for name in module.__all__:
                self.public.setdefault(id(getattr(module, name)), f"{module.__name__}.{name}")
        self.findings: set[str] = set()
        self.walked: set[int] = set()
        self.methods: dict[str, int] = {}
        self._aliases: set[int] = set()
        for name, loaded in list(sys.modules.items()):
            if _in_package(name) and getattr(loaded, "__file__", None):
                namespace = dict(vars(loaded))
                self._aliases.update(
                    id(namespace[assigned])
                    for assigned in _top_level_assignments(loaded)
                    if assigned in namespace and typing.get_origin(namespace[assigned]) is not None
                )
        for module in modules:
            for name in module.__all__:
                self.check_object(getattr(module, name), f"{module.__name__}.{name}", name)

    def defined_here(self, obj: object) -> bool:
        """Say whether the package defines `obj`.

        Returns:
            True for a class or function whose `__module__` is a package module, and for a
            typing construct (a union or `Literal`) a package module assigns at its top level.
        """
        if inspect.isclass(obj) or inspect.isroutine(obj):
            return _in_package(getattr(obj, "__module__", None))
        return id(obj) in self._aliases

    def check_object(self, obj: object, where: str, name: str | None = None) -> None:
        """Record `obj` if it is private, under the name the annotation used, and walk it."""
        if isinstance(obj, list | tuple):
            for item in obj:
                self.check_object(item, where)
            return
        if inspect.ismodule(obj) or isinstance(
            obj, TypeVar | ParamSpec | ParamSpecArgs | ParamSpecKwargs
        ):
            return
        if isinstance(obj, Enum):
            obj = type(obj)
        if not self.defined_here(obj):
            for arg in typing.get_args(obj):
                self.check_object(arg, where)
            return
        if id(obj) not in self.public:
            self.findings.add(f"{where}: {name or getattr(obj, '__qualname__', repr(obj))}")
        if id(obj) in self.walked:
            return
        self.walked.add(id(obj))
        where = self.public.get(id(obj), where)
        if inspect.isclass(obj):
            self.walk_class(obj, where)
        elif inspect.isroutine(obj):
            self.check_callable(obj, sys.modules[obj.__module__], where)
        elif typing.get_args(obj):
            self.check_object(typing.get_args(obj), where)
        else:
            self.check_object(type(obj), where)

    def walk_class(self, cls: type, where: str) -> None:
        """Check each member of `cls` a package class defines, inherited ones included."""
        for name in dir(cls):
            owner = next((klass for klass in cls.__mro__ if name in vars(klass)), None)
            if owner is None or not _in_package(owner.__module__):
                continue
            is_dunder = name.startswith("__") and name.endswith("__")
            if name.startswith("_") and not is_dunder:
                continue
            self.check_member(
                vars(owner)[name],
                sys.modules[owner.__module__],
                f"{where}.{name}",
                is_dunder=is_dunder,
            )
        for klass in cls.__mro__:
            if _in_package(klass.__module__):
                for name, annotation in inspect.get_annotations(klass).items():
                    if not name.startswith("_"):
                        self.check_annotation(
                            annotation, sys.modules[klass.__module__], f"{where}.{name}"
                        )

    def check_member(
        self, member: object, module: ModuleType, path: str, *, is_dunder: bool
    ) -> None:
        """Check one member a package class defines: a method, a property, or a value."""
        if isinstance(member, property):
            if member.fget is not None:
                self.check_callable(member.fget, module, path)
        elif isinstance(member, functools.cached_property):
            self.check_callable(member.func, module, path)
        elif isinstance(member, staticmethod | classmethod):
            self.check_callable(member.__func__, module, path)
        elif inspect.isfunction(member):
            self.check_callable(member, module, path)
        elif not is_dunder:
            self.check_object(type(member), path)

    def check_callable(
        self, function: Callable[..., object], module: ModuleType, where: str
    ) -> None:
        """Check the parameters, defaults, and return of a function and of each of its overloads."""
        function = inspect.unwrap(function)
        variants = [function, *typing.get_overloads(function)]
        self.methods[where] = len(variants) - 1
        constructor = where.endswith(".__init__")
        for variant in variants:
            signature = inspect.signature(variant)
            for parameter in signature.parameters.values():
                if parameter.name in {"self", "cls"}:
                    continue
                if constructor and parameter.name.startswith("_"):
                    continue
                self.check_annotation(parameter.annotation, module, f"{where}({parameter.name})")
                if parameter.default is not inspect.Parameter.empty:
                    self.check_object(type(parameter.default), f"{where}({parameter.name}=)")
            self.check_annotation(signature.return_annotation, module, f"{where} ->")

    def check_annotation(self, annotation: object, module: ModuleType, where: str) -> None:
        """Check each name an annotation uses, resolved in the module that wrote it."""
        if annotation is inspect.Parameter.empty:
            return
        if not isinstance(annotation, str):
            self.check_object(annotation, where)
            return
        names = _annotation_namespace(module)
        for node in ast.walk(ast.parse(annotation, mode="eval")):
            if isinstance(node, ast.Name | ast.Attribute):
                self.check_object(_resolve(names, node, where), where, ast.unparse(node))


def _top_level_assignments(module: ModuleType) -> frozenset[str]:
    """Return the names a module assigns at its top level, such as a type alias.

    Returns:
        Each name an assignment statement at the module's top level binds.
    """
    return _assignments_in(inspect.getsource(module))


@functools.cache
def _assignments_in(source: str) -> frozenset[str]:
    """Return the names the top-level assignment statements of `source` bind.

    Returns:
        The names.
    """
    names: set[str] = set()
    for node in ast.parse(source).body:
        targets = (
            node.targets
            if isinstance(node, ast.Assign)
            else [node.target]
            if isinstance(node, ast.AnnAssign)
            else []
        )
        names.update(target.id for target in targets if isinstance(target, ast.Name))
    return frozenset(names)


def _annotation_namespace(module: ModuleType) -> dict[str, object]:
    """Return the names an annotation in `module` may use.

    Returns:
        The builtins, the module's `if TYPE_CHECKING:` imports, and its globals.
    """
    return {**vars(builtins), **_type_checking_namespace(module), **vars(module)}


@functools.cache
def _type_checking_namespace(module: ModuleType) -> dict[str, object]:
    """Return what the module's `if TYPE_CHECKING:` block imports, which run time never binds.

    Returns:
        Each imported name and its object.
    """
    names: dict[str, object] = {}
    for node in ast.parse(inspect.getsource(module)).body:
        if isinstance(node, ast.If) and ast.unparse(node.test) == "TYPE_CHECKING":
            for statement in node.body:
                if isinstance(statement, ast.ImportFrom) and statement.module is not None:
                    source = importlib.import_module(statement.module)
                    for alias in statement.names:
                        names[alias.asname or alias.name] = getattr(source, alias.name)
                elif isinstance(statement, ast.Import):
                    for alias in statement.names:
                        names[alias.asname or alias.name] = importlib.import_module(alias.name)
    return names


def _resolve(names: dict[str, object], node: ast.expr, where: str) -> object:
    """Return what a name or dotted name in an annotation means.

    Returns:
        The object, found through each attribute of a dotted name in turn.

    Raises:
        AssertionError: If nothing defines the name.
    """
    if isinstance(node, ast.Attribute):
        return getattr(_resolve(names, node.value, where), node.attr)
    assert isinstance(node, ast.Name), f"{where}: an annotation holds {ast.unparse(node)!r}"
    assert node.id in names, f"{where}: the annotation names {node.id!r}, which nothing defines"
    return names[node.id]


def _walk_public_api() -> _PublicApiWalk:
    """Walk the public surface of the package as it is.

    Returns:
        The finished walk.
    """
    return _PublicApiWalk(tuple(importlib.import_module(name) for name in _PUBLIC_MODULES))


def test_no_public_signature_names_a_private_type() -> None:
    walk = _walk_public_api()
    assert sorted(walk.findings) == []


def test_the_walk_reaches_every_namespace_and_its_dataset_methods() -> None:
    walk = _walk_public_api()
    assert "deephaven_massive.MassiveSession.partners" in walk.methods
    assert "deephaven_massive.namespaces.Benzinga.ratings" in walk.methods
    assert "deephaven_massive.queries.Query.columns" in walk.methods
    # Overloads are filed under each method's renamed path; the walk still finds them.
    assert walk.methods["deephaven_massive.namespaces.Stocks.bars"] == 2
    assert walk.methods["deephaven_massive.namespaces.Futures.bars"] == 2


class _Hidden:
    """A stand-in for a private class of the package."""


_Hidden.__module__ = "deephaven_massive._session"


def _planted(annotations: dict[str, str], *parameters: str) -> Callable[..., object]:
    """Return a method whose annotations are text resolved in `_session`, as the library's are.

    Returns:
        A method taking `self` and `parameters`, annotated with `annotations`.
    """

    def method(self: object, *args: object) -> object:
        raise NotImplementedError

    method.__signature__ = inspect.Signature(  # type: ignore[attr-defined]  # read by inspect
        [inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        + [
            inspect.Parameter(
                name, inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=annotations[name]
            )
            for name in parameters
        ],
        return_annotation=annotations.get("return", inspect.Signature.empty),
    )
    return method


def _private_default(self: object, value: object = _Hidden()) -> None:
    raise NotImplementedError


@pytest.mark.parametrize(
    ("member", "finding"),
    [
        (property(_planted({"return": "SessionCore"})), "planted ->: SessionCore"),
        (
            functools.cached_property(_planted({"return": "SessionCore"})),
            "planted ->: SessionCore",
        ),
        (_planted({"span": "NormalizedSymbols"}, "span"), "planted(span): NormalizedSymbols"),
        (_planted({"_core": "SessionCore"}, "_core"), "planted(_core): SessionCore"),
        (
            _planted({"return": "planted_tables.session_core.SessionCore"}),
            "planted ->: planted_tables.session_core.SessionCore",
        ),
        (_private_default, "planted(value=): _Hidden"),
        (_Hidden(), "planted: _Hidden"),
    ],
)
def test_the_walk_finds_a_private_type_planted_in_a_public_class(
    monkeypatch: pytest.MonkeyPatch, member: object, finding: str
) -> None:
    session_module = importlib.import_module("deephaven_massive._session")
    alias = importlib.import_module("deephaven_massive._datasets.symbols").NormalizedSymbols
    monkeypatch.setattr(session_module, "NormalizedSymbols", alias, raising=False)
    # A test module that imports a private alias does not make it look foreign.
    monkeypatch.setattr(sys.modules[__name__], "NormalizedSymbols", alias, raising=False)
    tables = importlib.import_module("deephaven_massive._tables")
    monkeypatch.setattr(session_module, "planted_tables", tables, raising=False)
    monkeypatch.setattr(session_module.MassiveSession, "planted", member, raising=False)
    walk = _walk_public_api()
    assert f"deephaven_massive.MassiveSession.{finding}" in walk.findings


def test_a_constructor_skips_its_underscore_parameters() -> None:
    # `_core` of a namespace's constructor is a private `SessionCore`, and nothing is reported.
    walk = _walk_public_api()
    assert "deephaven_massive.namespaces.Stocks.__init__" in walk.methods
    assert walk.findings == set()


def test_the_walk_finds_a_private_type_inside_a_public_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queries = importlib.import_module("deephaven_massive.queries")
    alias = importlib.import_module("deephaven_massive._datasets.symbols").NormalizedSymbols
    monkeypatch.setattr(queries, "Symbols", alias)
    walk = _walk_public_api()
    assert "deephaven_massive.queries.Symbols: SymbolTable" in walk.findings


def test_the_walk_fails_on_a_name_nothing_defines(monkeypatch: pytest.MonkeyPatch) -> None:
    session_module = importlib.import_module("deephaven_massive._session")
    planted = _planted({"return": "NoSuchName"})
    monkeypatch.setattr(session_module.MassiveSession, "planted", planted, raising=False)
    with pytest.raises(AssertionError, match="names 'NoSuchName', which nothing defines"):
        _walk_public_api()
