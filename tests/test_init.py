"""Tests for the public surface of `deephaven_massive`: its names, and the types they declare."""

import ast
import builtins
import functools
import importlib
import importlib.metadata
import inspect
import re
import sys
import types
import typing
from collections.abc import Callable
from enum import Enum, StrEnum
from types import ModuleType
from typing import ParamSpec, ParamSpecArgs, ParamSpecKwargs, TypeVar

import pytest

import deephaven_massive as dm
from deephaven_massive._datasets import dates, filters, symbols

EXPECTED_EXPORTS = [
    "MassiveSession",
    "MassiveConfig",
    "MassiveError",
    "AuthenticationError",
    "EntitlementError",
    "ErrorCause",
    "ALL",
    "AllSymbols",
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
    "canonical_symbol_stock_yahoo",
    "canonical_symbol_stock_nasdaq",
    "canonical_symbol_stock_cms",
    "canonical_symbol_option",
    "canonical_symbol_index",
    "canonical_symbol_forex",
    "canonical_symbol_crypto",
    "canonicalize_symbols",
    "datasets",
    "disable_debug_logging",
    "enable_debug_logging",
    "LogOutput",
    "SymbolsLike",
    "DateLike",
    "FilterValue",
    "__version__",
]


def test_version_is_the_installed_distribution_version() -> None:
    assert dm.__version__ == importlib.metadata.version("deephaven-massive")


def test_all_is_exactly_the_public_api() -> None:
    assert len(EXPECTED_EXPORTS) == 45
    assert sorted(dm.__all__) == sorted(EXPECTED_EXPORTS)


@pytest.mark.parametrize("name", ["TYPE_CHECKING", "annotations", "importlib", "import_module"])
def test_helper_names_are_not_reachable(name: str) -> None:
    with pytest.raises(AttributeError):
        getattr(dm, name)


def test_no_public_names_outside_all() -> None:
    # Nothing beyond the exports is reachable as `dm.<name>`, except the public packages
    # `queries` and `namespaces`, which Python binds here once the library imports them.
    public = {name for name in vars(dm) if not name.startswith("_")}
    assert public - set(dm.__all__) == {"queries", "namespaces"}


_ALIASES = {"SymbolsLike", "DateLike", "FilterValue"}
"""The type aliases of the arguments, which are unions and carry no module of their own."""


def test_public_names_come_from_private_modules() -> None:
    skipped = {"ALL", "__version__", *_ALIASES}
    public = [getattr(dm, name) for name in dm.__all__ if name not in skipped]
    for obj in public:
        assert obj.__module__.startswith("deephaven_massive._"), obj


def test_the_argument_aliases_are_the_ones_the_library_reads() -> None:
    assert dm.SymbolsLike is symbols.SymbolsLike
    assert dm.DateLike is dates.DateLike
    assert dm.FilterValue is filters.FilterValue


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


# The public modules, whose `__all__` is the whole public surface.
_PUBLIC_MODULES = (
    "deephaven_massive",
    "deephaven_massive.queries",
    "deephaven_massive.namespaces",
)

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
    """Walk the declared public contract and record each private type it declares.

    The contract is the public names of the public modules and, for each public class, its
    members, inherited ones included. Every type that contract declares must be public: each
    parameter, return, and property type, each class-level value's and annotated attribute's
    type, each default's type, each type inside an alias, and each exception a `Raises:`
    section documents. A type is private when the package defines it and no public module
    exports it: a class or function whose `__module__` is a package module, or an alias a
    package module assigns at its top level. A class a declaration names is walked in turn, so
    a sub-namespace returned by a property is checked like a top-level one.

    An implementation detail may stay private: a private base class (its members are walked as
    the public class's own), and the runtime class of a value declared as a public type.
    Nothing else is exempt: a constructor's parameters are checked like any method's.
    """

    def __init__(self, modules: tuple[ModuleType, ...]) -> None:
        self.public: dict[int, str] = {}
        self.exports: dict[str, object] = {}
        self.exports_by_name: dict[str, object] = {}
        for module in modules:
            for name in module.__all__:
                value = getattr(module, name)
                self.public.setdefault(id(value), f"{module.__name__}.{name}")
                self.exports[f"{module.__name__}.{name}"] = value
                self.exports_by_name[name] = value
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
        for variant in variants:
            signature = inspect.signature(variant)
            for parameter in signature.parameters.values():
                if parameter.name in {"self", "cls"}:
                    continue
                self.check_annotation(parameter.annotation, module, f"{where}({parameter.name})")
                if parameter.default is not inspect.Parameter.empty:
                    self.check_object(type(parameter.default), f"{where}({parameter.name}=)")
            self.check_annotation(signature.return_annotation, module, f"{where} ->")
        self.check_raises(function, where)

    def check_raises(self, function: Callable[..., object], where: str) -> None:
        """Check that each exception a `Raises:` section documents is a builtin or public one."""
        for name in _documented_raises(function):
            module, _, short = name.rpartition(".")
            if module:
                # A path names the public module it comes from; `dm` is the package's alias.
                module = "deephaven_massive" if module == "dm" else module
                found = self.exports.get(f"{module}.{short}")
            else:
                found = self.exports_by_name.get(name, getattr(builtins, name, None))
            if not (isinstance(found, type) and issubclass(found, BaseException)):
                self.findings.add(f"{where} raises: {name}")

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


_RAISES_ENTRY = re.compile(r"^    ([A-Za-z_][\w.]*):")


def _documented_raises(function: Callable[..., object]) -> frozenset[str]:
    """Return the exception names a function's Google `Raises:` section lists.

    Returns:
        Each name as written, such as `MassiveError` or `dm.MassiveError`.
    """
    names: set[str] = set()
    inside = False
    for line in (inspect.getdoc(function) or "").splitlines():
        if line == "Raises:":
            inside = True
        elif inside and line and not line.startswith(" "):
            inside = False
        elif inside and (entry := _RAISES_ENTRY.match(line)) is not None:
            names.add(entry.group(1))
    return frozenset(names)


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
    assert "deephaven_massive.namespaces.PartnersBenzingaNamespace.ratings" in walk.methods
    assert "deephaven_massive.queries.Query.columns" in walk.methods
    assert "deephaven_massive.namespaces.StocksNamespace.bars" in walk.methods
    assert "deephaven_massive.namespaces.FuturesNamespace.bars" in walk.methods


class _Hidden:
    """A stand-in for a private class of the package."""


_Hidden.__module__ = "deephaven_massive._session"


def _takes_nothing(self: object) -> object:
    raise NotImplementedError


def _takes_span(self: object, span: object) -> object:
    raise NotImplementedError


def _takes_core(self: object, _core: object) -> object:
    raise NotImplementedError


def _planted(shape: Callable[..., object], annotations: dict[str, str]) -> Callable[..., object]:
    """Return a copy of `shape` annotated with text resolved in `_session`, as the library's are.

    Args:
        shape: A function with the parameters the planted method takes.
        annotations: The annotation text of each parameter and of `"return"`.

    Returns:
        A new function with `shape`'s code and the given annotations.
    """
    planted = types.FunctionType(shape.__code__, shape.__globals__, "planted")
    planted.__annotations__ = annotations
    return planted


def _private_default(self: object, value: object = _Hidden()) -> None:
    raise NotImplementedError


def _documents_a_private_error(self: object) -> None:
    """Fail.

    Raises:
        ValueError: Never; a builtin is public.
        dm.MassiveError: Never; a public type, by its public path.
        RowDataError: Always; a private type.
    """
    raise NotImplementedError


def _documents_a_value(self: object) -> None:
    """Fail.

    Raises:
        dm.ALL: Never; a public value, not an exception.
    """
    raise NotImplementedError


def _documents_a_private_path(self: object) -> None:
    """Fail.

    Raises:
        deephaven_massive._errors.MassiveError: Never; a public type, but by a private path.
    """
    raise NotImplementedError


class _Stocks:
    """A stand-in for the private namespace class `m.stocks` once declared."""


_Stocks.__module__ = "deephaven_massive._session"


@pytest.mark.parametrize(
    ("member", "finding"),
    [
        (property(_planted(_takes_nothing, {"return": "SessionCore"})), "planted ->: SessionCore"),
        (
            functools.cached_property(_planted(_takes_nothing, {"return": "SessionCore"})),
            "planted ->: SessionCore",
        ),
        (_planted(_takes_span, {"span": "NormalizedSymbols"}), "planted(span): NormalizedSymbols"),
        (_planted(_takes_core, {"_core": "SessionCore"}), "planted(_core): SessionCore"),
        (
            _planted(_takes_nothing, {"return": "planted_tables.session_core.SessionCore"}),
            "planted ->: planted_tables.session_core.SessionCore",
        ),
        (_private_default, "planted(value=): _Hidden"),
        (property(_planted(_takes_nothing, {"return": "_Stocks"})), "planted ->: _Stocks"),
        (_documents_a_private_error, "planted raises: RowDataError"),
        (_documents_a_value, "planted raises: dm.ALL"),
        (
            _documents_a_private_path,
            "planted raises: deephaven_massive._errors.MassiveError",
        ),
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
    monkeypatch.setattr(session_module, "_Stocks", _Stocks, raising=False)
    monkeypatch.setattr(session_module.MassiveSession, "planted", member, raising=False)
    walk = _walk_public_api()
    assert f"deephaven_massive.MassiveSession.{finding}" in walk.findings
    assert {found for found in walk.findings if ".planted" not in found} == set()


def test_a_namespace_constructor_declares_only_public_types() -> None:
    # A namespace takes the public `MassiveSession` it belongs to.
    walk = _walk_public_api()
    assert "deephaven_massive.namespaces.StocksNamespace.__init__" in walk.methods
    assert walk.findings == set()


def test_the_walk_finds_a_private_type_inside_a_public_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alias = importlib.import_module("deephaven_massive._datasets.symbols").NormalizedSymbols
    monkeypatch.setattr(dm, "SymbolsLike", alias)
    walk = _walk_public_api()
    assert "deephaven_massive.SymbolsLike: SymbolTable" in walk.findings


def test_the_walk_fails_on_a_name_nothing_defines(monkeypatch: pytest.MonkeyPatch) -> None:
    session_module = importlib.import_module("deephaven_massive._session")
    planted = _planted(_takes_nothing, {"return": "NoSuchName"})
    monkeypatch.setattr(session_module.MassiveSession, "planted", planted, raising=False)
    with pytest.raises(AssertionError, match="names 'NoSuchName', which nothing defines"):
        _walk_public_api()
