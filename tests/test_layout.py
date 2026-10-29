"""Enforce the test layout: `tests/` mirrors `src/deephaven_massive/` one module to one file.

The module `<path>/<name>.py` is tested by `tests/<path>/test_<name>.py`, where every path
component drops one leading underscore: `_namespaces/stocks.py` is tested by
`tests/namespaces/test_stocks.py`, and `_massive/rest/client.py` by
`tests/massive/rest/test_client.py`. A dunder module keeps only its bare name, so a package's
`__init__.py` is mirrored by `tests/<path>/test_init.py` and a `__main__.py` by
`tests/<path>/test_main.py`. Live tests add the suffix `_integration` to the mirror's stem.

A directory directly under `tests/` whose name starts with `_` (such as `tests/_fakes/`) is a
helper package outside the mirror rule, as long as it holds no test modules: pytest collects a
test module wherever it lies, so one inside a helper package must still mirror a source module.
The only other test files that mirror no module are `conftest.py`, this file, and
`test_readme.py`, all at the top of `tests/`. Fixtures under `tests/data/` follow the test
paths, so none of their directories starts with `_` either.

The file also enforces the boundary of the wire package `deephaven_massive._massive`, by reading
the imports of every module with `ast`:

- Inside `_massive`, a module imports only the standard library, the wire libraries
  (`urllib3`, `websockets`, `certifi`, `orjson`, `boto3`, `botocore`, `pyarrow`), and other
  `_massive` modules; never Deephaven, `jpy`, or the rest of the library. The exceptions are
  `typing_extensions` anywhere and `types_boto3_s3` in `flatfiles`, inside
  `if TYPE_CHECKING:`, which never runs.
- Its subpackages are layered: `net` and `spec` import nothing else in `_massive`; `rest`,
  `ws`, and `flatfiles` import only `net` and `spec`; `refresh` may import anything, and only
  `refresh` and its tests import `refresh`.
- `tests/massive/`, the wire package's own tests, may import any `_massive` module.
- Everything else, the rest of `tests/` included, sees only the interface: it imports no
  `_massive` submodule, reads only names in `_massive.__all__` (by `.` or a literal
  `getattr`, through any alias of the package or of the root package), imports none of
  `urllib3`, `websockets`, `certifi`, `boto3`, or `botocore` (`tests/_fakes/` excepted), and
  names no file of the vendored spec snapshot (`vendor/massive/`, outside the package, which
  only the refresh reads).

A module named in a string counts as imported, since that is the form every dynamic import
(`importlib.import_module` under any alias, `__import__`), patch (`mock.patch`,
`monkeypatch.setattr`), and `sys.modules` lookup takes. A string with a leading dot (such as
the file extension `".csv.gz"`) is not a module name; a relative name counts only as the first
argument of `import_module` with a literal package, and resolves against that package. Inside
`_massive`, a module outside the six known subpackages is itself a violation, so a new module
cannot escape the layers.
This file is exempt from the rules on strings, since it names what it forbids. A bare string
naming a network library outside `_massive` (a logger name such as `"urllib3"` passed to
`caplog.at_level`, say) is flagged on purpose: details of those libraries belong inside the
wire package.

Last, the file enforces the library's layers. Every module under `src/` is in one of six
layers, lowest first: wire (`_massive/`), vocabulary (the root `_config`, `_enums`, `_errors`,
`_types`), datasets (`_datasets/`), tables (`_tables/`), ingest (`_ingest/`), and API (the root
`__init__`, `_session`, `_query`, and `_namespaces/`); a module in no known layer is itself a
violation. Then:

- every import, relative import, resolvable literal dynamic import, and module-name string,
  inside `if TYPE_CHECKING:` or not, targets the module's own layer or a lower one;
- outside the two lazy `__init__.py` files (the package's and `_massive`'s), every import the
  rules cannot resolve is a violation: a computed name, a relative literal without a literal
  package, an `__import__` level other than 0, an import function used other than by a direct
  call, and a `sys.modules` lookup by a computed key;
- Deephaven: the vocabulary imports only `deephaven.table` (the `Table` of `Symbols`), the
  datasets import `deephaven` (and the vocabulary's run-time `Table`) only for type checking,
  and nothing imports `deephaven_server`
  (the wire boundary above already keeps Deephaven out of `_massive`);
- the vocabulary and the datasets import no I/O or thread module and read neither
  `threading.Thread` nor `threading.Timer`;
- `_massive.__all__` splits into vocabulary names and client names, and only the ingest layer
  uses a client name, star-imports `_massive`, or reads it by a computed `getattr`;
- in the API, only `_session.py` imports the ingest layer, and only `_ingest.clients`;
- the `__init__.py` of `_namespaces`, `_datasets`, `_tables`, and `_ingest` holds a docstring
  and nothing else.

The tests are outside the layers: they may import any module and keep only the wire interface
rule.

Each rule is a pure function over relative paths (and, for the boundary and the layers, source
text), so it is checked both against the real tree and against a synthetic tree that breaks it.
"""

import ast
import functools
import json
import re
import subprocess
import sys
import tomllib
from collections import Counter
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_SRC = _ROOT / "src" / "deephaven_massive"
_TESTS = _ROOT / "tests"
_DATA = _TESTS / "data"

# Test files that mirror no source module. A new entry needs owner approval.
APPROVED_EXCEPTIONS = frozenset(
    {
        PurePosixPath("conftest.py"),  # embedded JVM and fixtures
        PurePosixPath("test_layout.py"),  # this file
        PurePosixPath("test_readme.py"),  # mirrors README.md
    }
)

_INTEGRATION_SUFFIX = "_integration"


def _mirror_name(name: str) -> str:
    """Return a path component's mirrored name.

    Args:
        name: A directory name or module stem.

    Returns:
        The bare name of a dunder (`__init__` gives `init`); otherwise the name without one
        leading underscore.
    """
    # `__` alone, or `____`, has no name between its underscores, so it is not a dunder.
    if len(name) > 4 and name.startswith("__") and name.endswith("__"):
        return name[2:-2]
    return name.removeprefix("_")


def mirror_of(module: PurePosixPath) -> PurePosixPath:
    """Return the unit-test path, relative to `tests/`, that mirrors a source module.

    Args:
        module: The module's path relative to `src/deephaven_massive/`.

    Returns:
        The mirrored test file's path relative to `tests/`.
    """
    directories = [_mirror_name(part) for part in module.parent.parts]
    return PurePosixPath(*directories, f"test_{_mirror_name(module.stem)}.py")


def unit_test_of(test_file: PurePosixPath) -> PurePosixPath:
    """Return the unit-test path a test file stands for, mapping live tests to their mirror.

    Args:
        test_file: A test file's path relative to `tests/`.

    Returns:
        The path itself for a unit test; for `test_<name>_integration.py`, `test_<name>.py`.
    """
    stem = test_file.stem.removesuffix(_INTEGRATION_SUFFIX)
    return test_file.with_name(f"{stem}.py")


def is_helper(test_file: PurePosixPath) -> bool:
    """Return whether a file is part of a helper package, outside the mirror rule.

    Args:
        test_file: A Python file's path relative to `tests/`.

    Returns:
        True for a file inside a directory directly under `tests/` whose name starts with `_`,
        unless pytest would treat it as a test module or a conftest (`test_*.py`,
        `*_test.py`, `conftest.py`).
    """
    return (
        len(test_file.parts) > 1
        and test_file.parts[0].startswith("_")
        and test_file.name != "conftest.py"
        and not test_file.name.startswith("test_")
        and not test_file.stem.endswith("_test")
    )


def public_modules(modules: Iterable[PurePosixPath]) -> list[str]:
    """Return the modules that break the privacy rule.

    Args:
        modules: Module paths relative to `src/deephaven_massive/`.

    Returns:
        Every module other than the package `__init__.py` whose first path component has no
        leading underscore.
    """
    return [
        str(module)
        for module in modules
        if module != PurePosixPath("__init__.py") and not module.parts[0].startswith("_")
    ]


def missing_mirrors(
    modules: Iterable[PurePosixPath], test_files: Iterable[PurePosixPath]
) -> list[str]:
    """Return the modules whose mirrored unit test does not exist.

    Args:
        modules: Module paths relative to `src/deephaven_massive/`.
        test_files: Test file paths relative to `tests/`.

    Returns:
        One `"<module> -> tests/<mirror>"` entry per module without its mirror.
    """
    present = set(test_files)
    return [
        f"{module} -> tests/{mirror_of(module)}"
        for module in modules
        if mirror_of(module) not in present
    ]


def shared_mirrors(modules: Iterable[PurePosixPath]) -> list[str]:
    """Return the mirrors claimed by more than one module.

    Args:
        modules: Module paths relative to `src/deephaven_massive/`.

    Returns:
        The mirrored test paths that two or more modules map to, sorted.
    """
    claimed = Counter(mirror_of(module) for module in modules)
    return sorted(f"tests/{mirror}" for mirror, count in claimed.items() if count > 1)


def orphans(modules: Iterable[PurePosixPath], test_files: Iterable[PurePosixPath]) -> list[str]:
    """Return the test files that mirror no module and are not allowed exceptions.

    Args:
        modules: Module paths relative to `src/deephaven_massive/`.
        test_files: Python file paths relative to `tests/`.

    Returns:
        One `"tests/<path>"` entry per file that is neither a helper, an approved exception,
        nor the mirror (unit or live) of a module.
    """
    mirrors = {mirror_of(module) for module in modules}
    return [
        f"tests/{test_file}"
        for test_file in test_files
        if not is_helper(test_file)
        and test_file not in APPROVED_EXCEPTIONS
        and unit_test_of(test_file) not in mirrors
    ]


def underscored_fixture_dirs(fixtures: Iterable[PurePosixPath]) -> list[str]:
    """Return the fixture directories whose name starts with `_`.

    Fixture directories follow the test paths, which never start a component with `_`.

    Args:
        fixtures: Fixture file paths relative to `tests/data/`.

    Returns:
        The offending directories, as `"tests/data/<path>/"`, sorted and without duplicates.
    """
    found = set()
    for fixture in fixtures:
        parents = fixture.parent.parts
        for depth, part in enumerate(parents, start=1):
            if part.startswith("_"):
                found.add(f"tests/data/{PurePosixPath(*parents[:depth])}/")
    return sorted(found)


def _paths(*paths: str) -> list[PurePosixPath]:
    return [PurePosixPath(path) for path in paths]


def _source_modules() -> list[PurePosixPath]:
    return sorted(PurePosixPath(path.relative_to(_SRC).as_posix()) for path in _SRC.rglob("*.py"))


def _test_files() -> list[PurePosixPath]:
    return sorted(
        PurePosixPath(path.relative_to(_TESTS).as_posix()) for path in _TESTS.rglob("*.py")
    )


def _fixtures() -> list[PurePosixPath]:
    return sorted(
        PurePosixPath(path.relative_to(_DATA).as_posix())
        for path in _DATA.rglob("*")
        if path.is_file()
    )


# The naming rule.


@pytest.mark.parametrize(
    ("module", "expected"),
    [
        ("_config.py", "test_config.py"),
        ("_datasets/catalog.py", "datasets/test_catalog.py"),
        ("_ingest/hosts.py", "ingest/test_hosts.py"),
        ("_namespaces/stocks.py", "namespaces/test_stocks.py"),
        ("_massive/rest/client.py", "massive/rest/test_client.py"),
        ("_massive/rest/_paging.py", "massive/rest/test_paging.py"),
    ],
)
def test_mirror_of_drops_one_leading_underscore_per_component(module: str, expected: str) -> None:
    assert mirror_of(PurePosixPath(module)) == PurePosixPath(expected)


# The dunder rule.


@pytest.mark.parametrize(
    ("module", "expected"),
    [
        ("__init__.py", "test_init.py"),
        ("_namespaces/__init__.py", "namespaces/test_init.py"),
        ("_massive/__init__.py", "massive/test_init.py"),
        ("_massive/refresh/__main__.py", "massive/refresh/test_main.py"),
    ],
)
def test_mirror_of_maps_a_dunder_module_to_its_bare_name(module: str, expected: str) -> None:
    assert mirror_of(PurePosixPath(module)) == PurePosixPath(expected)


# Live tests.


@pytest.mark.parametrize(
    ("test_file", "expected"),
    [
        ("test_config.py", "test_config.py"),
        ("ingest/test_hosts_integration.py", "ingest/test_hosts.py"),
        ("namespaces/test_stocks_integration.py", "namespaces/test_stocks.py"),
    ],
)
def test_unit_test_of_maps_live_tests_to_their_mirror(test_file: str, expected: str) -> None:
    assert unit_test_of(PurePosixPath(test_file)) == PurePosixPath(expected)


def test_a_live_test_without_a_module_is_an_orphan() -> None:
    found = orphans(_paths("_ingest/hosts.py"), _paths("ingest/test_ws_integration.py"))
    assert found == ["tests/ingest/test_ws_integration.py"]


# The privacy rule.


def test_every_module_below_the_package_is_private() -> None:
    # The first path component below `deephaven_massive/` carries a leading underscore, so the
    # public surface is `__init__.py` alone.
    assert public_modules(_source_modules()) == []


def test_a_public_module_is_reported() -> None:
    found = public_modules(_paths("__init__.py", "_config.py", "config.py", "namespaces/stocks.py"))
    assert found == ["config.py", "namespaces/stocks.py"]


# Every module has its mirror.


def test_every_source_module_has_its_mirrored_test() -> None:
    assert missing_mirrors(_source_modules(), _test_files()) == []


@pytest.mark.parametrize(
    ("module", "wrong_test", "mirror"),
    [
        # The naming rule: no component keeps its underscore.
        ("_namespaces/stocks.py", "_namespaces/test_stocks.py", "namespaces/test_stocks.py"),
        ("_config.py", "test__config.py", "test_config.py"),
        # The dunder rule: a dunder module keeps only its bare name.
        ("_massive/__init__.py", "massive/test___init__.py", "massive/test_init.py"),
        (
            "_massive/refresh/__main__.py",
            "massive/refresh/test___main__.py",
            "massive/refresh/test_main.py",
        ),
    ],
)
def test_a_module_without_its_mirror_is_reported(module: str, wrong_test: str, mirror: str) -> None:
    found = missing_mirrors(_paths(module), _paths(wrong_test))
    assert found == [f"{module} -> tests/{mirror}"]


def test_no_two_source_modules_share_a_mirror() -> None:
    assert shared_mirrors(_source_modules()) == []


def test_two_modules_with_one_mirror_are_reported() -> None:
    # A package's `__init__.py` and a sibling `_init.py` both map to `test_init.py`.
    modules = _paths("_massive/__init__.py", "_massive/_init.py", "_massive/client.py")
    assert shared_mirrors(modules) == ["tests/massive/test_init.py"]


# Every test file mirrors a module or is an allowed exception.


def test_every_test_file_mirrors_a_source_module() -> None:
    assert orphans(_source_modules(), _test_files()) == []


def test_a_test_file_without_a_module_is_an_orphan() -> None:
    found = orphans(_paths("_config.py"), _paths("test_config.py", "ingest/test_hosts.py"))
    assert found == ["tests/ingest/test_hosts.py"]


def test_test_catalog_is_a_plain_mirror() -> None:
    catalog_test = _paths("datasets/test_catalog.py")
    assert orphans(_paths("_datasets/catalog.py"), catalog_test) == []
    assert orphans(_paths("_config.py"), catalog_test) == ["tests/datasets/test_catalog.py"]


def test_the_approved_exceptions_need_no_module() -> None:
    test_files = _paths("conftest.py", "test_layout.py", "test_readme.py")
    assert orphans([], test_files) == []


def test_an_approved_exception_counts_only_at_the_top_of_tests() -> None:
    test_files = _paths("namespaces/conftest.py", "namespaces/test_readme.py")
    assert orphans([], test_files) == [
        "tests/namespaces/conftest.py",
        "tests/namespaces/test_readme.py",
    ]


# Helper packages.


def test_an_underscore_directory_under_tests_is_a_helper_package() -> None:
    assert orphans([], _paths("_fakes/__init__.py", "_fakes/rest.py", "_fakes/ws/server.py")) == []


@pytest.mark.parametrize(
    "test_file",
    [
        # A test module inside a helper package is collected, so it must mirror a module.
        "_namespaces/test_stocks.py",
        "_fakes/test_rest.py",
        "_fakes/rest_test.py",
        "_fakes/conftest.py",
        # Only a directory directly under `tests/` can be a helper package.
        "namespaces/_helpers/base.py",
        # A file is not a directory.
        "_util.py",
        # A directory without an underscore is mirrored.
        "fakes/rest.py",
    ],
)
def test_a_file_outside_a_helper_package_is_an_orphan(test_file: str) -> None:
    assert orphans([], _paths(test_file)) == [f"tests/{test_file}"]


# Fixture directories.


def test_no_fixture_directory_starts_with_an_underscore() -> None:
    assert underscored_fixture_dirs(_fixtures()) == []


def test_an_underscored_fixture_directory_is_reported() -> None:
    fixtures = _paths(
        "datasets/symbols/crypto_tickers.csv",
        "_flatfiles/a.csv.gz",
        "_flatfiles/b.csv.gz",
        "massive/_rest/page.json",
        "top.json",
    )
    assert underscored_fixture_dirs(fixtures) == [
        "tests/data/_flatfiles/",
        "tests/data/massive/_rest/",
    ]


# ---------------------------------------------------------------------------------------------
# The boundary of the wire package.
# ---------------------------------------------------------------------------------------------

_PACKAGE = "deephaven_massive"
_WIRE = "deephaven_massive._massive"

# What `_massive` may import besides the standard library and itself.
_WIRE_LIBRARIES = frozenset(
    {"urllib3", "websockets", "certifi", "orjson", "boto3", "botocore", "pyarrow"}
)
# The network libraries nothing outside `_massive` imports (`tests/_fakes/` excepted).
_NETWORK_LIBRARIES = frozenset({"urllib3", "websockets", "certifi", "boto3", "botocore"})

# Each layer of `_massive`, and the layers it may import (`refresh` may import any).
_WIRE_SUBPACKAGES = {
    "net": frozenset({"net"}),
    "spec": frozenset({"spec"}),
    "rest": frozenset({"rest", "net", "spec"}),
    "ws": frozenset({"ws", "net", "spec"}),
    "flatfiles": frozenset({"flatfiles", "net", "spec"}),
}
_REFRESH = "refresh"
# The libraries `_massive` may import for type checking only, and the layers that may: any
# layer for `typing_extensions` (`override` on Python 3.11), only `flatfiles` for the boto3
# stubs' `S3Client`.
_TYPE_ONLY_LIBRARIES: dict[str, frozenset[str] | None] = {
    "typing_extensions": None,
    "types_boto3_s3": frozenset({"flatfiles"}),
}

# The vendor files of the spec snapshot, and its directory (outside the package, at the top of
# the checkout).
_SNAPSHOT_FILES = frozenset({"openapi.json", "websocket.json", "llms.txt", "manifest.json"})
_SNAPSHOT_DIRECTORY = "vendor/massive"

# A string that may name a module: dotted identifiers, optionally relative.
_DOTTED_NAME = re.compile(r"\.*[A-Za-z_]\w*(\.[A-Za-z_]\w*)*")
# The top-level packages whose names in strings the rules check.
_NAMED_TOPS = frozenset({_PACKAGE, "deephaven", "deephaven_server", "jpy", *_NETWORK_LIBRARIES})


def _module_of(path: PurePosixPath) -> tuple[str, str]:
    """Return the dotted module name of a repository file and the package relative imports use.

    Args:
        path: A Python file's path relative to the repository root, under `src/` or `tests/`.

    Returns:
        The module name (`deephaven_massive._massive.net` for
        `src/deephaven_massive/_massive/net/__init__.py`, `tests.test_layout` for this file)
        and its package: the module itself for an `__init__.py`, otherwise its parent.
    """
    parts = path.parts[1:] if path.parts[0] == "src" else path.parts
    package = ".".join(parts[:-1])
    if path.name == "__init__.py":
        return package, package
    return f"{package}.{path.stem}", package


def _absolute(package: str, level: int, name: str | None) -> str:
    """Resolve a relative import to an absolute module name.

    Args:
        package: The importing module's package.
        level: The number of leading dots.
        name: The text after the dots, or `None` for `from . import x`.

    Returns:
        The absolute name the import refers to.
    """
    base = package.split(".")
    base = base[: len(base) - (level - 1)]
    return ".".join([*base, name] if name else base)


def _imports(tree: ast.Module, package: str) -> list[tuple[int, str]]:
    """Return every module a file imports by statement, as absolute names with their line.

    A `from a import b` counts as importing `a.b`, since `b` may be a submodule, and so does
    `__import__("a", fromlist=["b"])` with literal names. A relative name passed to
    `import_module` with a literal package (`import_module("._massive.net", "deephaven_massive")`
    or `package=`) counts as importing the module it resolves to. Every other dynamic import
    names its module in a string, which `_module_strings` reports.

    Args:
        tree: The file's syntax tree.
        package: The file's package, to resolve relative imports.

    Returns:
        `(line, name)` pairs in the order of the walk.
    """
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                base = _absolute(package, node.level, node.module)
            found.extend(
                (node.lineno, base if alias.name == "*" else f"{base}.{alias.name}")
                for alias in node.names
            )
        elif isinstance(node, ast.Call) and ast.unparse(node.func) == "__import__" and node.args:
            found.extend((node.lineno, name) for name in _fromlist_imports(node))
        elif isinstance(node, ast.Call) and (relative := _relative_import(node)) is not None:
            found.append((node.lineno, relative))
    return found


def _relative_import(call: ast.Call) -> str | None:
    """Resolve `import_module("<.relative>", <package>)` with literal arguments.

    Args:
        call: A call expression.

    Returns:
        The absolute module name, or `None` when the call is not `import_module` (by any
        attribute path, such as `importlib.import_module`) or its name or package is not a
        literal, or the name is not relative.
    """
    if ast.unparse(call.func).split(".")[-1] != "import_module" or not call.args:
        return None
    name = call.args[0]
    package = next((k.value for k in call.keywords if k.arg == "package"), None)
    if package is None and len(call.args) > 1:
        package = call.args[1]
    if not (
        isinstance(name, ast.Constant)
        and isinstance(name.value, str)
        and name.value.startswith(".")
        and isinstance(package, ast.Constant)
        and isinstance(package.value, str)
    ):
        return None
    stripped = name.value.lstrip(".")
    return _absolute(package.value, len(name.value) - len(stripped), stripped or None)


def _fromlist_imports(call: ast.Call) -> list[str]:
    """Return the `base.member` names an `__import__(base, ..., fromlist)` call imports.

    Args:
        call: The call, whose first argument is the base.

    Returns:
        One name per literal string in a literal `fromlist`; empty when the base or the list is
        not literal.
    """
    base = call.args[0]
    fromlist = next((k.value for k in call.keywords if k.arg == "fromlist"), None)
    if fromlist is None and len(call.args) > 3:  # fromlist is the fourth argument
        fromlist = call.args[3]
    if not (
        isinstance(base, ast.Constant)
        and isinstance(base.value, str)
        and isinstance(fromlist, ast.List | ast.Tuple)
    ):
        return []
    return [
        f"{base.value}.{element.value}"
        for element in fromlist.elts
        if isinstance(element, ast.Constant) and isinstance(element.value, str)
    ]


def _docstrings(tree: ast.Module) -> set[int]:
    """Return the ids of the string constants that are docstrings or other bare prose.

    Args:
        tree: The file's syntax tree.

    Returns:
        The `id()` of each constant that is a whole expression statement.
    """
    return {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
    }


def _module_strings(tree: ast.Module) -> list[tuple[int, str]]:
    """Return the string constants that name a module the boundary rules care about.

    A module named in a string can be imported (`importlib.import_module`, `__import__`, under
    any alias or keyword), patched (`mock.patch`, `monkeypatch.setattr`), or looked up
    (`sys.modules`), so the rules treat such a string as an import. A string that starts with
    a dot is not a module name on its own (`".csv.gz"` is a file extension): it names a module
    only as the relative argument of `import_module`, which `_imports` resolves.

    Args:
        tree: The file's syntax tree.

    Returns:
        `(line, name)` for each non-docstring constant that is a dotted name in this package,
        in Deephaven, or in a network library.
    """
    skipped = _docstrings(tree)
    found = []
    for node in ast.walk(tree):
        if (
            not isinstance(node, ast.Constant)
            or not isinstance(node.value, str)
            or id(node) in skipped
            or node.value.startswith(".")
            or not _DOTTED_NAME.fullmatch(node.value)
        ):
            continue
        name = node.value
        if name.split(".")[0] in _NAMED_TOPS:
            found.append((node.lineno, name))
    return found


def _wire_aliases(tree: ast.Module, package: str) -> set[str]:
    """Return the expressions that name the `_massive` package in a file.

    Args:
        tree: The file's syntax tree.
        package: The file's package, to resolve relative imports.

    Returns:
        The full dotted name; every name an import binds to the package
        (`import deephaven_massive._massive as wire`, `from deephaven_massive import _massive`);
        and `<root>._massive` for every name bound to the root package
        (`import deephaven_massive as dm` gives `dm._massive`).
    """
    aliases = {_WIRE}
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            aliases.update(a.asname for a in node.names if a.name == _WIRE and a.asname)
            roots.update(a.asname or a.name for a in node.names if a.name == _PACKAGE)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                base = _absolute(package, node.level, node.module)
            aliases.update(a.asname or a.name for a in node.names if f"{base}.{a.name}" == _WIRE)
    return aliases | {f"{root}._massive" for root in roots}


def _wire_reads(tree: ast.Module, aliases: set[str]) -> list[tuple[int, str]]:
    """Return every attribute read on the `_massive` package, by `.` or by literal `getattr`.

    Args:
        tree: The file's syntax tree.
        aliases: The expressions that name the package, from `_wire_aliases`.

    Returns:
        `(line, attribute)` pairs.
    """
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and ast.unparse(node.value) in aliases:
            found.append((node.lineno, node.attr))
        elif isinstance(node, ast.Call) and (name := _getattr_name(node, aliases)) is not None:
            found.append((node.lineno, name))
    return found


def _getattr_name(call: ast.Call, aliases: set[str]) -> str | None:
    """Return the attribute a literal `getattr(package, "name"[, default])` call reads.

    Args:
        call: A call expression.
        aliases: The expressions that name the `_massive` package, from `_wire_aliases`.

    Returns:
        The attribute name, or None unless the call is `getattr` on the package with a
        literal name.
    """
    # getattr(object, name[, default])
    if ast.unparse(call.func) != "getattr" or len(call.args) < 2:
        return None
    target, name = call.args[0], call.args[1]
    if ast.unparse(target) not in aliases:
        return None
    return name.value if isinstance(name, ast.Constant) and isinstance(name.value, str) else None


def _snapshot_names(tree: ast.Module) -> list[tuple[int, str]]:
    """Return the string constants that name a file of the spec snapshot.

    Docstrings are prose, not references to a file, so they are skipped.

    Args:
        tree: The file's syntax tree.

    Returns:
        `(line, text)` for each constant that contains the snapshot directory or whose last
        path component is a snapshot file.
    """
    skipped = _docstrings(tree)
    return [
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in skipped
        and (
            _SNAPSHOT_DIRECTORY in node.value
            or PurePosixPath(node.value.strip()).name in _SNAPSHOT_FILES
        )
    ]


def _type_checking_lines(tree: ast.Module) -> set[int]:
    """Return the lines of the imports inside `if TYPE_CHECKING:` blocks.

    Args:
        tree: The file's syntax tree.

    Returns:
        The line of each `import` or `from ... import` in the body (not the `else`) of an
        `if TYPE_CHECKING:` statement.
    """
    return {
        statement.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and ast.unparse(node.test) in {"TYPE_CHECKING", "typing.TYPE_CHECKING"}
        for statement in node.body
        if isinstance(statement, ast.Import | ast.ImportFrom)
    }


def _wire_layer(name: str) -> str | None:
    """Return the `_massive` subpackage a module name falls in.

    Args:
        name: An absolute module name inside `_massive`.

    Returns:
        The subpackage (`net`, `spec`, `rest`, `ws`, `flatfiles`, or `refresh`), or `None` for
        the package itself and the names of its interface.
    """
    parts = name.removeprefix(f"{_WIRE}.").split(".") if name != _WIRE else []
    return parts[0] if parts and parts[0] in {*_WIRE_SUBPACKAGES, _REFRESH} else None


def _inside(name: str, package: str) -> bool:
    return name == package or name.startswith(f"{package}.")


def _wire_rules(where: str, tree: ast.Module, *, module: str, package: str) -> list[str]:
    """Check a module inside `_massive` against its allow list and its layer.

    Args:
        where: The file's path, for the messages.
        tree: The file's syntax tree.
        module: The file's module name.
        package: The file's package, to resolve relative imports.

    Returns:
        One message per import or module name the rules forbid. A module outside the known
        subpackages and the package's own `__init__.py` is itself a violation, so a new
        module cannot escape the layers.
    """
    layer = _wire_layer(module) if module != _WIRE else None
    if module != _WIRE and layer is None:
        return [f"{where}:1: not in a known layer of the wire package: {module}"]
    type_only = _type_checking_lines(tree)
    found = []
    for line, name in [*_imports(tree, package), *_module_strings(tree)]:
        top = name.split(".")[0]
        if top in _TYPE_ONLY_LIBRARIES and line in type_only:
            layers = _TYPE_ONLY_LIBRARIES[top]
            if layers is None or layer in layers:
                continue
        if _inside(name, _WIRE):
            target = _wire_layer(name)
            if target == _REFRESH and layer != _REFRESH:
                found.append(f"{where}:{line}: only refresh imports refresh: {name}")
            elif layer in _WIRE_SUBPACKAGES and target not in _WIRE_SUBPACKAGES[layer]:
                found.append(f"{where}:{line}: {layer} must not import {name}")
        elif top not in sys.stdlib_module_names and top not in _WIRE_LIBRARIES:
            found.append(f"{where}:{line}: the wire package must not import {name}")
    return found


def _outside_rules(
    where: str,
    tree: ast.Module,
    package: str,
    *,
    interface: frozenset[str],
    network_allowed: bool,
    strings_checked: bool,
) -> list[str]:
    """Check a module outside `_massive` against the interface rules.

    Args:
        where: The file's path, for the messages.
        tree: The file's syntax tree.
        package: The file's package, to resolve relative imports.
        interface: The names in `_massive.__all__`.
        network_allowed: Whether the file may import the network libraries (`tests/_fakes/`).
        strings_checked: Whether the rules on string constants apply (not to this file, which
            names forbidden modules and files in order to forbid them).

    Returns:
        One message per import, attribute read, or string the rules forbid.
    """
    references = [(line, name, "imports") for line, name in _imports(tree, package)]
    if strings_checked:
        references += [(line, name, "names") for line, name in _module_strings(tree)]
    found = []
    for line, name, how in references:
        if _inside(name, _WIRE) and name != _WIRE:
            member = name.removeprefix(f"{_WIRE}.").split(".")[0]
            if member not in interface or name.count(".") > _WIRE.count(".") + 1:
                found.append(f"{where}:{line}: {how} a _massive submodule: {name}")
        elif name.split(".")[0] in _NETWORK_LIBRARIES and not network_allowed:
            found.append(f"{where}:{line}: {how} a network library: {name}")
    found.extend(
        f"{where}:{line}: reads _massive.{attribute}, which is not in its __all__"
        for line, attribute in _wire_reads(tree, _wire_aliases(tree, package))
        if attribute not in interface
    )
    if strings_checked:
        found.extend(
            f"{where}:{line}: names a snapshot file: {text!r}"
            for line, text in _snapshot_names(tree)
        )
    return found


@functools.cache
def _parse(source: str) -> ast.Module:
    """Return the syntax tree of a module's text, parsed once per run for the same text.

    The wire-boundary and layer checks both read every module under `src/`; caching by text
    lets them share one parse. The checks only read the tree, never change it.

    Args:
        source: The module's text.

    Returns:
        The parsed module.
    """
    return ast.parse(source)


def boundary_violations(path: PurePosixPath, source: str, interface: frozenset[str]) -> list[str]:
    """Return every break of the wire package's boundary and layers in one file.

    Args:
        path: The file's path relative to the repository root (`src/deephaven_massive/...` or
            `tests/...`).
        source: The file's text.
        interface: The names in `_massive.__all__`.

    Returns:
        One `"<path>:<line>: <rule>"` message per violation, empty for a clean file.
    """
    tree = _parse(source)
    module, package = _module_of(path)
    where = str(path)
    if _inside(module, _WIRE):
        return _wire_rules(where, tree, module=module, package=package)
    if _inside(module, "tests.massive"):
        own_refresh_test = _inside(module, "tests.massive.refresh")
        return [
            f"{where}:{line}: only the refresh tests import refresh: {name}"
            for line, name in [*_imports(tree, package), *_module_strings(tree)]
            if _inside(name, f"{_WIRE}.{_REFRESH}") and not own_refresh_test
        ]
    return _outside_rules(
        where,
        tree,
        package,
        interface=interface,
        network_allowed=_inside(module, "tests._fakes"),
        strings_checked=path != PurePosixPath("tests/test_layout.py"),
    )


def _interface() -> frozenset[str]:
    """Return `_massive.__all__`, read from its source without importing the package.

    Returns:
        The names of the wire package's interface.

    Raises:
        AssertionError: If the file assigns no `__all__`.
    """
    tree = ast.parse((_SRC / "_massive" / "__init__.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and ast.unparse(node.targets[0]) == "__all__":
            return frozenset(ast.literal_eval(node.value))
    raise AssertionError("_massive/__init__.py defines no __all__")


_INTERFACE = frozenset({"WireError", "RestClient"})


def _check(path: str, source: str) -> list[str]:
    return boundary_violations(PurePosixPath(path), source, _INTERFACE)


def test_the_tree_keeps_the_wire_boundary() -> None:
    interface = _interface()
    files = [*_SRC.rglob("*.py"), *_TESTS.rglob("*.py")]
    found = [
        message
        for file in sorted(files)
        for message in boundary_violations(
            PurePosixPath(file.relative_to(_ROOT).as_posix()), file.read_text(), interface
        )
    ]
    assert found == []


def test_the_interface_is_read_from_the_source() -> None:
    assert {"WireError", "WireAuthError", "WireEntitlementError"} <= _interface()


# Inside the wire package: the allow list.

_WIRE_FILE = "src/deephaven_massive/_massive/rest/client.py"


@pytest.mark.parametrize(
    "source",
    [
        "import ssl\nimport urllib3\nimport certifi\n",
        "from __future__ import annotations\nimport pyarrow as pa\n",
        "import websockets.asyncio.client\nimport boto3\nfrom botocore.config import Config\n",
        "import orjson\n",
        "from deephaven_massive._massive.net.errors import WireError\n",
        "from ..spec import types\nfrom . import query\n",
    ],
)
def test_the_wire_package_may_import_its_allow_list(source: str) -> None:
    assert _check(_WIRE_FILE, source) == []


@pytest.mark.parametrize(
    ("source", "name"),
    [
        ("import deephaven.table\n", "deephaven.table"),
        ("from deephaven_server import Server\n", "deephaven_server.Server"),
        ("import jpy\n", "jpy"),
        ("from deephaven_massive._errors import MassiveError\n", "deephaven_massive._errors"),
        ("from ... import _enums\n", "deephaven_massive._enums"),
        ("import deephaven_massive\n", "deephaven_massive"),
        ("import requests\n", "requests"),
        ("importlib.import_module('deephaven.table')\n", "deephaven.table"),
    ],
)
def test_the_wire_package_must_not_import_outside_its_allow_list(source: str, name: str) -> None:
    [message] = _check(_WIRE_FILE, source)
    assert message.startswith(f"{_WIRE_FILE}:1: the wire package must not import {name}")


def test_the_wire_package_may_import_the_type_only_libraries_for_type_checking_only() -> None:
    type_only = (
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from typing_extensions import override\n"
        "else:\n"
        "    import ssl\n"
    )
    assert _check(_WIRE_FILE, type_only) == []
    for source, line in [
        ("from typing_extensions import override\n", 1),
        ("if TYPE_CHECKING:\n    pass\nelse:\n    import typing_extensions\n", 4),
        ("if TYPE_CHECKING:\n    import deephaven.table\n", 2),
    ]:
        [message] = _check(_WIRE_FILE, source)
        assert message.startswith(f"{_WIRE_FILE}:{line}: the wire package must not import")


_FLAT_FILE_CLIENT = "src/deephaven_massive/_massive/flatfiles/client.py"


def test_only_flatfiles_may_import_the_boto3_s3_stubs_for_type_checking_only() -> None:
    stubs = "if TYPE_CHECKING:\n    from types_boto3_s3.client import S3Client\n"
    assert _check(_FLAT_FILE_CLIENT, stubs) == []
    for path, source, line in [
        (_WIRE_FILE, stubs, 2),  # another subpackage
        ("src/deephaven_massive/_massive/net/http.py", stubs, 2),
        (_FLAT_FILE_CLIENT, "from types_boto3_s3.client import S3Client\n", 1),
        (_FLAT_FILE_CLIENT, "if TYPE_CHECKING:\n    pass\nelse:\n    import types_boto3_s3\n", 4),
        (_FLAT_FILE_CLIENT, "if TYPE_CHECKING:\n    import types_boto3_ec2\n", 2),
    ]:
        [message] = _check(path, source)
        assert message.startswith(f"{path}:{line}: the wire package must not import")


# Inside the wire package: the layers.


@pytest.mark.parametrize(
    ("file", "source"),
    [
        ("net/http.py", "from deephaven_massive._massive.net import errors\n"),
        ("spec/__init__.py", "from .types import Source\n"),
        ("ws/client.py", "from ..net import codec\nfrom ..spec.types import Field\n"),
        ("flatfiles/reader.py", "from deephaven_massive._massive.spec import types\n"),
        ("refresh/generate.py", "from ..rest import client\nfrom .. import WireError\n"),
        ("refresh/__main__.py", "from .generate import main\n"),
        ("__init__.py", "from deephaven_massive._massive.rest.client import RestClient\n"),
    ],
)
def test_each_layer_may_import_its_own_layers(file: str, source: str) -> None:
    assert _check(f"src/deephaven_massive/_massive/{file}", source) == []


@pytest.mark.parametrize(
    ("file", "source", "message"),
    [
        ("net/http.py", "from ..spec import types\n", "net must not import"),
        ("net/codec.py", "from deephaven_massive._massive import WireError\n", "net must not"),
        ("spec/types.py", "from ..net.errors import WireError\n", "spec must not import"),
        ("rest/client.py", "from ..ws import client\n", "rest must not import"),
        ("ws/client.py", "from ..flatfiles import layout\n", "ws must not import"),
        ("flatfiles/reader.py", "from ..rest import batch\n", "flatfiles must not import"),
        ("rest/batch.py", "from .. import WireError\n", "rest must not import"),
        ("net/errors.py", "from ..refresh import overrides\n", "only refresh imports refresh"),
        ("__init__.py", "from .refresh import generate\n", "only refresh imports refresh"),
    ],
)
def test_a_layer_must_not_import_another(file: str, source: str, message: str) -> None:
    [found] = _check(f"src/deephaven_massive/_massive/{file}", source)
    assert message in found


@pytest.mark.parametrize(
    ("file", "source", "message"),
    [
        ("net/http.py", "im('deephaven_massive._massive.rest.client')\n", "net must not import"),
        ("rest/client.py", "x = 'deephaven_massive._massive.ws'\n", "rest must not import"),
        ("ws/client.py", "im('deephaven.table')\n", "the wire package must not import"),
        ("spec/types.py", "x = 'deephaven_massive._errors'\n", "the wire package must not"),
    ],
)
def test_a_layer_must_not_name_another_in_a_string(file: str, source: str, message: str) -> None:
    [found] = _check(f"src/deephaven_massive/_massive/{file}", source)
    assert message in found


@pytest.mark.parametrize("module", ["util.py", "extra/__init__.py", "extra/helpers.py"])
def test_a_module_outside_the_known_layers_is_reported(module: str) -> None:
    # A new module would otherwise escape every layer rule.
    path = f"src/deephaven_massive/_massive/{module}"
    [found] = _check(path, "import ssl\n")
    assert found.startswith(f"{path}:1: not in a known layer of the wire package")


# The wire package's own tests.


def test_the_wire_tests_may_import_any_wire_module() -> None:
    source = "from deephaven_massive._massive.net import codec\nimport urllib3\n"
    assert _check("tests/massive/net/test_codec.py", source) == []


def test_only_the_refresh_tests_import_refresh() -> None:
    source = "from deephaven_massive._massive.refresh import generate\n"
    assert _check("tests/massive/refresh/test_generate.py", source) == []
    assert _check("tests/massive/rest/test_client.py", source) == [
        (
            "tests/massive/rest/test_client.py:1: only the refresh tests import refresh: "
            "deephaven_massive._massive.refresh.generate"
        )
    ]


def test_the_refresh_rule_covers_names_in_strings() -> None:
    source = "mock.patch('deephaven_massive._massive.refresh.fetch.download')\n"
    assert _check("tests/massive/refresh/test_fetch.py", source) == []
    [found] = _check("tests/massive/spec/test_init.py", source)
    assert "only the refresh tests import refresh" in found


# Everywhere else: only the interface.

_LIBRARY_FILE = "src/deephaven_massive/_query.py"  # any library module outside the wire


@pytest.mark.parametrize("path", [_LIBRARY_FILE, "src/deephaven_massive/_namespaces/stocks.py"])
@pytest.mark.parametrize(
    "source",
    [
        "from deephaven_massive._massive import RestClient, WireError\n",
        "from deephaven_massive import _massive\nx = _massive.WireError\n",
        "import deephaven_massive._massive as wire\nx = wire.RestClient\n",
        "import deephaven_massive._massive\nx = deephaven_massive._massive.WireError\n",
        "import pyarrow\nimport orjson\n",
    ],
)
def test_the_library_may_use_the_interface(path: str, source: str) -> None:
    assert _check(path, source) == []


def test_the_library_may_use_the_interface_through_relative_imports() -> None:
    assert _check(_LIBRARY_FILE, "from ._massive import WireError\n") == []
    nested = "src/deephaven_massive/_namespaces/stocks.py"
    assert _check(nested, "from .. import _massive\nx = _massive.RestClient\n") == []


@pytest.mark.parametrize(
    ("source", "name"),
    [
        ("import deephaven_massive._massive.net\n", "deephaven_massive._massive.net"),
        (
            "from deephaven_massive._massive.net.errors import WireError\n",
            "deephaven_massive._massive.net.errors.WireError",
        ),
        ("from deephaven_massive._massive import net\n", "deephaven_massive._massive.net"),
        ("from ._massive.spec import types\n", "deephaven_massive._massive.spec.types"),
        ("from ._massive import codec\n", "deephaven_massive._massive.codec"),
        (
            "__import__('deephaven_massive._massive', fromlist=['net'])\n",
            "deephaven_massive._massive.net",
        ),
        (
            "__import__('deephaven_massive._massive', None, None, ('spec',))\n",
            "deephaven_massive._massive.spec",
        ),
        # A relative name is resolved against the literal package given with it.
        (
            "importlib.import_module('._massive.net', 'deephaven_massive')\n",
            "deephaven_massive._massive.net",
        ),
        (
            "import_module('.net', package='deephaven_massive._massive')\n",
            "deephaven_massive._massive.net",
        ),
        (
            "importlib.import_module('.._massive.rest', 'deephaven_massive._namespaces')\n",
            "deephaven_massive._massive.rest",
        ),
    ],
)
def test_outside_code_must_not_import_a_wire_submodule(source: str, name: str) -> None:
    assert _check(_LIBRARY_FILE, source) == [
        f"{_LIBRARY_FILE}:1: imports a _massive submodule: {name}"
    ]


@pytest.mark.parametrize(
    ("source", "name"),
    [
        # Every dynamic import names its module in a string, whatever the spelling.
        (
            "importlib.import_module('deephaven_massive._massive.rest')\n",
            "deephaven_massive._massive.rest",
        ),
        (
            "im('deephaven_massive._massive.net')\n",  # from importlib import import_module as im
            "deephaven_massive._massive.net",
        ),
        (
            "il.import_module('deephaven_massive._massive.net')\n",  # import importlib as il
            "deephaven_massive._massive.net",
        ),
        (
            "importlib.import_module(name='deephaven_massive._massive.net')\n",
            "deephaven_massive._massive.net",
        ),
        ("__import__('deephaven_massive._massive.ws')\n", "deephaven_massive._massive.ws"),
        # Patching and looking up a module name it as well.
        (
            "mock.patch('deephaven_massive._massive.net.http.ssl_context')\n",
            "deephaven_massive._massive.net.http.ssl_context",
        ),
        (
            "monkeypatch.setattr('deephaven_massive._massive.net.codec._orjson', None)\n",
            "deephaven_massive._massive.net.codec._orjson",
        ),
        (
            "sys.modules['deephaven_massive._massive.net.codec']\n",
            "deephaven_massive._massive.net.codec",
        ),
        (
            "x = 'deephaven_massive._massive.WireError.mro'\n",  # deeper than an interface name
            "deephaven_massive._massive.WireError.mro",
        ),
    ],
)
def test_outside_code_must_not_name_a_wire_submodule(source: str, name: str) -> None:
    assert _check(_LIBRARY_FILE, source) == [
        f"{_LIBRARY_FILE}:1: names a _massive submodule: {name}"
    ]


@pytest.mark.parametrize(
    "source",
    [
        "x = 'deephaven_massive._massive'\n",
        "x = 'deephaven_massive._massive.WireError'\n",
        "x = 'deephaven_massive._types'\n",
        "x = 'results'\ny = 'net'\nz = 'stocks/T'\n",
        # A string with a leading dot is a module only as import_module's relative argument.
        "suffix = '.csv.gz'\nkind = '._massive.net'\n",
        "importlib.import_module('._massive.net')\n",  # no package: Python itself refuses
        "importlib.import_module('.net', name)\n",  # a package that is not a literal
        "os.path.join('._massive', 'deephaven_massive')\n",  # not import_module
        '"""See deephaven_massive._massive.net for the codec."""\n',
    ],
)
def test_strings_naming_the_interface_or_other_things_pass(source: str) -> None:
    assert _check(_LIBRARY_FILE, source) == []


@pytest.mark.parametrize(
    "source",
    [
        "from deephaven_massive import _massive\nx = _massive.net\n",
        "import deephaven_massive._massive as wire\nx = wire.net\n",
        "import deephaven_massive._massive\nx = deephaven_massive._massive.net\n",
        "from . import _massive as wire\nx = wire.net\n",
        "import deephaven_massive as dm\nx = dm._massive.net\n",
        "import deephaven_massive\nx = deephaven_massive._massive.net\n",
        "from deephaven_massive import _massive\nx = getattr(_massive, 'net')\n",
        "import deephaven_massive as dm\nx = getattr(dm._massive, 'net', None)\n",
    ],
)
def test_outside_code_must_read_only_interface_names(source: str) -> None:
    assert _check(_LIBRARY_FILE, source) == [
        f"{_LIBRARY_FILE}:2: reads _massive.net, which is not in its __all__"
    ]


@pytest.mark.parametrize(
    "library",
    ["urllib3", "websockets", "certifi", "boto3", "botocore"],
)
@pytest.mark.parametrize("path", [_LIBRARY_FILE, "tests/test_session.py"])
def test_outside_code_must_not_import_a_network_library(library: str, path: str) -> None:
    assert _check(path, f"import {library}.x\n") == [
        f"{path}:1: imports a network library: {library}.x"
    ]


def test_the_fakes_may_import_the_network_libraries() -> None:
    source = "import urllib3\nimport websockets.asyncio.server\nimport botocore\n"
    assert _check("tests/_fakes/ws.py", source) == []


@pytest.mark.parametrize(
    "text",
    [
        "vendor/massive/openapi.json",
        "openapi.json",
        "websocket.json",
        "llms.txt",
        "vendor/massive",
        "flat-files/manifest.json",
    ],
)
@pytest.mark.parametrize("path", [_LIBRARY_FILE, "tests/datasets/test_catalog.py"])
def test_outside_code_must_not_name_a_snapshot_file(text: str, path: str) -> None:
    assert _check(path, f"x = {text!r}\n") == [f"{path}:1: names a snapshot file: {text!r}"]


@pytest.mark.parametrize(
    "source",
    [
        '"""Generated from openapi.json, with vendor/massive as its input."""\n',
        "x = 'snapshot'\ny = 'snapshots()'\nz = 'rest.json'\n",
    ],
)
def test_prose_and_other_names_are_not_snapshot_files(source: str) -> None:
    assert _check(_LIBRARY_FILE, source) == []


def test_only_this_file_may_name_the_snapshot_files() -> None:
    assert _check("tests/test_layout.py", "x = 'openapi.json'\n") == []


# ---------------------------------------------------------------------------------------------
# The library's layers.
# ---------------------------------------------------------------------------------------------

# The six layers, lowest first. A module imports only its own layer or lower ones.
_RANKS = {"wire": 0, "vocabulary": 1, "datasets": 2, "tables": 3, "ingest": 4, "api": 5}
# The root modules of the vocabulary and of the API; every other root module is in no layer.
_VOCABULARY_MODULES = frozenset({"_config", "_enums", "_errors", "_types"})
_API_MODULES = frozenset({"__init__", "_session", "_query"})
# The packages that are each one layer.
_LAYER_PACKAGES = {
    "_massive": "wire",
    "_datasets": "datasets",
    "_tables": "tables",
    "_ingest": "ingest",
    "_namespaces": "api",
}
# The two lazy `__init__.py` files, the only modules that import by a computed name.
_LAZY_INITS = frozenset(
    {
        PurePosixPath("src/deephaven_massive/__init__.py"),
        PurePosixPath("src/deephaven_massive/_massive/__init__.py"),
    }
)
# The package `__init__.py` files that hold a docstring and nothing else.
_DOCSTRING_ONLY_INITS = frozenset(
    PurePosixPath(f"src/deephaven_massive/{package}/__init__.py")
    for package in ("_namespaces", "_datasets", "_tables", "_ingest")
)
# What the vocabulary and the datasets never import: they do no I/O and start no threads.
_IO_AND_THREAD_MODULES = frozenset(
    {
        "queue",
        "asyncio",
        "concurrent",
        "socket",
        "ssl",
        "http",
        "urllib",
        "subprocess",
        "_thread",
        "multiprocessing",
        "select",
        "selectors",
    }
)
_THREAD_CLASSES = frozenset({"Thread", "Timer"})
# `_massive.__all__`, split: the names every layer may use, and the clients only ingest uses.
_WIRE_VOCABULARY = frozenset(
    {
        "WireError",
        "WireAuthError",
        "WireEntitlementError",
        "DriftReport",
        "Field",
        "ParamKind",
        "Provenance",
        "RestPath",
        "Source",
        "Transport",
        "WireType",
        "WireUnit",
        "encode_value",
        "provenance",
        "source",
        "sources",
    }
)
_WIRE_CLIENTS = frozenset(
    {
        "RestClient",
        "Page",
        "to_batch",
        "WsClient",
        "WsState",
        "Status",
        "StatusKind",
        "Reject",
        "FlatFileClient",
        "FlatFile",
        "ReadPlan",
        "ReadColumn",
        "object_key",
    }
)
_INGEST = f"{_PACKAGE}._ingest"
_TYPES_TABLE = f"{_PACKAGE}._types.Table"
_SESSION = f"{_PACKAGE}._session"


def layer_of_file(path: PurePosixPath) -> str | None:
    """Return the layer of a library module.

    Args:
        path: The module's path relative to `src/deephaven_massive/`.

    Returns:
        `wire`, `vocabulary`, `datasets`, `tables`, `ingest`, or `api`; None for a module in no
        known layer (a new root module, or a new package).
    """
    if len(path.parts) > 1:
        return _LAYER_PACKAGES.get(path.parts[0])
    if path.stem in _VOCABULARY_MODULES:
        return "vocabulary"
    return "api" if path.stem in _API_MODULES else None


def layer_of_name(name: str) -> str | None:
    """Return the layer an imported name falls in.

    Args:
        name: An absolute module or attribute name, such as `deephaven_massive._enums.Timespan`.

    Returns:
        The layer of the library module the name is in; `api` for the root package and the
        names it defines; None for a name outside the library.
    """
    parts = name.split(".")
    if parts[0] != _PACKAGE:
        return None
    if len(parts) == 1:
        return "api"
    if parts[1] in _VOCABULARY_MODULES:
        return "vocabulary"
    return _LAYER_PACKAGES.get(parts[1], "api")


def _import_functions(tree: ast.Module) -> set[str]:
    """Return the bare names under which a file can call a dynamic import.

    Args:
        tree: The file's syntax tree.

    Returns:
        `__import__`, `import_module`, and every alias a `from importlib import import_module
        as <alias>` binds. A call through an attribute (`importlib.import_module`,
        `il.import_module`) is recognized by `_is_import_call` without an alias.
    """
    names = {"__import__", "import_module"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "importlib":
            names.update(a.asname or a.name for a in node.names if a.name == "import_module")
    return names


def _is_import_call(call: ast.Call, functions: set[str]) -> bool:
    """Tell whether a call is a dynamic import.

    Args:
        call: A call expression.
        functions: The bare names of the import functions, from `_import_functions`.

    Returns:
        True for `<anything>.import_module(...)`, `<anything>.__import__(...)`, and a call of
        one of `functions`.
    """
    function = call.func
    if isinstance(function, ast.Attribute):
        return function.attr in {"import_module", "__import__"}
    return isinstance(function, ast.Name) and function.id in functions


def _dynamic_imports(tree: ast.Module) -> list[tuple[int, ast.expr | None]]:
    """Return every dynamic import in a file with the expression that names its module.

    Args:
        tree: The file's syntax tree.

    Returns:
        `(line, module argument)` for each call of `importlib.import_module` or `__import__`
        under any alias; the argument is the first positional one or the `name=` keyword.
    """
    functions = _import_functions(tree)
    found: list[tuple[int, ast.expr | None]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _is_import_call(node, functions):
            keyword = next((k.value for k in node.keywords if k.arg == "name"), None)
            found.append((node.lineno, node.args[0] if node.args else keyword))
    return found


def _literal_imports(tree: ast.Module) -> list[tuple[int, str]]:
    """Return the modules a file imports dynamically by a literal name.

    Args:
        tree: The file's syntax tree.

    Returns:
        `(line, module name)` pairs.
    """
    return [
        (line, argument.value)
        for line, argument in _dynamic_imports(tree)
        if isinstance(argument, ast.Constant) and isinstance(argument.value, str)
    ]


_IMPORT_FUNCTIONS = frozenset({"import_module", "__import__"})


def _import_level(call: ast.Call) -> ast.expr | None:
    """Return the `level` argument of an `__import__` call, if it has one.

    Args:
        call: A dynamic import call.

    Returns:
        The fifth positional argument or the `level=` keyword; None for `import_module`.
    """
    function = call.func
    name = function.attr if isinstance(function, ast.Attribute) else ast.unparse(function)
    if name != "__import__":
        return None
    keyword = next((k.value for k in call.keywords if k.arg == "level"), None)
    return (
        call.args[4] if len(call.args) > 4 else keyword
    )  # __import__(name, g, l, fromlist, level)


def _is_resolved_name(call: ast.Call, argument: ast.expr | None) -> bool:
    """Tell whether a dynamic import names its module so that the rules can resolve it.

    Args:
        call: The dynamic import call.
        argument: Its module argument.

    Returns:
        True for a literal absolute name with no `level` other than a literal 0, and for a
        literal relative name passed to `import_module` with a literal package.
    """
    if not (isinstance(argument, ast.Constant) and isinstance(argument.value, str)):
        return False
    if argument.value.startswith("."):
        return _relative_import(call) is not None
    level = _import_level(call)
    return level is None or (isinstance(level, ast.Constant) and level.value == 0)


def _modules_lookups(tree: ast.Module) -> list[tuple[int, str]]:
    """Return every `sys.modules` lookup whose key is not a literal.

    Args:
        tree: The file's syntax tree.

    Returns:
        `(line, source text)` for `<x>.modules[key]` and `<x>.modules.get(key)` with a key that
        is not a string literal.
    """
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Subscript) and ast.unparse(node.value).endswith("sys.modules"):
            key: ast.expr = node.slice
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and ast.unparse(node.func.value).endswith("sys.modules")
            and node.args
        ):
            key = node.args[0]
        else:
            continue
        if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
            found.append((node.lineno, ast.unparse(node)))
    return found


def _names_import_function(node: ast.expr, functions: set[str]) -> bool:
    """Tell whether an expression refers to `import_module` or `__import__` by name.

    Args:
        node: An expression that is not the function of a direct call.
        functions: The bare names of the import functions, from `_import_functions`.

    Returns:
        True for an attribute or name that is an import function, or a string naming one (the
        argument of a `getattr`).
    """
    if isinstance(node, ast.Attribute):
        return node.attr in _IMPORT_FUNCTIONS
    if isinstance(node, ast.Name):
        return node.id in functions
    return isinstance(node, ast.Constant) and node.value in _IMPORT_FUNCTIONS


def _computed_imports(tree: ast.Module) -> list[tuple[int, str]]:
    """Return every import whose module the rules cannot read from the source.

    That is a dynamic import whose name is not a literal; a literal relative name without a
    literal package; an `__import__` with a `level` other than a literal 0; any use of
    `import_module` or `__import__` other than a direct call (an alias by assignment, a
    `getattr`); and a `sys.modules` lookup by a key that is not a literal.

    Args:
        tree: The file's syntax tree.

    Returns:
        `(line, source text)` pairs.
    """
    functions = _import_functions(tree)
    found: list[tuple[int, str]] = []
    called: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _is_import_call(node, functions):
            called.add(id(node.func))
            keyword = next((k.value for k in node.keywords if k.arg == "name"), None)
            argument = node.args[0] if node.args else keyword
            if not _is_resolved_name(node, argument):
                found.append((node.lineno, ast.unparse(node)))
    skipped = called | _docstrings(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.expr) or id(node) in skipped:
            continue
        if _names_import_function(node, functions):
            found.append((node.lineno, ast.unparse(node)))
    found.extend(_modules_lookups(tree))
    return sorted(found)


def _thread_reads(tree: ast.Module) -> list[tuple[int, str]]:
    """Return every read of `threading.Thread` or `threading.Timer`, through any alias.

    Args:
        tree: The file's syntax tree.

    Returns:
        `(line, "threading.<name>")` for an attribute read on a name bound to `threading`,
        a literal `getattr` on it, and `from threading import Thread` or `Timer`.
    """
    aliases = {
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
        if alias.name == "threading"
    }
    found = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in aliases
            and node.attr in _THREAD_CLASSES
        ):
            found.append((node.lineno, f"threading.{node.attr}"))
        elif isinstance(node, ast.Call) and _getattr_name(node, aliases) in _THREAD_CLASSES:
            found.append((node.lineno, f"threading.{_getattr_name(node, aliases)}"))
        elif isinstance(node, ast.ImportFrom) and node.module == "threading":
            found.extend(
                (node.lineno, f"threading.{alias.name}")
                for alias in node.names
                if alias.name in _THREAD_CLASSES
            )
    return found


def _client_reads(
    tree: ast.Module, package: str, references: list[tuple[int, str]]
) -> list[tuple[int, str]]:
    """Return every use of a wire client name: by import, string, or attribute read.

    Args:
        tree: The file's syntax tree.
        package: The file's package, to resolve relative imports.
        references: The file's imports and module-name strings.

    Returns:
        `(line, name)` for each client name of `_massive.__all__` the file uses; `*` for a star
        import of `_massive`, and the source text of a `getattr` on it whose name is not a
        literal, since either may reach any client.
    """
    aliases = _wire_aliases(tree, package)
    members = [
        (line, name.removeprefix(f"{_WIRE}.").split(".")[0])
        for line, name in references
        if _inside(name, _WIRE) and name != _WIRE
    ]
    found = [(line, member) for line, member in members if member in _WIRE_CLIENTS]
    found.extend(
        (line, attribute)
        for line, attribute in _wire_reads(tree, aliases)
        if attribute in _WIRE_CLIENTS
    )
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and any(a.name == "*" for a in node.names):
            base = _absolute(package, node.level, node.module) if node.level else node.module
            if base == _WIRE:
                found.append((node.lineno, "*"))
        elif (
            isinstance(node, ast.Call)
            and ast.unparse(node.func) == "getattr"
            and node.args
            and ast.unparse(node.args[0]) in aliases
            and _getattr_name(node, aliases) is None
        ):
            found.append((node.lineno, ast.unparse(node)))
    return found


def _deephaven_rule(layer: str, name: str, *, type_only: bool) -> str | None:
    """Return the Deephaven rule a reference breaks, if any.

    Args:
        layer: The importing module's layer.
        name: The imported or named module.
        type_only: Whether the import sits in an `if TYPE_CHECKING:` block.

    Returns:
        The rule's text, or None when the reference is allowed.
    """
    # The vocabulary binds Deephaven's `Table` at run time, so importing it from there imports
    # `deephaven.table.Table`.
    module = "deephaven.table.Table" if name == _TYPES_TABLE else name
    top = module.split(".", maxsplit=1)[0]
    if top == "deephaven_server":
        return f"nothing imports deephaven_server: {name}"
    if top != "deephaven":
        return None
    if layer == "vocabulary" and not _inside(module, "deephaven.table"):
        return f"the vocabulary imports only deephaven.table: {name}"
    if layer == "datasets" and not type_only:
        return f"the datasets layer imports deephaven only for type checking: {name}"
    return None


def layer_violations(path: PurePosixPath, source: str) -> list[str]:
    """Return every break of the library's layer rules in one module.

    Args:
        path: The module's path relative to the repository root (`src/deephaven_massive/...`).
        source: The module's text.

    Returns:
        One `"<path>:<line>: <rule>"` message per violation, empty for a clean module.
    """
    where = str(path)
    layer = layer_of_file(PurePosixPath(*path.parts[2:]))
    if layer is None:
        return [f"{where}:1: not in a known layer"]
    tree = _parse(source)
    module, package = _module_of(path)
    type_only = _type_checking_lines(tree)
    references = [*_imports(tree, package), *_module_strings(tree)]
    found = []
    for line, name in references:
        target = layer_of_name(name)
        if target is not None and _RANKS[target] > _RANKS[layer]:
            found.append(
                f"{where}:{line}: the {layer} layer must not import the {target} layer: {name}"
            )
        rule = _deephaven_rule(layer, name, type_only=line in type_only)
        if rule is not None:
            found.append(f"{where}:{line}: {rule}")
        if layer == "api" and _inside(name, _INGEST):
            if module != _SESSION:
                found.append(f"{where}:{line}: only _session.py imports the ingest layer: {name}")
            elif not _inside(name, f"{_INGEST}.clients"):
                found.append(f"{where}:{line}: _session.py imports only _ingest.clients: {name}")
    if path not in _LAZY_INITS:
        found.extend(
            f"{where}:{line}: a computed import: {text}" for line, text in _computed_imports(tree)
        )
    if layer in {"vocabulary", "datasets"}:
        found.extend(
            f"{where}:{line}: the {layer} layer does no I/O and starts no threads: {name}"
            for line, name in [*_imports(tree, package), *_literal_imports(tree)]
            if name.split(".")[0] in _IO_AND_THREAD_MODULES
        )
        found.extend(
            f"{where}:{line}: the {layer} layer starts no threads: {name}"
            for line, name in _thread_reads(tree)
        )
    if layer not in {"wire", "ingest"}:
        found.extend(
            f"{where}:{line}: only the ingest layer uses the wire client {name}"
            for line, name in _client_reads(tree, package, references)
        )
    docstring_only = len(tree.body) == 1 and ast.get_docstring(tree) is not None
    if path in _DOCSTRING_ONLY_INITS and not docstring_only:
        found.append(f"{where}:1: a layer package's __init__.py holds a docstring and nothing else")
    return sorted(found, key=lambda message: int(message.split(":")[1]))


def test_the_tree_keeps_the_layers() -> None:
    found = [
        message
        for file in sorted(_SRC.rglob("*.py"))
        for message in layer_violations(
            PurePosixPath(file.relative_to(_ROOT).as_posix()), file.read_text()
        )
    ]
    assert found == []


def test_the_wire_interface_splits_into_vocabulary_and_clients() -> None:
    assert _WIRE_VOCABULARY.isdisjoint(_WIRE_CLIENTS)
    assert _interface() == _WIRE_VOCABULARY | _WIRE_CLIENTS


# The modules a vocabulary name must never load: the clients and the network stack under them,
# which the datasets layer, importing only vocabulary, must not reach even indirectly.
_CLIENT_AND_NETWORK_MODULES = (
    f"{_WIRE}.rest",
    f"{_WIRE}.ws",
    f"{_WIRE}.flatfiles",
    f"{_WIRE}.net.http",
    "urllib3",
    "certifi",
    "websockets",
    "boto3",
    "botocore",
    "ssl",
    "socket",
    "http.client",
)


def _modules_loaded_by(names: list[str]) -> list[str]:
    """Read names of the wire interface in a fresh interpreter; report client modules loaded.

    Args:
        names: Attribute names to read from `deephaven_massive._massive`.

    Returns:
        The modules of `_CLIENT_AND_NETWORK_MODULES` that reading the names added to
        `sys.modules`, sorted. Whatever importing the package itself loads is left out: the
        library's front door reads its version through `importlib.metadata`, which on Python
        before 3.13 imports `email` and with it `socket`, and no name of the interface is to
        blame for that.
    """
    # The script is a literal and the data travels on stdin; `-P` keeps the working directory
    # off `sys.path`.
    result = subprocess.run(
        [
            sys.executable,
            "-P",
            "-c",
            (
                "import importlib, json, sys\n"
                "names, watched = json.load(sys.stdin)\n"
                "wire = importlib.import_module('deephaven_massive._massive')\n"
                "before = set(sys.modules)\n"
                "for name in names:\n"
                "    getattr(wire, name)\n"
                "print(json.dumps(sorted(set(watched) & (set(sys.modules) - before))))"
            ),
        ],
        input=json.dumps([names, _CLIENT_AND_NETWORK_MODULES]),
        capture_output=True,
        text=True,
        check=True,
    )
    loaded: list[str] = json.loads(result.stdout)
    return loaded


def test_the_wire_vocabulary_loads_no_client_or_network_module() -> None:
    assert _modules_loaded_by(sorted(_WIRE_VOCABULARY)) == []
    # The probe sees a client when one is read.
    assert f"{_WIRE}.rest" in _modules_loaded_by(["RestClient"])


def _layer_check(module: str, source: str) -> list[str]:
    return layer_violations(PurePosixPath(f"src/deephaven_massive/{module}"), source)


# Every module is in a known layer.


@pytest.mark.parametrize(
    ("module", "layer"),
    [
        ("_massive/rest/client.py", "wire"),
        ("_config.py", "vocabulary"),
        ("_types.py", "vocabulary"),
        ("_datasets/catalog.py", "datasets"),
        ("_tables/core.py", "tables"),
        ("_ingest/clients.py", "ingest"),
        ("__init__.py", "api"),
        ("_session.py", "api"),
        ("_query.py", "api"),
        ("_namespaces/stocks.py", "api"),
    ],
)
def test_every_library_module_has_a_layer(module: str, layer: str) -> None:
    assert layer_of_file(PurePosixPath(module)) == layer


@pytest.mark.parametrize("module", ["_rest.py", "_util/strings.py", "helpers.py"])
def test_a_module_in_no_known_layer_is_reported(module: str) -> None:
    assert _layer_check(module, "") == [f"src/deephaven_massive/{module}:1: not in a known layer"]


# Imports point down.


@pytest.mark.parametrize(
    ("module", "source"),
    [
        ("_errors.py", "from deephaven_massive._massive import WireError\n"),
        ("_datasets/catalog.py", "from deephaven_massive._enums import Timespan\n"),
        ("_tables/core.py", "from deephaven_massive._datasets.catalog import DatasetSpec\n"),
        ("_ingest/flatfiles.py", "from deephaven_massive._tables.core import SessionCore\n"),
        ("_query.py", "from deephaven_massive._tables.liveness import entered\n"),
        ("_namespaces/base.py", "from deephaven_massive._query import Query\n"),
        ("_tables/liveness.py", "from . import diagnostics\n"),
        ("__init__.py", "_EXPORTS = {'MassiveSession': 'deephaven_massive._session'}\n"),
    ],
)
def test_a_module_may_import_its_own_layer_and_lower_ones(module: str, source: str) -> None:
    assert _layer_check(module, source) == []


@pytest.mark.parametrize(
    ("module", "source", "message"),
    [
        (
            "_datasets/catalog.py",
            "if TYPE_CHECKING:\n    from deephaven_massive._query import QueryBase\n",
            "2: the datasets layer must not import the api layer: deephaven_massive._query",
        ),
        (
            "_tables/core.py",
            "from deephaven_massive._ingest.flatfiles import FlatFileHistory\n",
            "1: the tables layer must not import the ingest layer: deephaven_massive._ingest",
        ),
        (
            "_enums.py",
            "from deephaven_massive import _session\n",
            "1: the vocabulary layer must not import the api layer: deephaven_massive._session",
        ),
        (
            "_massive/net/http.py",
            "from ... import _errors\n",
            "1: the wire layer must not import the vocabulary layer: deephaven_massive._errors",
        ),
        (
            "_datasets/schema.py",
            "from .. import _tables\n",
            "1: the datasets layer must not import the tables layer: deephaven_massive._tables",
        ),
        (
            "_tables/diagnostics.py",
            "import deephaven_massive\n",
            "1: the tables layer must not import the api layer: deephaven_massive",
        ),
        (
            "_datasets/dates.py",
            "x = 'deephaven_massive._namespaces.stocks'\n",
            "1: the datasets layer must not import the api layer",
        ),
        (
            "_datasets/dates.py",
            "importlib.import_module('deephaven_massive._tables.core')\n",
            "1: the datasets layer must not import the tables layer",
        ),
    ],
)
def test_an_import_must_not_point_up(module: str, source: str, message: str) -> None:
    [found] = _layer_check(module, source)
    assert found.startswith(f"src/deephaven_massive/{module}:{message}")


# No computed imports outside the two lazy `__init__.py` files.


@pytest.mark.parametrize(
    "source",
    [
        "importlib.import_module(f'deephaven_massive._namespaces.{module_name}')\n",
        "from importlib import import_module as im\nim(name)\n",
        "__import__(name)\n",
        "importlib.import_module(name='x' + y)\n",
        "importlib.import_module()\n",
        # Literal names that only resolve at run time.
        "importlib.import_module('.._session', __package__)\n",
        "__import__('_session', globals(), None, ['MassiveSession'], 2)\n",
        "__import__('orjson', level=level)\n",
        # An import function reached other than by a direct call.
        "im = importlib.import_module\n",
        "from importlib import import_module as im\nloaders = [im]\n",
        "getattr(importlib, 'import_module')\n",
        "getattr(builtins, '__import__')\n",
        # A module looked up by a computed key.
        "sys.modules['deephaven_massive.' + name]\n",
        "sys.modules[f'deephaven_massive.{name}']\n",
        "sys.modules.get(name)\n",
    ],
)
def test_a_computed_import_is_reported(source: str) -> None:
    [found] = _layer_check("_datasets/catalog.py", source)
    assert "a computed import" in found


@pytest.mark.parametrize("module", ["__init__.py", "_massive/__init__.py"])
def test_the_lazy_inits_may_import_by_a_computed_name(module: str) -> None:
    source = "from importlib import import_module as _import_module\n_import_module(module)\n"
    assert _layer_check(module, source) == []


@pytest.mark.parametrize(
    "source",
    [
        "importlib.import_module('orjson')\n",
        "importlib.import_module('.codec', 'deephaven_massive._massive.net')\n",
        "__import__('orjson', globals(), None, [], 0)\n",
        "sys.modules['orjson']\n",
        "sys.modules.get('orjson')\n",
        '"""Loaded with importlib.import_module, never __import__."""\n',
    ],
)
def test_a_literal_import_is_not_computed(source: str) -> None:
    assert _layer_check("_massive/net/codec.py", source) == []


# Deephaven by layer.


@pytest.mark.parametrize(
    ("module", "source"),
    [
        ("_types.py", "from deephaven.table import Table\n"),
        ("_datasets/symbols.py", "if TYPE_CHECKING:\n    from deephaven.table import Table\n"),
        ("_tables/columns.py", "from deephaven import dtypes\n"),
        ("_namespaces/base.py", "from deephaven.table import Table\n"),
        ("_datasets/symbols.py", "import typing\nif typing.TYPE_CHECKING:\n    import deephaven\n"),
        (
            "_datasets/symbols.py",
            "if TYPE_CHECKING:\n    from deephaven_massive._types import Table\n",
        ),
        ("_datasets/symbols.py", "from deephaven_massive._types import Symbols, _All\n"),
        ("_tables/watchlist.py", "from deephaven_massive._types import Table\n"),
        ("_config.py", "from deephaven_massive._types import Table\n"),
    ],
)
def test_deephaven_where_the_layer_allows_it(module: str, source: str) -> None:
    assert _layer_check(module, source) == []


@pytest.mark.parametrize(
    ("module", "source", "message"),
    [
        (
            "_datasets/symbols.py",
            "from deephaven.table import Table\n",
            "1: the datasets layer imports deephaven only for type checking: deephaven.table.Table",
        ),
        (
            "_datasets/schema.py",
            "if TYPE_CHECKING:\n    pass\nelse:\n    from deephaven import dtypes\n",
            "4: the datasets layer imports deephaven only for type checking: deephaven.dtypes",
        ),
        (
            "_enums.py",
            "from deephaven import dtypes\n",
            "1: the vocabulary imports only deephaven.table: deephaven.dtypes",
        ),
        (
            "_types.py",
            "if TYPE_CHECKING:\n    from deephaven.execution_context import ExecutionContext\n",
            (
                "2: the vocabulary imports only deephaven.table: "
                "deephaven.execution_context.ExecutionContext"
            ),
        ),
        (
            "_session.py",
            "from deephaven_server import Server\n",
            "1: nothing imports deephaven_server: deephaven_server.Server",
        ),
        (
            "_tables/core.py",
            "if TYPE_CHECKING:\n    import deephaven_server\n",
            "2: nothing imports deephaven_server: deephaven_server",
        ),
        (
            # The vocabulary binds Deephaven's Table at run time: the same import, by proxy.
            "_datasets/catalog.py",
            "from deephaven_massive._types import Table\n",
            (
                "1: the datasets layer imports deephaven only for type checking: "
                "deephaven_massive._types.Table"
            ),
        ),
    ],
)
def test_deephaven_where_the_layer_forbids_it(module: str, source: str, message: str) -> None:
    assert _layer_check(module, source) == [f"src/deephaven_massive/{module}:{message}"]


# No I/O and no threads below the tables.


@pytest.mark.parametrize("module", ["_config.py", "_datasets/dates.py"])
@pytest.mark.parametrize(
    ("source", "name"),
    [
        ("import queue\n", "queue"),
        ("import asyncio\n", "asyncio"),
        (
            "from concurrent.futures import ThreadPoolExecutor\n",
            "concurrent.futures.ThreadPoolExecutor",
        ),
        ("import socket\n", "socket"),
        ("import ssl\n", "ssl"),
        ("from http import HTTPStatus\n", "http.HTTPStatus"),
        ("import urllib.parse\n", "urllib.parse"),
        ("import subprocess\n", "subprocess"),
        ("importlib.import_module('socket')\n", "socket"),
        ("import _thread\n", "_thread"),
        ("import multiprocessing.pool\n", "multiprocessing.pool"),
        ("import select\n", "select"),
        ("import selectors\n", "selectors"),
    ],
)
def test_the_lower_layers_import_no_io_or_thread_module(
    module: str, source: str, name: str
) -> None:
    [found] = _layer_check(module, source)
    assert found.endswith(f"does no I/O and starts no threads: {name}")


@pytest.mark.parametrize("module", ["_errors.py", "_datasets/history_source.py"])
@pytest.mark.parametrize(
    ("source", "name"),
    [
        ("import threading\nthreading.Thread(target=f)\n", "threading.Thread"),
        ("import threading as t\nt.Timer(1, f)\n", "threading.Timer"),
        ("from threading import Thread\n", "threading.Thread"),
        ("import threading\ngetattr(threading, 'Thread')(target=f)\n", "threading.Thread"),
    ],
)
def test_the_lower_layers_start_no_threads(module: str, source: str, name: str) -> None:
    [found] = _layer_check(module, source)
    assert found.endswith(f"starts no threads: {name}")


def test_the_lower_layers_may_take_a_lock() -> None:
    source = "import threading\n_lock = threading.Lock()\nlocal = threading.local()\n"
    assert _layer_check("_errors.py", source) == []


def test_the_upper_layers_may_do_io_and_start_threads() -> None:
    source = "import queue\nimport threading\nthreading.Thread(target=f)\n"
    assert _layer_check("_tables/liveness.py", source) == []


# Only ingest uses the wire clients.


@pytest.mark.parametrize(
    "source",
    [
        "from deephaven_massive import _massive\nx = _massive.FlatFileClient\n",
        "from deephaven_massive._massive import RestClient\n",
        "if TYPE_CHECKING:\n    from deephaven_massive._massive import WsClient\n",
        "import deephaven_massive._massive as wire\nx = getattr(wire, 'ReadPlan')\n",
        "x = 'deephaven_massive._massive.Page'\n",
        "from deephaven_massive._massive import *\n",
        "from deephaven_massive import _massive\nx = getattr(_massive, 'Flat' + 'FileClient')\n",
    ],
)
@pytest.mark.parametrize("module", ["_tables/core.py", "_query.py", "_datasets/request.py"])
def test_only_ingest_uses_a_wire_client(module: str, source: str) -> None:
    [found] = _layer_check(module, source)
    assert "only the ingest layer uses the wire client" in found


def test_a_relative_star_import_of_the_wire_package_counts() -> None:
    [found] = _layer_check("_query.py", "from ._massive import *\n")
    assert found.endswith("only the ingest layer uses the wire client *")


def test_ingest_uses_the_wire_clients_and_everyone_the_vocabulary() -> None:
    clients = "from deephaven_massive._massive import FlatFileClient, RestClient\n"
    assert _layer_check("_ingest/clients.py", clients) == []
    vocabulary = "from deephaven_massive import _massive\nx = _massive.WireError, _massive.Source\n"
    assert _layer_check("_errors.py", vocabulary) == []
    assert _layer_check("_datasets/schema.py", vocabulary) == []


# The composition root: only `_session.py` imports ingest, and only `_ingest.clients`.


def test_the_session_imports_the_ingest_clients() -> None:
    source = "from deephaven_massive._ingest.clients import SessionClients\n"
    assert _layer_check("_session.py", source) == []


@pytest.mark.parametrize(
    ("module", "source", "message"),
    [
        (
            "_query.py",
            "from deephaven_massive._ingest.clients import SessionClients\n",
            "only _session.py imports the ingest layer",
        ),
        (
            "_namespaces/base.py",
            "if TYPE_CHECKING:\n    from deephaven_massive._ingest import flatfiles\n",
            "only _session.py imports the ingest layer",
        ),
        (
            "__init__.py",
            "_EXPORTS = {'X': 'deephaven_massive._ingest.clients'}\n",
            "only _session.py imports the ingest layer",
        ),
        (
            "_session.py",
            "from deephaven_massive._ingest.flatfiles import FlatFileHistory\n",
            "_session.py imports only _ingest.clients",
        ),
        (
            "_session.py",
            "from deephaven_massive import _ingest\n",
            "_session.py imports only _ingest.clients",
        ),
    ],
)
def test_only_the_session_imports_ingest(module: str, source: str, message: str) -> None:
    [found] = _layer_check(module, source)
    assert message in found


# The layer packages' `__init__.py` files.


@pytest.mark.parametrize("package", ["_namespaces", "_datasets", "_tables", "_ingest"])
@pytest.mark.parametrize(
    "source",
    [
        "",
        "from deephaven_massive._tables.core import SessionCore\n",
        '"""The package."""\n\nfrom .core import SessionCore\n',
        '"""The package."""\n\n__all__ = []\n',
    ],
)
def test_a_layer_package_init_holds_only_a_docstring(package: str, source: str) -> None:
    message = "a layer package's __init__.py holds a docstring and nothing else"
    found = _layer_check(f"{package}/__init__.py", source)
    assert f"src/deephaven_massive/{package}/__init__.py:1: {message}" in found


@pytest.mark.parametrize("package", ["_namespaces", "_datasets", "_tables", "_ingest"])
def test_a_docstring_alone_is_a_valid_layer_package_init(package: str) -> None:
    assert _layer_check(f"{package}/__init__.py", '"""The package."""\n') == []


# The live tests stay out of every default run. `addopts` deselects the `integration` marker,
# and the conftest hook that checks the live markers runs first (`tryfirst`), before that
# deselection; run later, a marked unit test would already be deselected and escape the check.


def _pytest_settings() -> dict[str, object]:
    """Return the `[tool.pytest]` table of `pyproject.toml`.

    Returns:
        The table.
    """
    data = tomllib.loads((_ROOT / "pyproject.toml").read_text())
    settings: dict[str, object] = data["tool"]["pytest"]
    return settings


def test_the_default_run_deselects_the_live_tests() -> None:
    addopts = _pytest_settings()["addopts"]
    assert isinstance(addopts, list)
    assert addopts[-2:] == ["-m", "not integration and not benchmark"]


def test_both_live_markers_are_registered() -> None:
    markers = _pytest_settings()["markers"]
    assert isinstance(markers, list)
    assert {str(line).split(":")[0] for line in markers} >= {
        "integration",
        "integration_limits",
        "integration_smoke",
    }


def test_the_live_marker_check_runs_before_deselection() -> None:
    tree = ast.parse((_ROOT / "tests" / "conftest.py").read_text())
    (hook,) = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "pytest_collection_modifyitems"
    ]
    decorators = [ast.unparse(decorator) for decorator in hook.decorator_list]
    assert decorators == ["pytest.hookimpl(tryfirst=True)"]
