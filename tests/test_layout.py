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
  `_massive` modules; never Deephaven, `jpy`, or the rest of the library.
- Its subpackages are layered: `net` and `spec` import nothing else in `_massive`; `rest`,
  `ws`, and `flatfiles` import only `net` and `spec`; `refresh` may import anything, and only
  `refresh` and its tests import `refresh`.
- `tests/massive/`, the wire package's own tests, may import any `_massive` module.
- Everything else, the rest of `tests/` included, sees only the interface: it imports no
  `_massive` submodule (by `import`, relative import, or a literal `importlib.import_module`
  or `__import__`), reads only names in `_massive.__all__`, imports none of `urllib3`,
  `websockets`, `certifi`, `boto3`, or `botocore` (`tests/_fakes/` excepted), and names no
  file of the vendored spec snapshot.

Each rule is a pure function over relative paths (and, for the boundary, source text), so it is
checked both against the real tree and against a synthetic tree that breaks it.
"""

import ast
import sys
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
        ("_catalog.py", "test_catalog.py"),
        ("_hosts.py", "test_hosts.py"),
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
        ("test_hosts_integration.py", "test_hosts.py"),
        ("namespaces/test_stocks_integration.py", "namespaces/test_stocks.py"),
    ],
)
def test_unit_test_of_maps_live_tests_to_their_mirror(test_file: str, expected: str) -> None:
    assert unit_test_of(PurePosixPath(test_file)) == PurePosixPath(expected)


def test_a_live_test_without_a_module_is_an_orphan() -> None:
    found = orphans(_paths("_hosts.py"), _paths("test_ws_integration.py"))
    assert found == ["tests/test_ws_integration.py"]


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
    found = orphans(_paths("_config.py"), _paths("test_config.py", "test_hosts.py"))
    assert found == ["tests/test_hosts.py"]


def test_test_catalog_is_a_plain_mirror() -> None:
    assert orphans(_paths("_catalog.py"), _paths("test_catalog.py")) == []
    assert orphans(_paths("_config.py"), _paths("test_catalog.py")) == ["tests/test_catalog.py"]


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
        "symbols/crypto_tickers.csv",
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
_LAYERS = {
    "net": frozenset({"net"}),
    "spec": frozenset({"spec"}),
    "rest": frozenset({"rest", "net", "spec"}),
    "ws": frozenset({"ws", "net", "spec"}),
    "flatfiles": frozenset({"flatfiles", "net", "spec"}),
}
_REFRESH = "refresh"

# The vendor files of the spec snapshot, and its directory.
_SNAPSHOT_FILES = frozenset({"openapi.json", "websocket.json", "llms.txt", "manifest.json"})
_SNAPSHOT_DIRECTORY = "refresh/snapshot"


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
    """Return every module a file imports, as absolute dotted names with their line.

    A `from a import b` counts as importing `a.b`, since `b` may be a submodule; a literal
    `importlib.import_module("a.b")` or `__import__("a.b")` counts as importing `a.b`.

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
        elif (
            isinstance(node, ast.Call)
            and ast.unparse(node.func) in {"importlib.import_module", "import_module", "__import__"}
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            found.append((node.lineno, node.args[0].value))
    return found


def _wire_aliases(tree: ast.Module, package: str) -> set[str]:
    """Return the expressions that name the `_massive` package in a file.

    Args:
        tree: The file's syntax tree.
        package: The file's package, to resolve relative imports.

    Returns:
        The full dotted name, plus every name an import binds to the package
        (`import deephaven_massive._massive as wire`, `from deephaven_massive import _massive`).
    """
    aliases = {_WIRE}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            aliases.update(a.asname for a in node.names if a.name == _WIRE and a.asname)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            if node.level:
                base = _absolute(package, node.level, node.module)
            aliases.update(a.asname or a.name for a in node.names if f"{base}.{a.name}" == _WIRE)
    return aliases


def _snapshot_names(tree: ast.Module) -> list[tuple[int, str]]:
    """Return the string constants that name a file of the spec snapshot.

    Docstrings are prose, not references to a file, so they are skipped.

    Args:
        tree: The file's syntax tree.

    Returns:
        `(line, text)` for each constant that contains the snapshot directory or whose last
        path component is a snapshot file.
    """
    docstrings = {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
    }
    return [
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
        and (
            _SNAPSHOT_DIRECTORY in node.value
            or PurePosixPath(node.value.strip()).name in _SNAPSHOT_FILES
        )
    ]


def _wire_layer(name: str) -> str | None:
    """Return the `_massive` subpackage a module name falls in.

    Args:
        name: An absolute module name inside `_massive`.

    Returns:
        The subpackage (`net`, `spec`, `rest`, `ws`, `flatfiles`, or `refresh`), or `None` for
        the package itself and the names of its interface.
    """
    parts = name.removeprefix(f"{_WIRE}.").split(".") if name != _WIRE else []
    return parts[0] if parts and parts[0] in {*_LAYERS, _REFRESH} else None


def _inside(name: str, package: str) -> bool:
    return name == package or name.startswith(f"{package}.")


def _wire_rules(where: str, imports: list[tuple[int, str]], layer: str | None) -> list[str]:
    """Check the imports of a module inside `_massive` against its allow list and layer.

    Args:
        where: The file's path, for the messages.
        imports: The module's imports, from `_imports`.
        layer: The module's own subpackage, or `None` for `_massive/__init__.py`.

    Returns:
        One message per import the rules forbid.
    """
    found = []
    for line, name in imports:
        top = name.split(".")[0]
        if _inside(name, _WIRE):
            target = _wire_layer(name)
            if target == _REFRESH and layer != _REFRESH:
                found.append(f"{where}:{line}: only refresh imports refresh: {name}")
            elif layer in _LAYERS and target not in _LAYERS[layer]:
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
    snapshot_checked: bool,
) -> list[str]:
    """Check a module outside `_massive` against the interface rules.

    Args:
        where: The file's path, for the messages.
        tree: The file's syntax tree.
        package: The file's package, to resolve relative imports.
        interface: The names in `_massive.__all__`.
        network_allowed: Whether the file may import the network libraries (`tests/_fakes/`).
        snapshot_checked: Whether the snapshot-name rule applies (not to this file, which
            names the snapshot files in order to forbid them).

    Returns:
        One message per import, attribute read, or string the rules forbid.
    """
    found = []
    for line, name in _imports(tree, package):
        if _inside(name, _WIRE) and name != _WIRE:
            member = name.removeprefix(f"{_WIRE}.").split(".")[0]
            if member not in interface or name.count(".") > _WIRE.count(".") + 1:
                found.append(f"{where}:{line}: imports a _massive submodule: {name}")
        elif name.split(".")[0] in _NETWORK_LIBRARIES and not network_allowed:
            found.append(f"{where}:{line}: imports a network library: {name}")
    aliases = _wire_aliases(tree, package)
    found.extend(
        f"{where}:{node.lineno}: reads _massive.{node.attr}, which is not in its __all__"
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and ast.unparse(node.value) in aliases
        and node.attr not in interface
    )
    if snapshot_checked:
        found.extend(
            f"{where}:{line}: names a snapshot file: {text!r}"
            for line, text in _snapshot_names(tree)
        )
    return found


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
    tree = ast.parse(source)
    module, package = _module_of(path)
    where = str(path)
    if _inside(module, _WIRE):
        layer = _wire_layer(module) if module != _WIRE else None
        return _wire_rules(where, _imports(tree, package), layer)
    if _inside(module, "tests.massive"):
        own_refresh_test = _inside(module, "tests.massive.refresh")
        return [
            f"{where}:{line}: only the refresh tests import refresh: {name}"
            for line, name in _imports(tree, package)
            if _inside(name, f"{_WIRE}.{_REFRESH}") and not own_refresh_test
        ]
    return _outside_rules(
        where,
        tree,
        package,
        interface=interface,
        network_allowed=_inside(module, "tests._fakes"),
        snapshot_checked=path != PurePosixPath("tests/test_layout.py"),
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


# Everywhere else: only the interface.

_LIBRARY_FILE = "src/deephaven_massive/_rest.py"


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
            "importlib.import_module('deephaven_massive._massive.rest')\n",
            "deephaven_massive._massive.rest",
        ),
        ("__import__('deephaven_massive._massive.ws')\n", "deephaven_massive._massive.ws"),
    ],
)
def test_outside_code_must_not_import_a_wire_submodule(source: str, name: str) -> None:
    assert _check(_LIBRARY_FILE, source) == [
        f"{_LIBRARY_FILE}:1: imports a _massive submodule: {name}"
    ]


@pytest.mark.parametrize(
    "source",
    [
        "from deephaven_massive import _massive\nx = _massive.net\n",
        "import deephaven_massive._massive as wire\nx = wire.net\n",
        "import deephaven_massive._massive\nx = deephaven_massive._massive.net\n",
        "from . import _massive as wire\nx = wire.net\n",
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
        "src/deephaven_massive/_massive/refresh/snapshot/openapi.json",
        "openapi.json",
        "websocket.json",
        "llms.txt",
        "refresh/snapshot",
        "flat-files/manifest.json",
    ],
)
@pytest.mark.parametrize("path", [_LIBRARY_FILE, "tests/test_catalog.py"])
def test_outside_code_must_not_name_a_snapshot_file(text: str, path: str) -> None:
    assert _check(path, f"x = {text!r}\n") == [f"{path}:1: names a snapshot file: {text!r}"]


@pytest.mark.parametrize(
    "source",
    [
        '"""Generated from openapi.json, with refresh/snapshot as its input."""\n',
        "x = 'snapshot'\ny = 'snapshots()'\nz = 'rest.json'\n",
    ],
)
def test_prose_and_other_names_are_not_snapshot_files(source: str) -> None:
    assert _check(_LIBRARY_FILE, source) == []


def test_only_this_file_may_name_the_snapshot_files() -> None:
    assert _check("tests/test_layout.py", "x = 'openapi.json'\n") == []
