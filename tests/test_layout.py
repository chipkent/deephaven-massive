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

Each rule is a pure function over relative paths, so it is checked both against the real tree
and against a synthetic tree that breaks it.
"""

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
