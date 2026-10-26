"""Enforce the test layout: `tests/` mirrors `src/deephaven_massive/` one module to one file.

The module `<path>/<name>.py` is tested by `tests/<path>/test_<name>.py`, where `<name>` loses
its leading underscore and `<path>` is copied as is. A package `__init__.py` is mirrored by
`tests/<path>/test_init.py`. Live tests add the suffix `_integration` to the mirror's stem.
"""

from pathlib import Path, PurePosixPath

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_SRC = _ROOT / "src" / "deephaven_massive"
_TESTS = _ROOT / "tests"

# Test files that mirror no source module. A new entry needs owner approval and a change to the
# test-layout section of the design.
APPROVED_EXCEPTIONS = frozenset(
    {
        PurePosixPath("conftest.py"),  # embedded JVM and fixtures
        PurePosixPath("test_layout.py"),  # this file
        PurePosixPath("test_catalog.py"),  # catalog coverage assertions
        PurePosixPath("test_readme.py"),  # mirrors README.md
    }
)

# The shared fake servers, an approved package that the mirror rule ignores entirely.
FAKES_DIR = PurePosixPath("_fakes")

# Paths relative to the repository root that hold legacy code and are not mirrored.
LEGACY_PATHS = (  # LEGACY-EXCLUSION: legacy code, to be removed
    PurePosixPath("src/deephaven_massive/engine"),
    PurePosixPath("src/deephaven_massive/domains"),
    PurePosixPath("src/deephaven_massive/schemas"),
    PurePosixPath("tests/legacy"),
)

_INTEGRATION_SUFFIX = "_integration"


def mirror_of(module: PurePosixPath) -> PurePosixPath:
    """Return the unit-test path, relative to `tests/`, that mirrors a source module.

    Args:
        module: The module's path relative to `src/deephaven_massive/`.

    Returns:
        The mirrored test file's path relative to `tests/`.
    """
    name = "init" if module.stem == "__init__" else module.stem.removeprefix("_")
    return module.parent / f"test_{name}.py"


def unit_test_of(test_file: PurePosixPath) -> PurePosixPath:
    """Return the unit-test path a test file stands for, mapping live tests to their mirror.

    Args:
        test_file: A test file's path relative to `tests/`.

    Returns:
        The path itself for a unit test; for `test_<name>_integration.py`, `test_<name>.py`.
    """
    stem = test_file.stem.removesuffix(_INTEGRATION_SUFFIX)
    return test_file.with_name(f"{stem}.py")


def _is_legacy(path: Path) -> bool:
    relative = PurePosixPath(path.relative_to(_ROOT).as_posix())
    return any(relative.is_relative_to(legacy) for legacy in LEGACY_PATHS)


def _source_modules() -> list[PurePosixPath]:
    return sorted(
        PurePosixPath(path.relative_to(_SRC).as_posix())
        for path in _SRC.rglob("*.py")
        if not _is_legacy(path)
    )


def _test_files() -> list[PurePosixPath]:
    files = []
    for path in _TESTS.rglob("*.py"):
        relative = PurePosixPath(path.relative_to(_TESTS).as_posix())
        if not relative.is_relative_to(FAKES_DIR) and not _is_legacy(path):
            files.append(relative)
    return sorted(files)


@pytest.mark.parametrize(
    ("module", "expected"),
    [
        ("__init__.py", "test_init.py"),
        ("_config.py", "test_config.py"),
        ("_hosts.py", "test_hosts.py"),
        ("_domains/__init__.py", "_domains/test_init.py"),
        ("_domains/stocks.py", "_domains/test_stocks.py"),
    ],
)
def test_mirror_of_follows_the_naming_rule(module: str, expected: str) -> None:
    assert mirror_of(PurePosixPath(module)) == PurePosixPath(expected)


@pytest.mark.parametrize(
    ("test_file", "expected"),
    [
        ("test_config.py", "test_config.py"),
        ("test_hosts_integration.py", "test_hosts.py"),
        ("_domains/test_stocks_integration.py", "_domains/test_stocks.py"),
    ],
)
def test_unit_test_of_maps_live_tests_to_their_mirror(test_file: str, expected: str) -> None:
    assert unit_test_of(PurePosixPath(test_file)) == PurePosixPath(expected)


def test_every_module_below_the_package_is_private() -> None:
    # The first path component below `deephaven_massive/` carries a leading underscore, so the
    # public surface is `__init__.py` alone.
    public = [
        str(module)
        for module in _source_modules()
        if module != PurePosixPath("__init__.py") and not module.parts[0].startswith("_")
    ]
    assert public == []


def test_every_source_module_has_its_mirrored_test() -> None:
    missing = [
        f"{module} -> tests/{mirror_of(module)}"
        for module in _source_modules()
        if not (_TESTS / mirror_of(module)).is_file()
    ]
    assert missing == []


def test_no_two_source_modules_share_a_mirror() -> None:
    mirrors = [mirror_of(module) for module in _source_modules()]
    assert len(mirrors) == len(set(mirrors))


def test_every_test_file_mirrors_a_source_module() -> None:
    mirrors = {mirror_of(module) for module in _source_modules()}
    orphans = [
        f"tests/{test_file}"
        for test_file in _test_files()
        if test_file not in APPROVED_EXCEPTIONS and unit_test_of(test_file) not in mirrors
    ]
    assert orphans == []
