from types import ModuleType

from deephaven_massive._massive import spec


def test_the_package_defines_no_names_yet() -> None:
    # Submodules imported elsewhere in the process are bound on the package; they do not count.
    defined = [
        name
        for name, value in vars(spec).items()
        if not name.startswith("__") and not isinstance(value, ModuleType)
    ]
    assert defined == []
