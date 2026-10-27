from types import ModuleType

from deephaven_massive._massive import flatfiles


def test_the_package_defines_no_names() -> None:
    # Re-exporting here would import `client`, and boto3 with it, whenever any flat-file name
    # resolved. Submodules imported elsewhere in the process are bound on the package; they do
    # not count.
    defined = [
        name
        for name, value in vars(flatfiles).items()
        if not name.startswith("__") and not isinstance(value, ModuleType)
    ]
    assert defined == []
