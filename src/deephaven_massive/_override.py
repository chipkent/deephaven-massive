"""The `override` decorator, on every supported Python.

`typing.override` exists only from Python 3.12, and the library supports 3.11, so this module
gives the library one `override` to mark the methods it overrides. At run time it returns the
method unchanged; mypy reads `typing_extensions.override` instead, so overrides stay checked.

Layer: vocabulary; it imports nothing of the library.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

__all__ = ["override"]

_F = TypeVar("_F")

if TYPE_CHECKING:
    from typing_extensions import override
else:

    def override(method: _F) -> _F:
        """Return `method` unchanged: `typing.override` exists only from Python 3.12.

        mypy reads `typing_extensions.override` instead, so overrides stay checked.

        Args:
            method: The overriding method.

        Returns:
            `method` itself.
        """
        return method
