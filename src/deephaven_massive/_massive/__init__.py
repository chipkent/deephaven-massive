"""The Massive wire package: every byte the library exchanges with Massive goes through here.

`deephaven_massive._massive` owns all access to Massive's REST API, WebSocket feeds, and S3
flat files, and the description of their wire format. It knows nothing of Deephaven, sessions,
or datasets: its clients take plain arguments and return plain Python and Arrow values, so
they are tested alone against fake servers. The rest of the library uses it only through the
names below, never through a submodule.

Its subpackages form layers. `net` (errors, TLS and proxy settings, JSON decoding) and `spec`
(the wire vocabulary) import nothing else in the package; `rest`, `ws`, and `flatfiles` (the
three clients) import only `net` and `spec`; `refresh` (the build-time schema generator, not
shipped) may import any of them, and nothing outside it imports it. Inside the package a module
imports only the standard library, `urllib3`, `websockets`, `certifi`, `orjson`, `boto3`,
`botocore`, `pyarrow`, and other modules of the package.

Interface:
    - `WireError`, `WireAuthError`, `WireEntitlementError`: the errors every client raises;
      `deephaven_massive._errors.from_wire` turns each into its public counterpart.
    - `FlatFileClient`, `FlatFile`, `ReadPlan`, `ReadColumn`, `object_key`: the S3 flat-file
      client, one file being decoded into Arrow batches, what to read from it, and where a
      dataset's file for a day lives.

Names resolve lazily on first access (PEP 562), so importing this package imports none of
its clients' dependencies, and never Deephaven.

Example:
    ::

        from deephaven_massive._massive import WireError

        try:
            raise WireError("HTTP 503 from GET /v3/trades/AAPL", status=503)
        except WireError as error:
            assert error.status == 503
"""

from importlib import import_module as _import_module
from typing import TYPE_CHECKING

__all__ = [
    "FlatFile",
    "FlatFileClient",
    "ReadColumn",
    "ReadPlan",
    "WireAuthError",
    "WireEntitlementError",
    "WireError",
    "object_key",
]

# Each name of the interface, and the module that defines it.
_EXPORTS: dict[str, str] = {
    "FlatFile": "deephaven_massive._massive.flatfiles.reader",
    "FlatFileClient": "deephaven_massive._massive.flatfiles.client",
    "ReadColumn": "deephaven_massive._massive.flatfiles.reader",
    "ReadPlan": "deephaven_massive._massive.flatfiles.reader",
    "WireAuthError": "deephaven_massive._massive.net.errors",
    "WireEntitlementError": "deephaven_massive._massive.net.errors",
    "WireError": "deephaven_massive._massive.net.errors",
    "object_key": "deephaven_massive._massive.flatfiles.layout",
}

# Type checkers read the imports, so they see each name's real type and flag a name outside
# the interface; at run time `__getattr__` resolves the names instead.
if TYPE_CHECKING:
    from deephaven_massive._massive.flatfiles.client import FlatFileClient
    from deephaven_massive._massive.flatfiles.layout import object_key
    from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan
    from deephaven_massive._massive.net.errors import (
        WireAuthError,
        WireEntitlementError,
        WireError,
    )
else:

    def __getattr__(name: str) -> object:
        """Resolve a name of the interface by importing its defining module.

        Nothing is cached in this module's namespace. The import system caches the defining
        module, and its per-module lock serializes concurrent first imports, so this function
        writes nothing itself and a repeated lookup costs a dictionary lookup.

        Args:
            name: The attribute requested.

        Returns:
            The object `name` refers to.

        Raises:
            AttributeError: If `name` is not in `__all__`.
        """
        try:
            module = _EXPORTS[name]
        except KeyError:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
        return getattr(_import_module(module), name)


def __dir__() -> list[str]:
    """Return the interface's names, for completion and `dir()`.

    Returns:
        `__all__`, sorted.
    """
    return sorted(__all__)


# Keep the namespace to the public names: the helper import goes once the module is built.
del TYPE_CHECKING
