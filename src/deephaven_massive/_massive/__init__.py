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
    - `source`, `sources`: the generated description of each REST path, WebSocket channel,
      and flat-file prefix, loaded once from the committed `spec/*.json`, each field carrying
      its description (the vendor's, or the refresh's correction where the vendor's is clearly
      wrong) in `Field.description`, and any per-asset-class text through
      `Field.description_for`, and each source its places in the dataset kinds
      (`Source.memberships`).
    - `provenance`, `Provenance`: the day the vendor sources were fetched, the vendor URL
      of each source, and what each vendor page says about Massive's plans
      (`Provenance.plans`), for documents that cite them.
    - `Transport`, `Source`, `RestPath`, `Field`, `ParamKind`, `WireType`, `WireUnit`,
      `DriftReport`, `DriftSample`: the value types of that description, and a drift report
      with the first row that showed each drifted key. A `RestPath`'s parameters are
      `Param`s, reached through `RestPath.params`.
    - `FlatFileClient`, `FlatFile`, `ReadPlan`, `ReadColumn`, `object_key`: the S3 flat-file
      client, one file being decoded into Arrow batches, what to read from it, and where a
      dataset's file for a day lives.
    - `RestClient`, `Page`, `to_batch`: the REST client, one decoded response, and the
      conversion of a page's rows to an Arrow batch with its drift report.
    - `RequestGovernor`, `BucketState`: when each REST request of a Massive subscription may
      start (a Basic subscription's pace, the pause after a 429), shared by the clients of one
      API key, and a snapshot of one subscription's state for diagnostics.
    - `Patience`: how long one read waits out its buckets' pauses, shared by its requests.
    - `BASIC_REQUESTS_PER_MINUTE`, `RATE_WINDOW`: a Basic subscription's limit (5 requests
      per minute) and the seconds a finished request holds its slot (61).
    - `Cancel`: the cancellation token that ends a request's waits.
    - `set_reply_capture`, `CapturedReply`: an optional process-wide sink that receives every
      raw reply the REST and WebSocket clients read (each REST body, refusals included, and
      each frame after authentication), already cleaned of the clients' secrets, for
      recording Massive's real replies. Off by default.
    - `origin_of`: the `(scheme, host, port)` a REST base URL reaches, which keys the
      governor the library shares. It is part of `net` and loads no client.
    - `check_rest_base_url`: the rule of a REST base URL (an origin alone, and `https` unless
      the host is loopback), which the REST client and `MassiveConfig.rest_base_url` both
      apply. It is part of `net` and loads no client.
    - `without_userinfo`: a URL with its user and password replaced by `***`, the one rule
      every message and log line that shows a configured URL uses. It is part of `net` and
      loads no client.
    - `encode_value`: the wire text of one REST parameter value, by its `ParamKind`. It is
      part of `spec` and loads no client: the REST client encodes every request with it, and
      the library's filter check calls it so a value it accepts always encodes.
    - `WsClient`, `WsState`, `Status`, `StatusKind`, `Reject`: the client of one WebSocket
      endpoint, its connection states, the status objects and rejected rows it reports.

Names resolve lazily on first access (PEP 562), so importing this package imports none of
its clients' dependencies (a REST-only session never imports `websockets`), and never
Deephaven.

Example:
    ::

        from deephaven_massive._massive import Transport, WireError, source

        trades = source(Transport.REST, "/v3/trades/{stockTicker}")
        assert trades.field("Price").wire_key == "price"

        try:
            raise WireError("HTTP 503 from GET /v3/trades/AAPL", status=503)
        except WireError as error:
            assert error.status == 503
"""

from importlib import import_module as _import_module
from typing import TYPE_CHECKING

__all__ = [
    "BASIC_REQUESTS_PER_MINUTE",
    "RATE_WINDOW",
    "BucketState",
    "Cancel",
    "CapturedReply",
    "DriftReport",
    "DriftSample",
    "Field",
    "FlatFile",
    "FlatFileClient",
    "Page",
    "ParamKind",
    "Patience",
    "Provenance",
    "ReadColumn",
    "ReadPlan",
    "Reject",
    "RequestGovernor",
    "RestClient",
    "RestPath",
    "Source",
    "Status",
    "StatusKind",
    "Transport",
    "WireAuthError",
    "WireEntitlementError",
    "WireError",
    "WireType",
    "WireUnit",
    "WsClient",
    "WsState",
    "check_rest_base_url",
    "encode_value",
    "object_key",
    "origin_of",
    "provenance",
    "set_reply_capture",
    "source",
    "sources",
    "to_batch",
    "without_userinfo",
]

# Each name of the interface, and the module that defines it.
_EXPORTS: dict[str, str] = {
    "BASIC_REQUESTS_PER_MINUTE": "deephaven_massive._massive.rest.governor",
    "RATE_WINDOW": "deephaven_massive._massive.rest.governor",
    "BucketState": "deephaven_massive._massive.rest.governor",
    "Cancel": "deephaven_massive._massive.net.cancel",
    "CapturedReply": "deephaven_massive._massive.net.capture",
    "set_reply_capture": "deephaven_massive._massive.net.capture",
    "RequestGovernor": "deephaven_massive._massive.rest.governor",
    "FlatFile": "deephaven_massive._massive.flatfiles.reader",
    "FlatFileClient": "deephaven_massive._massive.flatfiles.client",
    "Page": "deephaven_massive._massive.rest.response",
    "Patience": "deephaven_massive._massive.rest.governor",
    "ReadColumn": "deephaven_massive._massive.flatfiles.reader",
    "ReadPlan": "deephaven_massive._massive.flatfiles.reader",
    "RestClient": "deephaven_massive._massive.rest.client",
    "WireAuthError": "deephaven_massive._massive.net.errors",
    "WireEntitlementError": "deephaven_massive._massive.net.errors",
    "WireError": "deephaven_massive._massive.net.errors",
    "Reject": "deephaven_massive._massive.ws",
    "Status": "deephaven_massive._massive.ws",
    "StatusKind": "deephaven_massive._massive.ws",
    "WsClient": "deephaven_massive._massive.ws",
    "WsState": "deephaven_massive._massive.ws",
    "provenance": "deephaven_massive._massive.spec",
    "source": "deephaven_massive._massive.spec",
    "sources": "deephaven_massive._massive.spec",
    "DriftReport": "deephaven_massive._massive.spec.types",
    "DriftSample": "deephaven_massive._massive.spec.types",
    "Field": "deephaven_massive._massive.spec.types",
    "ParamKind": "deephaven_massive._massive.spec.types",
    "Provenance": "deephaven_massive._massive.spec.types",
    "RestPath": "deephaven_massive._massive.spec.types",
    "Source": "deephaven_massive._massive.spec.types",
    "Transport": "deephaven_massive._massive.spec.types",
    "WireType": "deephaven_massive._massive.spec.types",
    "WireUnit": "deephaven_massive._massive.spec.types",
    "object_key": "deephaven_massive._massive.flatfiles.object_keys",
    "check_rest_base_url": "deephaven_massive._massive.net.base_urls",
    "origin_of": "deephaven_massive._massive.net.origins",
    "without_userinfo": "deephaven_massive._massive.net.redaction",
    "to_batch": "deephaven_massive._massive.rest.batch",
    "encode_value": "deephaven_massive._massive.spec.values",
}

# Type checkers read the imports, so they see each name's real type and flag a name outside
# the interface; at run time `__getattr__` resolves the names instead.
if TYPE_CHECKING:
    from deephaven_massive._massive.flatfiles.client import FlatFileClient
    from deephaven_massive._massive.flatfiles.object_keys import object_key
    from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan
    from deephaven_massive._massive.net.base_urls import check_rest_base_url
    from deephaven_massive._massive.net.cancel import Cancel
    from deephaven_massive._massive.net.capture import CapturedReply, set_reply_capture
    from deephaven_massive._massive.net.errors import (
        WireAuthError,
        WireEntitlementError,
        WireError,
    )
    from deephaven_massive._massive.net.origins import origin_of
    from deephaven_massive._massive.net.redaction import without_userinfo
    from deephaven_massive._massive.rest.batch import to_batch
    from deephaven_massive._massive.rest.client import RestClient
    from deephaven_massive._massive.rest.governor import (
        BASIC_REQUESTS_PER_MINUTE,
        RATE_WINDOW,
        BucketState,
        Patience,
        RequestGovernor,
    )
    from deephaven_massive._massive.rest.response import Page
    from deephaven_massive._massive.spec import provenance, source, sources
    from deephaven_massive._massive.spec.types import (
        DriftReport,
        DriftSample,
        Field,
        ParamKind,
        Provenance,
        RestPath,
        Source,
        Transport,
        WireType,
        WireUnit,
    )
    from deephaven_massive._massive.spec.values import encode_value
    from deephaven_massive._massive.ws import Reject, Status, StatusKind, WsClient, WsState
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
