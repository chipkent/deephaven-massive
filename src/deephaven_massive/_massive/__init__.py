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
    - `S3_ENDPOINT_URL`, `S3_BUCKET`: Massive's flat-file endpoint and bucket, which the
      library reads. They are not settings; a test points the library at a fake server by
      replacing them here, before the session's first flat-file read.
    - `RestClient`, `Page`, `to_batch`: the REST client, one decoded response, and the
      conversion of a page's rows to an Arrow batch with its drift report.
    - `REST_BASE_URL`: Massive's REST API, which the library calls. It is not a setting; a
      test points the library at a fake server by replacing it here, before the session is
      created.
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
    - `check_api_key`: the rule of an API key (non-empty visible ASCII, as a header needs),
      which the REST client and `MassiveConfig.api_key` both apply. It is part of `net` and
      loads no client.
    - `without_userinfo`: a URL with its user and password replaced by `***`, the one rule
      every message and log line that shows a configured URL uses. It is part of `net` and
      loads no client.
    - `encode_value`: the wire text of one REST parameter value, by its `ParamKind`. It is
      part of `spec` and loads no client: the REST client encodes every request with it, and
      the library's filter check calls it so a value it accepts always encodes.
    - `WsClient`, `WsState`, `Status`, `StatusKind`, `Reject`: the client of one WebSocket
      endpoint, its connection states, the status objects and rejected rows it reports.

Importing the package imports every client and its dependencies, and never Deephaven.

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

from deephaven_massive._massive.flatfiles.client import S3_BUCKET, S3_ENDPOINT_URL, FlatFileClient
from deephaven_massive._massive.flatfiles.object_keys import object_key
from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan
from deephaven_massive._massive.net.api_keys import check_api_key
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
from deephaven_massive._massive.rest.client import REST_BASE_URL, RestClient
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

__all__ = [
    "BASIC_REQUESTS_PER_MINUTE",
    "RATE_WINDOW",
    "REST_BASE_URL",
    "S3_BUCKET",
    "S3_ENDPOINT_URL",
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
    "check_api_key",
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
