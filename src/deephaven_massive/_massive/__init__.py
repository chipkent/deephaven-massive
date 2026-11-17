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
    - `WireFailure`: what kind of failure a wire error is, whether Massive answered and, if
      not, why not; every wire error has exactly one.
    - `source`, `sources`: the generated description of each REST path, WebSocket channel,
      and flat-file prefix, loaded once from the committed `spec/*.json`, each field carrying
      its description (the vendor's, or the refresh's correction where the vendor's is clearly
      wrong) in `Field.description`, and any per-asset-class text through
      `Field.description_for`, and each source its places in the dataset kinds
      (`Source.memberships`).
    - `provenance`, `Provenance`: the day the vendor sources were fetched, the vendor URL
      of each source, and what each vendor page says about Massive's plans
      (`Provenance.plans`, each page's a `PlanAccess`), for documents that cite them.
    - `Transport`, `Source`, `RestPath`, `WsChannel`, `Feed`, `FeedHost`, `Field`,
      `ParamKind`, `WireType`, `WireUnit`, `DriftReport`, `DriftSample`: the value types of
      that description (a channel's documented hosts among them), and a drift report with the
      first row that showed each drifted key.
    - `RowFault`, `RowMark`, `RowMarks`: the rows a decoder could not read whole, by fault,
      which every decoder returns beside its batch and drift. A `RestPath`'s parameters are
      `Param`s, reached through `RestPath.params`.
    - `FlatFileClient`, `FlatFile`, `ReadPlan`, `ReadColumn`, `object_key`: the S3 flat-file
      client, one file being decoded into Arrow batches, what to read from it, and where a
      dataset's file for a day lives.
    - `RestClient`, `Page`, `to_batch`: the REST client, one decoded response, and the
      conversion of a page's rows to an Arrow batch with its drift report.
    - `RequestGovernor`, `BucketState`: when each REST request of a Massive subscription may
      start (a subscription's declared or learned pace, the pause after a 429), shared by the
      clients of one API key, and a snapshot of one subscription's state for diagnostics.
    - `Patience`: how long one read waits out its buckets' pauses, shared by its requests.
    - `RATE_WINDOW`: the seconds a finished request holds its slot of a paced subscription
      (61: Massive's minute and one second of margin).
    - `RETRYABLE_STATUSES`: the HTTP statuses whose request may succeed when sent again later
      (429, 500, 502, 503, 504), which the library reports as retryable.
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
    - `check_rest_base_url`: the rule of a REST base URL (an origin alone, and `https` unless
      the host is loopback), which the REST client and `MassiveConfig.rest_base_url` both
      apply. It is part of `net` and loads no client.
    - `without_userinfo`: a URL with its user and password replaced by `***`, the one rule
      every message and log line that shows a configured URL uses. It is part of `net` and
      loads no client.
    - `without_userinfo_or_query`: a URL with its query and fragment hidden as well, for a
      URL that may hold neither, such as `MassiveConfig.rest_base_url`. It is part of `net`
      and loads no client.
    - `encode_value`: the wire text of one REST parameter value, by its `ParamKind`. It is
      part of `spec` and loads no client: the REST client encodes every request with it, and
      the library's filter check calls it so a value it accepts always encodes.
    - `WsClient`, `WsState`, `Status`, `StatusKind`: the client of one WebSocket endpoint, its
      connection states, and the status objects it reports.

Importing the package imports every client and its dependencies, and never Deephaven.

Example:
    ::

        from deephaven_massive._massive import Transport, WireError, WireFailure, source

        trades = source(Transport.REST, "/v3/trades/{stockTicker}")
        assert trades.field("Price").wire_key == "price"

        try:
            raise WireError(
                "HTTP 503 from GET /v3/trades/AAPL", status=503, kind=WireFailure.REFUSED
            )
        except WireError as error:
            assert error.status == 503
"""

from deephaven_massive._massive.flatfiles.client import FlatFileClient
from deephaven_massive._massive.flatfiles.object_keys import object_key
from deephaven_massive._massive.flatfiles.reader import FlatFile, ReadColumn, ReadPlan
from deephaven_massive._massive.net.api_keys import check_api_key
from deephaven_massive._massive.net.base_urls import check_rest_base_url
from deephaven_massive._massive.net.cancel import Cancel
from deephaven_massive._massive.net.capture import CapturedReply, set_reply_capture
from deephaven_massive._massive.net.errors import (
    WireAuthError,
    WireEntitlementError,
    WireError,
    WireFailure,
)
from deephaven_massive._massive.net.origins import origin_of
from deephaven_massive._massive.net.redaction import (
    Redactor,
    without_userinfo,
    without_userinfo_or_query,
)
from deephaven_massive._massive.net.retry import RETRYABLE_STATUSES
from deephaven_massive._massive.rest.batch import to_batch
from deephaven_massive._massive.rest.client import RestClient
from deephaven_massive._massive.rest.governor import (
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
    Feed,
    FeedHost,
    Field,
    ParamKind,
    PlanAccess,
    Provenance,
    RestPath,
    RowFault,
    RowMark,
    RowMarks,
    Source,
    Transport,
    WireType,
    WireUnit,
    WsChannel,
)
from deephaven_massive._massive.spec.values import encode_value
from deephaven_massive._massive.ws import Status, StatusKind, WsClient, WsState

__all__ = [
    "RATE_WINDOW",
    "RETRYABLE_STATUSES",
    "BucketState",
    "Cancel",
    "CapturedReply",
    "DriftReport",
    "DriftSample",
    "Feed",
    "FeedHost",
    "Field",
    "FlatFile",
    "FlatFileClient",
    "Page",
    "ParamKind",
    "Patience",
    "PlanAccess",
    "Provenance",
    "ReadColumn",
    "ReadPlan",
    "Redactor",
    "RequestGovernor",
    "RestClient",
    "RestPath",
    "RowFault",
    "RowMark",
    "RowMarks",
    "Source",
    "Status",
    "StatusKind",
    "Transport",
    "WireAuthError",
    "WireEntitlementError",
    "WireError",
    "WireFailure",
    "WireType",
    "WireUnit",
    "WsChannel",
    "WsClient",
    "WsState",
    "check_api_key",
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
    "without_userinfo_or_query",
]
