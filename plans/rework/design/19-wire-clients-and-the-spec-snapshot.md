## 19. Wire clients and the spec snapshot

The library talks to Massive through three clients of its own (REST, WebSocket, and S3 flat
files) and generates every wire schema from vendored, pinned vendor specs. It does not depend
on Massive's Python SDK, and nothing here works around a third-party client: each rule below
implements Massive's documented protocol or a requirement of this design.

**One package.** `deephaven_massive._massive` owns all Massive wire access: REST, WebSocket,
and S3 flat files (the bucket client, the key layout, CSV decoding, the S3 error mapping), the
generated wire schemas, the units, and the long-form names, and its `refresh/` subpackage
generates those schemas from the vendored spec snapshot. The snapshot itself lives outside the
package, in `vendor/massive/` at the top of the source checkout: it is Massive's published
content, vendored as generation input, not covered by the repository's license, and not
distributed in the packages (`vendor/README.md` says the same beside it). What stays outside
is the library's: Deephaven tables, sessions and liveness, dataset semantics
(`Symbol`, `Timestamp`, `Underlying`, the merged dataset schemas, conversion to nanoseconds,
the timestamp bound), the source choice, the missing-file policy, reader pools, cancellation,
public error text, and drift reporting. The clients take plain arguments, never
`MassiveConfig`, so they are tested alone against the fakes of `tests/_fakes/`. The listing
below, like the tree of [section 4.1](04-package-layout.md#41-source-modules), is the target layout.

```
src/deephaven_massive/_massive/
__init__.py          lazy interface: __all__ and re-exports only
net/                 shared by every client; imports nothing else in _massive
  __init__.py        USER_AGENT; re-exports
  errors.py          WireError, WireAuthError, WireEntitlementError; linked_errors (an
                     exception's whole chain of arguments, causes, and contexts), and
                     is_tls_failure and is_proxy_auth_failure, which search it
  http.py            CA bundle (SSL_CERT_FILE/SSL_CERT_DIR, else certifi); one shared TLS context
                     per process and certificate setting for websockets, and a new one for each
                     urllib3 pool; proxy from HTTPS_PROXY/NO_PROXY
  cancel.py          Cancel: a one-way threading.Event that follows parent tokens and runs
                     callbacks when set
  slots.py           ConnectionSlots: one slot per pooled connection, which every REST
                     attempt and every open flat file takes, so a client never has more
                     requests in flight than its pool holds; a Cancel ends a wait for one
  origins.py         origin_of(url): the (scheme, host, port) a URL reaches, default port
                     filled in; the REST client's next_url check and the governor registry's
                     key both compare origins with it
  retry.py           the retry schedule (0.5/1/2 s, +20 % jitter; Retry-After; an unpaced 429
                     pauses,
                     retryable classification; TLS never retried (net.errors finds it);
                     Clock (injectable, tests never sleep); an unusable Retry-After is logged
                     at WARNING, never 0
  codec.py           JSON codec: orjson when installed (logged once), else stdlib
  redaction.py       Redactor, redact: a client's secrets, however JSON
                     spells them, become *** in whatever it receives, before it is decoded,
                     quoted, or logged
  capture.py         set_reply_capture, CapturedReply: the optional sink every cleaned REST
                     body and WebSocket frame is handed to
spec/                run-time wire vocabulary; imports nothing else in _massive
  __init__.py        source(transport, key), sources(transport), provenance(): load once under
                     a lock, and the cache that holds them
  generated_schemas.py  parsing of the generated spec/*.json files into the types of spec.types
  types.py           Transport, Source(key, transport, fields, memberships),
                     Membership(kind, group, position, projection), RestPath(Source + template,
                     result key, params), Field(wire key, column, WireType, unit, required,
                     conversion), Conversion (named conversions, each with its declared and
                     resulting WireType), Param(+ unit), ParamKind, WireType, WireUnit,
                     ENVELOPE_KEYS, DriftReport(extra, missing, statuses, samples),
                     DriftSample(symbol, value); WireType.common_type, the narrowest wire type
                     that holds values of two types, which the refresh and the run-time schema
                     merge both call
  convert.py         convert(wire_type, value): one checked, exact scalar per WireType, shared by
                     every JSON decoder (REST to_batch, WebSocket frames); column_converter, the
                     same check over one field's values of many objects at once; UnconvertibleError;
                     convert_field applies a field's named conversion and raises with the
                     shared bad-value message; field_converter gives a decoder each field's
                     converter once, and unconvertible_message, missing_key_message and
                     excerpt word an odd value or a dropped row alike in both decoders, which
                     add only their own context
  values.py          encode_value(param, value, *, label): one REST parameter value's wire
                     text by ParamKind, for the REST client and the library's filter check
  rest.json  ws.json  flatfiles.json        generated, committed
rest/                imports net, spec
  __init__.py        RestClient, Page, to_batch
  client.py          pool, bearer auth, retries, a governor permit per attempt, next_url paging,
                     close
  governor.py        RequestGovernor: per-bucket pacing (Basic: 5 per minute), pauses after
                     429, learning, give-up; BucketState snapshots
  query.py           request targets (values encoded by spec.values), next_url checks
  response.py        strict decoding, envelopes, result keys, error mapping (http_error);
                     RequestTarget, the request a page answers
  batch.py           to_batch(rows, rest_path, *, required) -> (RecordBatch, DriftReport)
ws/                  imports net, spec
  __init__.py        WsClient, WsState, Status, StatusKind, Reject
  client.py          loop thread, connect/reconnect task, sender, close/join, per-channel
                     required keys (set_required)
  protocol.py        handshake and auth frames, subscribe frames, frame decoding against ws.json,
                     rows + rejects + DriftReport, failure classification
  backoff.py         backoff policy, stable-uptime reset
flatfiles/           imports net, spec
  __init__.py        docstring only: _massive's interface maps each name to its submodule,
                     so no flat-file name but FlatFileClient imports boto3
  client.py          FlatFileClient: a lazy boto3 client per connection slot, shared model loader,
                     closed flag, open(key) -> FlatFile | None, S3 error mapping, prefixes(),
                     close(); botocore's body read errors become OSError
  object_keys.py     object_key(prefix, day, venue): <prefix>/<YYYY>/<MM>/<date>.csv.gz; {venue} template
  reader.py          FlatFile.batches(plan) -> (RecordBatch, DriftReport) per block; header peek
refresh/             build time only; excluded from the wheel; may import any of the above
  __init__.py        empty
  __main__.py        [--check | --fetch]; the plain run regenerates spec/*.json
  generate.py        the refresh: snapshot -> generators -> spec/*.json, written only when every
                     gate passes and the text changes
  snapshot.py        the snapshot's location (vendor/massive/), layout, and manifest; store
                     (staged write, atomic swap) and verified load
  fetch.py           download and put each file in canonical form; stores through snapshot.py
  docs.py            llms.txt and the pages it links: each page's title, each REST page's
                     endpoint line, each WebSocket page's feed (cluster and channel), the
                     flat-file overview prefix tables, "Columns" tables, and "Example Data" headers
  knowledge_base.py  ARTICLES (slug -> what the library takes from it), the knowledge-base
                     articles the library relies on; the Markdown of an article's HTML page
  plan_access.py     each vendor page's "Plan Access", "Plan Recency", and "Plan History"
                     sections -> PlanAccess, stored in descriptions.json by page URL
  openapi.py         OpenAPI reading: $ref, allOf merge, one-branch choices, checked accessors
  plain_text.py      the four fixed rules that turn a vendor description into plain text
  naming.py          the naming policy shared by every transport: PascalCase columns, snake
                     case concepts, short and timestamp-like names, the dataset key columns
  fields.py          the field rules every generator shares: representation rules, names,
                     units, type fixes, documented examples, and the gates met per field
  rest_sources.py    REST paths -> RestPath: flattening, result keys, parameters, path
                     coverage, and the gates met per parameter
  ws_sources.py, flat_file_sources.py  the WebSocket and flat-file generators
  kinds.py           KINDS and PROJECTIONS -> memberships; type-agreement (by
                     WireType.common_type), transport-agreement, and the kind column order
  gates.py           Gate (the named gates of section 19.4), Problem, RefreshError, and the
                     checks that span sources: stale entries, exclusions, concept collisions
  render.py          spec/*.json in canonical text
  hand_input.py      HandInput: the hand tables bundled for the generators
  lexicon.py         SHAPES (object shape -> lexicon), lexicons, identity entries
  overrides.py       NAMES, KINDS, PROJECTIONS, UNITS and tag conflicts, TYPE_FIXES,
                     KEY_FIXES, UNDECLARED_FIELDS, OPTIONAL_FIELDS, TRANSPORT_ONLY,
                     RESULT_KEYS, PARAM_UNITS, VENUES
  exclusions.py      EXCLUSIONS (kind, item, reason), the executable rows of section 8.12;
                     SAMPLE_ERRORS (path, wire key, sample, reason), the wrong vendor samples;
                     IGNORED_KEYS (source, wire key, reason), the keys dropped without drift
  drift.py           live-against-snapshot comparison, unaccounted items, deprecations
  spec_diff.py       flatten the paths of an OpenAPI specification and describe how two
                     copies differ

vendor/massive/      outside the package; Massive's published content, vendored as generation
                     input, not covered by the repository's license, in no distribution
  openapi.json  websocket.json  llms.txt  manifest.json
  rest/  websocket/  flat-files/   (the pages of each area llms.txt links)
  knowledge-base/  (the articles the library relies on, as Markdown extracted from HTML)
```

**The interface.** `_massive/__init__.py` defines `__all__` and re-exports exactly these 39
names, resolving them lazily (PEP 562), so a REST-only session never imports boto3 or
websockets:

- errors: `WireError`, `WireAuthError`, `WireEntitlementError`;
- REST: `RestClient`, `Page`, `to_batch`, `RequestGovernor`, `BucketState`, `Patience`,
  `BASIC_REQUESTS_PER_MINUTE`, `RATE_WINDOW`, and `Cancel` (the token that ends a request's
  waits, from `net.cancel`);
- WebSocket: `WsClient`, `WsState`, `Status`, `StatusKind`, `Reject`;
- flat files: `FlatFileClient`, `FlatFile`, `ReadPlan`, `ReadColumn`, `object_key`;
- spec: `source`, `sources`, `Transport`, `Source`, `RestPath`, `Field`, `ParamKind`,
  `WireType`, `WireUnit`, `DriftReport`, `DriftSample`, `provenance`, `Provenance`, and
  `encode_value` (one parameter value's wire text by its `ParamKind`, in `spec.values`, which
  the REST client and the library's filter check both run, so a filter the check accepts
  always encodes; it loads no client, and a subprocess test checks that no vocabulary name
  loads a client or network module);
- origins: `origin_of` (from `net.origins`), the vocabulary name the library uses to key a
  shared governor by REST origin.
- reply capture: `set_reply_capture` and `CapturedReply` (from `net.capture`), an optional
  process-wide sink that receives every reply the REST and WebSocket clients read, each REST
  body (refusals included) and each frame after authentication, already cleaned of the
  clients' secrets by their `Redactor`; off by default, at the cost of one `None` check per
  reply. Only the live-run program uses it, to record Massive's real replies.

`Param` is reached as an attribute of `RestPath`. `Transport` (`REST`, `WS`, `FLAT_FILES`) is
the library's one transport enum.

**Wire errors.** There are three: `WireError`, `WireAuthError`, and `WireEntitlementError`,
each carrying `status` (or `None`), `host`, `path`, `request_id` (or `None`), and `message`. A
429 that persists after the retries is `WireError(status=429)`; a fatal WebSocket handshake is
`WireError(status=<http>)`. `_errors.from_wire` turns each into `MassiveError`,
`AuthenticationError`, or `EntitlementError` with the same message, `status`, and
`request_id`.

**The boundary and the layers**, enforced by an AST test in `tests/test_layout.py` with a
negative test per rule:

- Inside `_massive`, a module imports only the standard library, `urllib3`, `websockets`,
  `certifi`, the optional `orjson`, `boto3`, `botocore`, `pyarrow`, and other `_massive`
  modules. It never imports `deephaven`, `deephaven_server`, `jpy`, or any module outside
  `_massive`. The exceptions are `typing_extensions` (for `override` on Python 3.11) and, in
  `flatfiles` only, `types_boto3_s3` (for `S3Client`), inside `if TYPE_CHECKING:` only, which
  never runs.
- **Layers.** `net` and `spec` import nothing else in `_massive`. `rest`, `ws`, and `flatfiles`
  import only `net` and `spec`. `refresh` may import any of them, and only `refresh` and its
  tests import `refresh`.
- **The wire package's own tests.** Modules under `tests/massive/` (the mirror tests of
  `_massive` and the refresh parity tests) may import any `_massive` module, and only those
  under `tests/massive/refresh/` import `refresh`.
- Everywhere else outside `_massive`, the rest of `tests/` included: no module imports
  `urllib3`, `websockets`, `certifi`, `boto3`, or `botocore` (except `tests/_fakes/`); no
  module imports a `_massive` submodule (by `import`, relative import, or
  `importlib.import_module`/`__import__` with a literal name); an attribute read on `_massive`
  names only a member of `_massive.__all__`; and no module names a snapshot file.
- A subprocess import of `deephaven_massive._massive` leaves `deephaven` out of `sys.modules`.

**Not shipped.** The wheel excludes all of `refresh/`, and neither the wheel nor the sdist
contains `vendor/` (the CI build job checks both archives). `spec/*.json` ship as package data.
The tests that read the snapshot (the parity test among them) skip with a stated reason where
`vendor/massive/` is absent and the `CI` environment variable is unset, as in an unpacked
sdist. With `CI` set they never skip, so a CI checkout without the snapshot fails them.

### 19.1 The REST client

`_massive.RestClient` is one object per session, created on first use and closed by
`m.close()` (a session closer). It is **safe to call from any number of threads at once**: its
shared mutable state is urllib3's connection pool, which locks internally, the
`RequestGovernor` it takes a permit from before every attempt, which locks its own state and
may be shared with other clients, and its closed `Cancel` token. Every call is independent; no
call mutates an argument (the parameters are copied before they are encoded).

```python
RestClient(api_key, *, base_url, max_concurrency, governor, on_refused=None)
client.get(rest_path, path_params, query_params, *, bucket, cancel=None,
           patience=UNLIMITED) -> Page                               # one request
client.pages(rest_path, path_params, query_params, *, bucket, cancel=None,
             patience=UNLIMITED, limit=None) -> Iterator[Page]       # follows next_url
RequestGovernor(buckets, *, learn_only=(), clock=SYSTEM_CLOCK, labels={})
    # declare, admission(bucket, *, lane, patience=UNLIMITED) -> Admission, done, release,
    # refused, charge_pause, snapshot, next_change, estimate, idle, subscribe,
    # subscribe_bucket, unsubscribe
Admission.acquire(cancel) -> Permit   # inside `with governor.admission(...)`; exit reports it
Patience(seconds, *, advice="")       # one read's budget of pause time, shared by its requests;
                                      # advice ends its give-up message
Cancel(*parents)                                  # a threading.Event set once, following parents
Page(status: str, request_id: str | None, rows: list[dict], body: dict | list,
     drift: DriftReport)          # drift.statuses only; repr omits rows and body
to_batch(rows, rest_path, *, required) -> (pyarrow.RecordBatch, DriftReport)
```

`rest_path` is the path's `RestPath` from `spec/rest.json`; the client reads its template,
result key, and parameter kinds from that value, so its tests build `RestPath` values by hand.

- **Transport.** `max_concurrency` urllib3 `PoolManager`s (or `ProxyManager`s, below), one
  per connection slot (`net.slots`), each with `maxsize=1`, its own `SSLContext`,
  `retries=False` (the client retries itself), and a 10 s connect and 30 s read timeout. An
  attempt takes a slot and an idle pool with it, uses that pool alone, and gives both back
  together, so fan-out never discards connections and no two threads open connections on one
  `SSLContext` (see the certificates entry below). Requests send `Accept-Encoding: gzip`
  (urllib3 decodes it). `User-Agent: deephaven-massive/<version>` (`net.USER_AGENT`).
  The body is streamed (`preload_content=False`) into one buffer in 1 MiB decoded reads. A
  body that decodes to more than `MAX_BODY_BYTES` (256 MiB, a module constant, not a setting)
  raises `WireError` with the response's status, never retried, so a gzip bomb from a broken
  or hostile server or proxy cannot exhaust memory. A body that does not decompress is
  treated the same way: both are faults of the reply's content, which the same request would
  bring again. A body that breaks off or times out is a transient failure, retried like a
  reset. The governor still hears of every attempt whose status arrived: a 429 is reported
  as a refusal (the bucket pauses) whether its body was read, was too large, would not
  decompress, or broke off, and any other unreadable reply as answered. A body read in full
  returns its connection to the pool; one abandoned part way (the cap, a reset, a decode
  error) closes it first, so its unread rest is never taken for the next reply. The body read
  in full passes through `Redactor.received` once, as the same `bytearray` (never copied to
  `bytes`), before the 429, error, and page branches read it. The cap sits far above the largest page the datasets ask for (section 8):
  pages of short rows (trades, quotes, bars, ratings) decode to some 10 to 40 MB at 50,000
  rows, and pages of long rows stay at tens of MB because their page size is lowered. The cap
  bounds a decompression bomb, not every memory risk: a body under it still parses into
  Python objects several times its size.
- **Authentication.** `Authorization: Bearer <key>` on every request. The key never appears in
  a URL: an `apiKey` parameter in a `next_url` is removed before the request. A key that is
  empty or holds anything but visible ASCII (a trailing newline from a key file) raises
  `ValueError` at construction, without quoting the key. The base URL must be `https`, except
  for a loopback host (`localhost`, `127.0.0.0/8`, `::1`), so the key never travels in clear
  text; it names only a scheme, host, and port (no user information, path, or query).
- **Proxies and certificates** come from `net.http`: `urllib.request.getproxies()` for
  `HTTPS_PROXY` and `urllib.request.proxy_bypass` for `NO_PROXY` select a `ProxyManager` for
  the REST host or a plain `PoolManager`, read once at construction. A user and password in
  the proxy URL are sent as `Proxy-Authorization` (`net.http.proxy_credentials`; urllib3 does
  not send them itself), and the pools hold the URL without them, so they never reach a log or
  an error. HTTPS goes through the proxy in a `CONNECT` tunnel, so the proxy sees
  `Proxy-Authorization` and never the bearer key. A proxy reached over `https` is verified
  with the same context as Massive, its pool's own. The CA bundle is the system default when
  `SSL_CERT_FILE` or `SSL_CERT_DIR` is set (an `ssl.create_default_context()` honours both) and
  `certifi`'s bundle otherwise. A context is not safe to share between threads that open
  connections on it if any of them writes to it: urllib3 writes the ALPN list and verification
  mode onto its context before every connection it opens, while another thread's `SSL_new` may
  be copying the ALPN list with the GIL released, a use-after-free in CPython
  (python/cpython#141012, unfixed in every release, the free-threaded build included) that
  corrupts the handshake (`TLSV1_ALERT_DECODE_ERROR`) or crashes the process. So each of the
  REST client's urllib3 pools gets a context of its own (`net.http.new_ssl_context()`), used
  by one thread at a time. A pool and its context are built when a request takes a connection
  slot and finds no idle pool, so there is at most one per slot; a context takes about 15 ms
  and 0.75 MB to parse the bundle, too much to build all of a large `max_concurrency` up
  front. The client logs what its contexts trust once, at DEBUG, when it is constructed. The
  refresh's downloads build one context per host's pool. botocore's flat-file
  pools build their own contexts. A pool's context does not examine `SSL_CERT_DIR`, so the
  REST client calls `net.http.check_certificate_directories()` once when it is constructed: a
  configured directory that cannot be listed, or an entry of one that cannot be examined,
  logs a WARNING, once per process for each path and error, as the shared context does.
  `websockets` and asyncio never write to a context,
  so the WebSocket clients share one, `net.http.ssl_context()`, built per process and
  certificate setting under a lock. The shared context's certificate
  setting is the values of `SSL_CERT_FILE`, `SSL_CERT_DIR`, and `SSLKEYLOGFILE`, plus the
  modification time, size, and inode of the file and directory the first two name (nothing
  when the path does not exist), so a bundle created, replaced, or edited at runtime, the
  recovery the certificate error messages advise, takes effect for the next client without a
  restart. A context of a urllib3 pool reads the files as they are when it is built. The
  proxy settings do not shape the context, so they are not part of its key. Every context
  offers ALPN `http/1.1`: REST and the WebSocket upgrade are both HTTP/1.1, and live handshakes
  with Massive's REST, real-time, and delayed hosts select it over verified TLS 1.3.
- **Query encoding.** Path parameters are encoded like query values, then percent-encoded
  whole into the template. Values, by the parameter's `ParamKind`: an enum by its value (one of
  `Param.values`), `bool` as `true`/`false` (never for an integer or number), a `date` as
  ISO-8601, an aware `datetime` for a `TIMESTAMP` parameter as an exact integer count of the
  parameter's `unit` (`Param.unit`, set exactly for `TIMESTAMP`: nanoseconds for most,
  milliseconds for the indicators' `timestamp` filters) computed from `timedelta` days,
  seconds, and microseconds (never through a float), and for a `DATE` parameter as ISO-8601
  text in UTC ending in `Z` (`2025-06-02T04:00:00Z`, the form of Massive's examples);
  a non-empty sequence of `str` joined with commas (the `.any_of` form). A naive `datetime`, an
  instant that is not a whole number of the parameter's unit (rounding would move a range
  bound), an empty sequence, and a non-finite number raise `ValueError`; a value of a type the
  kind does not accept raises `TypeError`. `None` values are omitted. A name that is not a
  query parameter of the path, or path parameters other than exactly its placeholders, raise
  `ValueError` before any request. Names arrive already in wire form (`strike_price.gte`); the
  dot mapping of [section 9](09-filters.md#9-filters) happens before the client.
- **Paging.** `pages` follows each page's `next_url`, which carries the cursor and the query.
  A `next_url` whose scheme, host, or port differ from `rest_base_url` raises `WireError` and
  is never requested, and the error quotes it without `apiKey` or user information; any other
  `next_url` is requested on the base URL's origin, its query kept byte for byte without
  `apiKey`. `limit` (at least 1) is the total-row cap: paging stops
  once that many rows have been yielded, and the last page is truncated. Only one page of rows
  is held at a time. A body without an envelope (`result_key` `None`, a list) ends paging.
- **Retries** follow `net/retry.py`. At most 3 retries (4 attempts) of a transient failure;
  every request is a `GET`, so a retry is always safe. HTTP 500, 502, 503, 504 and connection
  errors wait 0.5, 1, then 2 s, each with up to 20% upward jitter. A TLS certificate failure
  (`ssl.SSLCertVerificationError`) is never retried: it raises `WireError` at once, naming
  `SSL_CERT_FILE`. Nor is a proxy's refusal of its credentials (HTTP 407 to the tunnel request,
  `net.errors.is_proxy_auth_failure`): repeating them can lock a corporate account, so it raises
  `WireError(status=407)` at once, naming `HTTPS_PROXY`. A connection reset in the middle of
  paging retries **that page** (its `next_url`), never the first page. Redirects are never
  followed (a 3xx is a `WireError`), so the bearer key never leaves the REST host. Every wait
  goes through an injectable clock (`net.retry.Clock`, whose `wait(event, seconds)` ends early
  when the event is set; the client uses its governor's), so tests never sleep. A `next_url`
  that repeats the target just fetched raises `WireError` rather than paging forever.
- **The governor.** Every attempt, every page included, first takes a permit of its `bucket`
  from the `RequestGovernor` (`rest/governor.py`), in its lane (the caller's `cancel` token,
  or the call itself when it has none), through a claim (`with governor.admission(...)`,
  then `Admission.acquire`), and reports how it ended, exactly once: answered, never sent
  (released: a `ConnectTimeoutError` or a `ProxyError` anywhere in the error's chain, so a
  connection that could not be made or a proxy or tunnel that failed), or refused with HTTP
  429; anything else, an exception on the wire or an interrupt included, is reported as not
  answered by the claim's exit. The governor ignores any report after the first. A slot is
  released on cancellation, failure, and interrupts at wait points; an interrupt inside a
  lock wait, or a second one during the claim's exit, is beyond what Python can make safe. The refusal hook runs before the governor hears of a 429, and the
  governor hears of it even when the hook raises. The governor's rules are those of
  [section 13.4](13-host-resolution-and-connection-protocol.md#134-rest): per bucket, lanes in turn and first in, first out within a lane; a
  paced bucket (declared, or learned from a 429 whose message names the per-minute maximum)
  admits a request only while fewer than 5 slots are held, a slot lasting from the request's
  start until 61 s after its response; a 429 pauses the bucket (its `Retry-After` in either
  form, an HTTP date measured against the response's own `Date` header, up to 10 minutes
  (`MAX_RETRY_AFTER`; a longer one is clamped and logged at INFO), else a whole window
  when paced, else 15, 30, then 60 s by the run's refusals; a `Retry-After` of zero counts as
  none), the burst of 429s in flight when the pause began counting once; after a pause one
  probe request goes alone until it is answered; a run ends with an answer to a request
  started after the latest pause, or a window after its pause ended with no refusal; a 429 is
  not a retry, and the attempt is made again once the governor allows it; the bucket never
  gives up, and a call whose read's `Patience` cannot cover a pause raises
  `WireError(status=429)` naming the bucket's requests, when the next one goes, the patience
  and what it has spent, the usual cause, the remedies (ending with the patience's `advice`, when it
  has one), and the latest refusal. A bucket named `learn_only` at construction
  (`other`) cannot be declared and is paced once a refusal names the limit. A
  request that may start at once with nothing queued before it is granted without queuing,
  and a wait without a deadline is woken only by a change of its bucket, never by a timer;
  the system clock waits it out in half-second chunks, so Ctrl-C ends it on every operating
  system. Every permit is taken through `RequestGovernor.admission`, a claim opened before the
  grant whose exit reports the permit, so no interrupt leaves one unreported, short of one
  inside a lock wait or a second one during the claim's exit, which no Python code can make
  safe. A
  bucket the governor was not built with raises `ValueError`. Before the governor pauses the bucket,
  the client calls its `on_refused(bucket, error, *, basic)` hook (the library's first-429
  WARNING).
- **Cancellation.** Each `get` and each page waits on a `Cancel` token of its own, made from
  the client's closed token and the caller's `cancel`, and closed again when the call ends, so
  nothing keeps a reference to it. `close()` sets the client's token: a thread waiting for the
  governor or a retry stops at once and raises `WireError("... the REST client is closed")`,
  and every later request raises it too; a caller's `cancel` ends the waits of its own requests
  only (`WireError("... cancelled")`), and closing one client ends none of another client's
  waits on the same governor. A request admitted just as its token is set is not sent.
- **Error mapping.** After the retries, the client raises: 401 as `WireAuthError`; 403, and a
  200 envelope with `status` `NOT_AUTHORIZED`, as `WireEntitlementError`; every other non-200
  status (429 included), a 200 envelope with `status` `ERROR`, and a connection failure as
  `WireError`. Each carries the status, the method and path (query without secrets), the
  `request_id`, and the first 200 characters of the server's `message` or body, and its message
  names them all.
- **Strict decoding.** The body is decoded once with the `net.codec` JSON codec. Bytes that are
  not UTF-8, or text that is not JSON, raise `WireError` with the status and the first 200
  bytes, on any call and on any page. So does a document nested deeper than 512 on the
  standard-library path: the codec counts the depth before decoding, so a hostile body can
  never exhaust the C stack of the calling thread (a JVM thread has 1 MB); orjson enforces its
  own limit of 1024. The page's rows come from the `RestPath`'s `result_key: str | None`:
  - **A key** (`results`, or the `RESULT_KEYS` entry such as `last`), possibly dotted
    (`results.values` for the indicators, `results.events` for ticker events): the body is an
    envelope object, and the decoder walks the dots. An object there is one row and a list is
    the rows. A missing (or null) step is an empty page only when `status` is `OK` or
    `DELAYED`, and a `WireError` otherwise. Rows under a key keep every key, `status` included
    (TMX corporate events carry their own).
  - **`None`**: the body is the rows (the generator writes `null` where the OpenAPI response has
    no envelope). A JSON list is the rows, every key kept (`/v1/marketstatus/upcoming` holiday
    rows have a real `status`), with `status=""` and `request_id=None`. A JSON object is one
    row and also the envelope (`/v1/marketstatus/now`, `/v1/open-close/*`, `/v1/conversion`):
    its `status` and `request_id` describe the page, and the `ENVELOPE_KEYS`
    (`status`, `request_id`, `next_url`, `count`, one constant in `spec/types.py` that the
    refresh shares) are removed from the row before drift detection.

  **Envelope status**, for every path: `NOT_AUTHORIZED` raises `WireEntitlementError` and
  `ERROR` raises `WireError`. `OK`, `DELAYED`, and `success` (the "last" endpoints) are the
  documented good statuses, named once (`rest.response.GOOD_STATUSES`). Any other status still
  yields its rows and is reported in `Page.drift.statuses`, which the library logs at WARNING
  once per (path, status). A `status` that is not text is a `WireError`. The `request_id` is
  metadata and never fails a call: Massive's documentation samples send it as text and as a
  number (`1`), so any JSON scalar is kept as text (`"1"`), and an object or list is dropped
  (`None`), in a 200 envelope and in an error body alike. The envelope's `status` and
  `request_id` are kept on the `Page`, so callers can read `DELAYED`.
- **Batches.** `to_batch` turns a page's rows into one Arrow batch of the path's generated
  fields, in long-form column names, and returns its `DriftReport`; it validates every value
  per [section 19.5](#195-runtime-drift). It is the only code that walks a wire path; no row
  object or model class sits between the JSON and Arrow. `required` holds long-form columns
  (`KeySpec.column`); a name that is not a column of the path raises `ValueError`. Arrow
  types: `long` and `epoch` → `int64` (an epoch stays a raw count for `_datasets/timestamps.py`), `double` →
  `float64`, `bool` → `bool`, `string` and `json-text` (compact JSON) → `string`, `date` →
  `date32` (strict `YYYY-MM-DD`), `iso-instant` → `timestamp[ns, UTC]` built from exact
  integer nanoseconds (up to nine fractional digits, integer arithmetic only, never through a
  float or a microsecond `datetime`), `long-array` → `list<int64>`, `string-array` →
  `list<string>`. Values are checked and
  converted by `spec.convert`, which the WebSocket decoder shares. ISO date-time text **without** an offset is an
  unconvertible value, never assumed to be UTC: a field that really is offset-less UTC gets a
  named `TYPE_FIXES` conversion. `DriftReport.missing` holds only the vendor-`required`
  fields that some row lacked or held as null; an extra nested object is reported by its own
  key, not by each leaf. A page is read a column at a time: each field's values are gathered
  into one list, the types of the whole list are checked at once, and a column of plain values
  goes to Arrow without a Python call per value; Arrow checks its numbers in one pass, and a
  column it refuses (an integer outside the 64-bit range, or one a double cannot hold exactly)
  is checked and converted in Python instead. A page that this reading cannot vouch for (a bad
  row, or a value its converter refuses) is read again row by row, which gives the same batch, drift, and error
  as before; the fallback logs its reason at DEBUG, once per page.
- **Logging.** Each request logs at DEBUG: method, path, query (with no secret), status,
  `request_id`, row count, and elapsed time. Retries log at INFO with the wait, and exhausted
  retries at DEBUG: the call that raises the error is what the user sees. The `Authorization` header is never logged.

### 19.2 The WebSocket client

`_massive.WsClient` is one object per connection (one `wss://host/path`), created by `_ingest/ws_connection.py`
and run on its own daemon thread under `asyncio.run`. Its interface and callbacks are those of
[section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol). **Apart from `start()` (called once by
its creator, before the loop thread exists), `close()`, and `join(timeout)`, every method must be
called on the client's loop thread.** All its state is confined to that thread except the
`close()` request flag (a `threading.Event`), and `close()` touches nothing else: it hands its
work to the loop. That is what makes the design correct on free-threaded 3.14t without locks.

- **Connecting.** `websockets.asyncio.client.connect(url, ssl=..., max_size=None,
  open_timeout=None, user_agent_header=net.USER_AGENT)`, the whole handshake (below) running
  under `asyncio.timeout(10)`. `max_size=None` lifts websockets' 1 MiB frame cap; keepalive
  pings stay at websockets' defaults; the proxy comes from the environment through websockets'
  own proxy support; the CA bundle comes from `net.http`.
- **Handshake.** Read the `connected` status, send `{"action":"auth","params":"<key>"}`, and read
  the reply, all within the fixed 10 s handshake timeout, or the attempt fails and is retried.
  `auth_success` opens a generation (`on_open`). `auth_failed` is fatal (`WireAuthError`).
  `max_connections` reports `WAITING_SLOT`, and the attempt ends when the server closes the
  socket (or after the handshake timeout), then waits its backoff. A `max_connections` status
  received after authentication is delivered as a `Status` and also makes the client report
  `WAITING_SLOT`, not `RECONNECTING`, when that socket closes. A greeting other than
  `connected`, a reply other than `auth_success`, `auth_failed`, or `max_connections`, or a
  handshake frame that is not JSON is vendor drift: logged at ERROR with the library-bug text
  and retried. Each backoff, refused handshake, and reconnect attempt logs at DEBUG; the
  library's connection logs one WARNING per outage ([section 17.1](17-diagnostics.md#171-the-log)). The auth frame
  is never logged, by this library or by websockets: every connection is given a logger on
  which DEBUG, the level of websockets' frame log, is disabled.
- **Receiving.** After `auth_success`, `protocol.py` decodes each text frame once, with the
  `net.codec` JSON codec, against the channel's generated fields in `ws.json` and the channel's
  required-key set and symbol column from `set_required` (none when none was set), into
  events delivered in
  order:
  - `on_status(generation, Status)` for each `ev == "status"` object. `Status.kind` is `ACK`
    (`topics` parsed from "subscribed to: <p1>[,<p2>...]", cluster-agnostic, a comma list
    tolerated; empty when the message cannot be parsed), `REFUSED` (`topics` holds the topic the
    message names, or is empty when it names none), `MAX_CONNECTIONS`, or `OTHER`. `message` is
    the server's text verbatim.
  - `on_data(generation, channel, rows, rejects, drift)` for each channel's data objects in
    the frame. Each row is a tuple holding every field of the channel's source, in the order
    of `Source.fields`, whose long-form columns name them (the library renames them), null
    when absent or when the value does not fit; values are validated and converted to their
    Python form (an epoch keeps its integer
    count; an ISO-8601 instant becomes exact integer nanoseconds since the epoch), and symbols
    stay as the wire spells them. A row whose required key is absent, null, or does not fit
    becomes a `Reject(channel, symbol or None, WireError, raw)` instead, with the raw value of
    the declared symbol column (`None` when none is declared or the value is missing); the
    frame's other rows are delivered. `drift` is the frame's `DriftReport`
    ([section 19.5](#195-runtime-drift)). A channel's objects in a frame are read a column
    at a time, with the column reader REST pages use (`spec.convert.column_converter`), from
    four objects on; fewer, or a column holding a value that reader cannot vouch for, are read
    one object at a time. Both readings give the same rows, rejects, and drift.

  No frame text is parsed outside `_massive`.
- **Sending.** `subscribe` and `unsubscribe` put their frames
  (`{"action":"subscribe","params":"T.AAPL,Q.AAPL"}`, at most 1,000 topics each) on one sender
  coroutine's queue, which sends them in order as soon as it runs: no change waits for a frame
  to arrive. Frames for a generation that is no longer current are dropped.
- **Reconnecting.** After every close and every failed attempt that `close()` did not request
  and that is not fatal (a server's clean close included), the client waits and tries again,
  forever. The first wait is `reconnect_backoff` (at least 15 s, [section 5](05-massiveconfig.md#5-massiveconfig));
  each further consecutive failure doubles it up to `max_reconnect_backoff`; jitter adds up to
  20%, clamped, so at the cap the wait is drawn from
  `[max(reconnect_backoff, max_reconnect_backoff / 1.2), max_reconnect_backoff]` and every wait
  lies in `[reconnect_backoff, max_reconnect_backoff]`. The failure count and the backoff reset
  only after a generation has stayed open for 60 s (stable uptime), never on `auth_success`
  alone. The client reports `RECONNECTING` with the next attempt's time before each wait.
- **Fatal and retryable failures.** `protocol.py` classifies an exception from `connect()`:
  - an HTTP 429 at the upgrade is retryable, and its wait is the larger of its `Retry-After`
    and the backoff, with the `Retry-After` capped at `max_reconnect_backoff`, so every wait
    still lies in `[reconnect_backoff, max_reconnect_backoff]`. This differs from REST, whose
    reads give up once a pause outlasts their patience, because a caller is blocked on the
    request: the WebSocket client never gives up, and an unbounded `Retry-After` would
    leave its streams silent for as long as the server named;
  - a TLS certificate failure (`ssl.SSLCertVerificationError`) is fatal, naming
    `SSL_CERT_FILE`, although it is an `OSError`;
  - everything else follows websockets' public rule, `websockets.client.process_exception`
    (the floor is websockets 17.1, its first release there): what it returns `None` for (EOF, `OSError`, timeouts, HTTP 500/502/503/504) is retried, and
    anything it returns (for example HTTP 400 or 404, an invalid URI) is fatal.

  A fatal error ends the client and is reported as `FAILED` with `WireError(status=<http>)`
  naming the host.
- **Closing.** `close()` sets the request flag and calls `loop.call_soon_threadsafe` with a
  callback that cancels the task once, so a second `close()` cannot interrupt the socket's
  closing (a no-op once the loop has ended; the task checks the flag when it starts, so a
  `close()` before `start()` also ends it). The loop is created at construction, so
  `WsClient.loop` is valid before `start()`. Cancellation interrupts whatever the task awaits, a backoff
  wait included; a `finally` block closes an open socket with code 1000 and reports `CLOSED`.
  `close()` never waits; `join(timeout)` waits for the thread with a bound.
- **Logging.** INFO on connect, authentication, and close; WARNING on `max_connections` and on
  each reconnect wait (with its length); ERROR on a fatal error.

### 19.3 The flat-file client

`_massive.FlatFileClient(access_key_id, secret_access_key, endpoint_url, bucket, max_pool,
clock=SYSTEM_CLOCK)` is one object per session, created by the session's first flat-file read
(only after the source choice has required both credentials, which are therefore `str`) and
closed by `m.close()`; `clock` is the `net.retry.Clock` its retry waits use. A request takes
one of `max_pool` connection slots (`net.slots.ConnectionSlots`), and an open object keeps
it until it is closed. Each slot has a boto3 S3 client of its own, with a pool of one
connection, built lazily from one boto3 session, under the client's lock, the first time a
request takes a slot and finds no idle client; taking a slot is the only way to get a client,
and the client goes back with the slot. So no `ssl.SSLContext` is used by two threads at once:
urllib3 sets the ALPN protocols on the context of every connection it opens (and botocore has
it load the CA bundle), while another thread's handshake may copy that state with the GIL
released, a use-after-free in CPython (python/cpython#141012, unfixed in every release, the
free-threaded build included) that corrupts the handshake (`TLSV1_ALERT_DECODE_ERROR`) or
crashes the process. The client keeps a closed flag (a `threading.Event`); once closed, it
makes no request, and `close()` closes every client built. A client given back after the
close (by a request in flight at the close, as it returns or as its object is closed) is
closed again. Every S3 client in the process is built on one shared botocore model loader, so
botocore parses its S3 model files once per process instead of once per client; the loader
stays in memory for the life of the process, and its lock serializes client builds (section
18).

- **`open(key) -> FlatFile | None`.** `None` for a missing key: HTTP 404 whose body is S3's
  XML error with the code `NoSuchKey`. Any other 404 (another S3 code, no code, or a body that
  is not S3's, such as the HTML page Massive's front end serves for a path it does not route,
  which botocore reads as the code `404`) is a `WireError` naming the bucket and the key, so a
  wrong bucket or endpoint never reads as a run of missing files. `object_key(prefix, day,
  venue)` builds keys as `<prefix>/<YYYY>/<MM>/<date>.csv.gz`, filling a `{venue}` template.
  The generator folds the four concrete futures overview prefixes into that template using
  `VENUES` (`cme`, `cbot`, `nymex`, `comex`, each with its reason) in `refresh/overrides.py`,
  which a library test checks against `FuturesVenue`.
- **Error mapping.** An S3 error code `InvalidAccessKeyId` or `SignatureDoesNotMatch` is
  `WireAuthError`; any other 403 is `WireEntitlementError`; anything else is `WireError`. Each
  carries the status, the host, and the key, and `request_id` comes from `x-amz-request-id`.
  A wire message says what happened to which object (`HTTP 403 for s3://<bucket>/<key> at
  <endpoint> (<code>: <message>)`); the user-facing remedy (the `MassiveConfig` settings to
  check, the `venues=` parameter) is added by `_ingest/flatfiles.py`, whose public texts are
  those of [section 16](16-historysourceauto.md#16-historysourceauto). Credentials are never logged, and a message
  names a proxy by its URL without the user and password (`net.http.proxy_credentials`), never
  by botocore's masked form, which leaves a password in clear text when it also appears earlier
  in the URL. One documented exception, for a third-party logger at DEBUG only: botocore's own
  DEBUG records, emitted only when someone explicitly enables DEBUG on the `botocore` logger,
  may show the S3 access key id and the request signature (its `Authorization` header). They
  never show the secret access key or the proxy password, and a test checks both, the
  password's base64 form included, across every record.
- **TLS and proxies.** The client is built as `session.client("s3", verify=<the net.http CA
  bundle>, config=Config(proxies=<from net.http>, proxies_config={"proxy_ca_bundle": <the same
  bundle>}, retries={"total_max_attempts": 1, "mode": "standard"}, connect_timeout=60,
  read_timeout=60))` on a botocore session whose `defaults_mode` is `legacy`. The CA bundle is
  `SSL_CERT_FILE`, else certifi (`SSL_CERT_DIR` does not apply to S3), and it also verifies an
  HTTPS proxy, as the REST client's context does. `proxies` is `{"https": <url>}`, or `{}` when
  no proxy applies: `None` would make botocore look the proxy up itself, under the leaf lock and
  from more variables (`HTTP_PROXY`, `ALL_PROXY`) than the other clients read. The bundle and
  the proxy are read when the first `open` creates the boto3 client, outside the leaf lock,
  because on macOS the system proxy exceptions can resolve the endpoint's host name. The
  `legacy` defaults mode keeps `AWS_DEFAULTS_MODE=auto` from asking the instance-metadata
  service for a region under the lock.
- **Retries.** botocore's own retries are off, and the mode is pinned so `AWS_RETRY_MODE=adaptive`
  cannot add botocore's client-side rate limiter, which sleeps outside the injected `Clock`.
  The shared `net/retry.py` schedule (0.5, 1, 2 s plus jitter, waited on the client's `Clock`,
  which `close()` interrupts) retries a failure before the object's first byte:
  - the `TRANSIENT_STATUSES` (HTTP 500, 502, 503 `SlowDown`, 504) and the S3 error code
    `RequestTimeout` (HTTP 400);
  - connection errors: botocore's `ConnectionError` family, `ConnectionClosedError`, and
    `ReadTimeoutError`, but not the bare `HTTPClientError` botocore raises for any unexpected
    exception in its HTTP layer;
  - an error response whose body is reset or stalls: botocore reads that body outside its own
    error handling, so urllib3's `ProtocolError` and `ReadTimeoutError` reach the client
    unwrapped.

  Each retry is logged at INFO and giving up at DEBUG, naming the object and the failure;
  the call that fails is what the user sees.
  Three failures are never retried:
  - A TLS failure (botocore's `SSLError`, or any error `is_tls_failure` recognises). A
    certificate failure's message names `SSL_CERT_FILE`.
  - A proxy's refusal of the client's credentials (HTTP 407 to the tunnel request, which
    `net.errors.is_proxy_auth_failure` recognises). Repeating credentials the proxy refused can
    lock a corporate account; the `WireError` has status 407 and names `HTTPS_PROXY`.
  - A failure partway through an object's body. The reader has already handed blocks on, so
    the client cannot repeat the request by itself: the read fails with a `WireError`, and the
    file's `dropped` flag is set. `_ingest/flatfiles.py` keeps a file's rows to itself until
    the file ends, so it reads such a file again from its start: it frees the file's partial
    tables (it clears the frames the error's traceback holds, `_gather`'s and the batch
    generator's, which keep the failed read's locals alive) and calls `wait_to_reread(error, retry, cancel)`, which waits on the shared
    schedule (logged at INFO), and returns `False` once `MAX_RETRIES` rereads are spent (logged
    at DEBUG); the library then raises the failure. A wait whose `cancel` is already set, or on
    a closed client, returns at once without a log line. `close()` ends such a wait at once by
    setting `cancel`, as a failed or interrupted call does: `Clock.wait` waits on one event,
    and the call's event is the one that stops the call. There is no `Range` resume: a
    whole-file reread needs no byte bookkeeping and can repeat no row.
- **`prefixes()`** lists the bucket's top-level prefixes, for `--check`.

`FlatFile(body, key, location, host)` reads any binary stream whose failed reads raise
`OSError`; the client wraps the response body so, turning botocore's read errors and the
urllib3 errors botocore lets through (a TLS failure while reading the body is urllib3's
`SSLError`) into `OSError`, and the reader imports no botocore.
`FlatFile.dropped` is `True` once a read of that stream has failed (the connection dropped or
stalled part-way through the body). It tells a failed transfer from a broken file: only the
first is worth reading again.
`FlatFile.batches(plan)` yields one `(RecordBatch, DriftReport)` per block, in this order:
(1) the symbol filter (`is_in`); (2) the key check: a row whose `required` header is empty or
does not parse, or whose time `plan.to_ns` cannot place, is dropped and counted, and a file
whose every row of the requested symbols is dropped raises after its last block; (3) `[start, end)` on `plan.to_ns(time column)`; (4) on the
survivors, the conversion of every header from text to its type (a value that does not parse
is null and reported in `unconvertible`), list parsing, and the rename to long-form names.
Instants stay in their wire unit; the library converts them. The reader first peeks the
header line and reads by name ([section 16](16-historysourceauto.md#16-historysourceauto)). `close()` is idempotent; the
reader thread that iterates the file closes it.

```python
ReadPlan(columns: tuple[ReadColumn, ...], symbol_header, symbols, time_header, window_ns,
         to_ns, block_size: int, chunk_size: int, parse_threads: int, required)
ReadColumn(header, column, read_type: pyarrow.DataType, list_type)
```

`_ingest/flatfiles.py` builds one plan per call. `block_size` is the reader's block of
decompressed text, a required field with no default;
`_ingest/flatfiles.py` always passes its `BLOCK_SIZE` (1 MiB). `chunk_size` and `parse_threads`
are how the reader parses a file in parallel: `_ingest/flatfiles.py` passes 16 blocks
(`CHUNK_BLOCKS`) and 3 (`PARSE_THREADS`).

`_ingest/flatfiles.py` builds the plan from `source(Transport.FLAT_FILES, prefix)`. `to_ns` is the
`_datasets/timestamps.py` routine, returning the nanoseconds, null where a value fails, and the first
failure's message, so a wrongly declared unit drops every row and fails the file; the window is
never converted into wire units.

### 19.4 Generated schemas

No schema is hand-written. The refresh generates three committed, canonical files from the
vendored snapshot: `spec/rest.json` (from `openapi.json`), `spec/ws.json` (from
`websocket.json`), and `spec/flatfiles.json` (from the flat-file "Columns" tables). Each holds,
per source, an ordered list of fields `(wire key, column, WireType, unit, required)`. Every
change shows up as a reviewed diff of a generated file. Hand input is naming, grouping, and
verification, never a column list.

**Sources and flattening.**

- **REST.** Every `openapi.json` path that is not deprecated (`x-polygon-deprecation`) or
  draft (`x-polygon-draft`) and not in `EXCLUSIONS`; `x-polygon-ignore` neither excludes nor
  generates a path ([section 8.12](08-namespace-tree.md#812-excluded-endpoints)). The generator resolves `$ref`,
  merges `allOf` (into an object when any part is an object or no part has a type, else into
  the one scalar type its typed parts share, two different scalar types being a `shape`
  problem; three crypto response bodies put the status field's string schema beside the
  object part, and stay objects), treats a `oneOf` or `anyOf` with exactly one branch as that branch, flattens
  nested objects into dotted wire paths below the result key (`results` unless `RESULT_KEYS`
  names another key, a dotted path of keys such as `results.values`, or `None` for a body that
  is itself the rows), keeps fields in the order the vendor documents them, then moves the price
  columns first (see the naming policy, rules 6a and 6b), and records the parameters (wire name,
  `ParamKind`, enum values, path or query, the unit of a `TIMESTAMP`, and the default the spec
  gives the parameter, as a JSON scalar or `null`). A JSON-object body
  without a result key is one row, and its `ENVELOPE_KEYS` are left out of the fields; one of
  those keys whose type is not the envelope's (`status`, `request_id`, `next_url` strings,
  `count` an integer) stops the refresh, since dropping it could lose data. A response with no
  rows where its result key says stops the refresh until a `RESULT_KEYS` entry says where they
  are.
- **REST parameters.** A name ending in `.any_of`, or an array, is `LIST` (sent comma-joined,
  even when its values come from an `enum`); any other `enum` is `ENUM`; booleans, integers, and numbers are `BOOLEAN`, `INTEGER`, and `NUMBER`; a
  timestamp-like string parameter (base name `timestamp`, `*_timestamp`, `updated`,
  `*_updated`, `window_start`, `window_end`, or `*_time`, the names of the `timestamp-unit`
  gate) takes its `PARAM_UNITS` entry: a unit makes it `TIMESTAMP` in that unit (`Param.unit`), and "not an
  epoch" makes it `DATE`; a string with `format: date` or `date-time`, a choice of formats, a
  base name `date`, `*_date`, `*_utc`, or `as_of`, or a description that gives the date format
  `yyyy-mm-dd` (any letter case) is `DATE`; any other string is `STRING`. The description is
  the vendor's evidence where the schema is a bare string: Massive types its date filters
  `{"type": "string"}` and states the format only in prose ("Value must be formatted
  'yyyy-mm-dd'"), whatever the parameter's name (`period_end`, `active_from`, `published`).
  The `.gt`, `.gte`, `.lt`, and `.lte` variants share their base name's entry.
- **WebSocket.** Every `websocket.json` channel not in `EXCLUSIONS`, keyed `(cluster,
  channel)` and spelled `"<cluster>/<channel>"` as its `Source.key` (`"stocks/T"`,
  `"futures/AM"`), since stocks, options, and futures share channel names. Fields are its
  message properties in spec order; `required` is copied where the vendor gives it.
- **Flat files.** Every dataset page with a row in its class's overview table. That row's
  `S3 <prefix>` is the source key, so no hand-written page-to-prefix map exists. A futures kind
  keyed by `{venue}` requires its four venue pages to list identical columns. Fields are the
  page's "Columns" table (name, type, description), in the order of the page's "Example Data"
  header row, which must name exactly the table's columns (case aside: some pages write
  `Ticker`): the vendor's tables list the columns in an order of their own (alphabetical on
  most pages), while the example header is the files' own column order (the stocks
  aggregates' `ticker, volume, open, close, high, low, window_start, transactions`). The
  example rows go through the `example-type` gate, each cell read as the file's reader reads
  it (an empty cell is null; a cell of a text or date column is text; any other cell is a
  number when it reads as one, else text). A dataset page with no overview row is reported as
  unaccounted by `--check`.
- **Representation rules** (generator rules, not hand entries): an array whose items the
  scalar rules make a `long` becomes `long-array`, and one whose items are plain `string`
  becomes `string-array`; an array of anything else (objects, other numbers, instants, dates,
  arrays), a free-form object, and `oneOf`/`anyOf` become `json-text`; the WebSocket envelope key `ev` is dropped; a field with a `UNITS` entry becomes
  an `epoch` in that unit; a string with `format: date-time`, or no format and a name ending in
  `_utc`, becomes an `iso-instant`; a string with `format: date`, or named `date` or `*_date`,
  becomes a `date`.
- **Projections.** `PROJECTIONS {(kind, source): Projection}` changes how a source joins
  one kind, **inside one membership only**, in one of two forms. `fields` keeps only the
  listed keys, under the listed long names; no membership uses it since the indices snapshot
  left the values kind ([section 7.5](07-query-classes-and-verbs.md#75-one-row-datasets)). `renames`
  keeps every field and renames only the listed ones, so a field the vendor adds later joins
  under its own name: the forex last quote and the crypto `XQ` stream join the quotes kind
  with `ask`/`ap` as `AskPrice` and `bid`/`bp` as `BidPrice`. The generator writes each long
  name through the naming rule, and the recorded projection maps every wire key it keeps to
  its PascalCase column, the same vocabulary as `Field.column`. A member whose projection
  renames is ordered, and gives the order, like an unprojected one (rule 6a); a member that
  keeps only some fields does neither, and its source keeps its own order.
- **Memberships are written into the generated files.** `KINDS` and `PROJECTIONS` are generator
  inputs in `refresh/`, which is not shipped. The generator writes each source's memberships
  onto its committed entry in `spec/rest.json`, `ws.json`, or `flatfiles.json`, and the `spec`
  loader exposes them as `Source.memberships: tuple[Membership, ...]`, where
  `Membership(kind, group, position, projection: Mapping[wire_key, column] | None)`: the
  kind, the asset-class group, the source's order within the group, and its projection. A
  source in no kind has no memberships; a source may hold several. Everything at run time, the
  merge and the kind-membership gate included, reads memberships from there and never from
  `refresh/`. Drift detection stays per source, against the full fields, so projected-away fields
  are never reported as drift.

**The type lattice.** `WireType` is one of `long`, `double`, `bool`, `string`, `date`, `epoch`
(an integer with a unit), `iso-instant`, `long-array`, `string-array`, and `json-text`. OpenAPI
`integer`, and `number` with `format: integer`, `int32`, or `int64` (the option chain's quote
exchange ids), map to `long`, and `number` to `double`. Doc pages map
`integer` to `long`, `number` to `double`, `string` to `string`, and `timestamp - integer` to
`epoch` (with its `UNITS` entry). Two types meet for one column name within one kind by these
joins: `long ⊔ double = double`; `long ⊔ long-array = long-array` (a scalar becomes a
one-element list); `epoch ⊔ epoch = epoch`, each source keeping its own unit. Any other pair is a
**type conflict** (the `type-agreement` gate). `WireType.common_type` is the one
implementation of these joins: the refresh's `type-agreement` gate and the run-time merge
both call it, so a kind the refresh accepts always merges. `_datasets/schema.py` maps `long` to `long`, `double` to
`double`, `bool` to `boolean`, `string`, `json-text`, and `date` to `String` (a date as ISO
`yyyy-MM-dd` text), `epoch` and
`iso-instant` to `Instant`, `long-array` to `long[]`, and `string-array` to `String[]`. An `iso-instant` is ISO-8601
date-time text with seconds and an offset (`Z` or `±HH:MM`), decoded to exact integer
nanoseconds since the epoch; text without an offset, or an instant outside the signed
64-bit nanosecond range (1677 to 2262), is an unconvertible value, never assumed UTC or
clamped. A `long-array` element outside the
`int64` range, and a `string-array` element that is not text, is an unconvertible value; a
null element is kept, as Deephaven's null in the array.

**Hand input.** All of it lives in `refresh/`, every entry carries a reason, and an entry that
names a source or field the snapshot lacks fails the refresh as stale (`stale-entry`).

| Kind | Where | What | Example |
|---|---|---|---|
| Lexicon | `lexicon.py` | `SHAPES` maps each object shape (aggregate row; snapshot `day`, `min`, `prevDay`, `lastTrade`, `lastQuote`; each WebSocket channel; each futures object) to its lexicon. A lexicon translates a short key to the long name Massive uses for the same field elsewhere in that asset class's REST or flat-file data, and **every entry cites that endpoint or file**; identity entries cover short real words (`id`, `cik`, `url`, `low`, `fmv`, `mic`). | trade lexicon `p` → `price`, `t` → `sip_timestamp` (`/v3/trades/{stockTicker}`); crypto trade `t` → `participant_timestamp` (`/v3/trades/{cryptoTicker}`); forex quote `a` → `ask_price` (`/v3/quotes/{fxTicker}`); segment `min` → `last_minute` (`/v3/snapshot`) |
| `NAMES` | `overrides.py` | `{(source, wire key): (long name, description)}` only for a short key Massive gives no long name anywhere: the name is derived from Massive's own description of the field, which the entry quotes exactly; a changed description makes the entry stale (`stale-entry`). `NAMES` never renames something the vendor already names, and never coins a dataset key column. | snapshot `min.av` → `accumulated_volume` ("The accumulated volume."), `min.dav` → `accumulated_decimal_volume`; WebSocket `av`, `dav`, `op` |
| `KINDS` | `overrides.py` | `{kind: {group: [sources]}}`: the sources of one dataset kind, grouped per asset class, in source order. Every dataset with a stream or flat-file source has a kind, WebSocket-only `luld`, `imbalances`, and `fmv` included. | `trades: {stocks: [/v3/trades/{stockTicker}, /v2/last/trade/{stocksTicker}, (stocks, T), us_stocks_sip/trades_v1], crypto: […]}` |
| `PROJECTIONS` | `overrides.py` | see above | indices snapshot → values, `last_updated` → `timestamp`; the forex last quote and the crypto `XQ` stream → quotes, `ask`/`ap` → `ask_price`, `bid`/`bp` → `bid_price` (2026-10-27: their sources spell the prices as Massive's last-quote objects do, and the quotes table keeps one column per field) |
| `VENUES` | `overrides.py` | the futures venues, folded into the `{venue}` template | `cme`, `cbot`, `nymex`, `comex` |
| `UNITS` | `overrides.py` | the verified unit per `(source, wire key)`, `unverified-ms` where [section 23](23-open-verification-items.md#23-open-verification-items) says so, or "not a timestamp" (a timestamp-like field that is not an epoch, kept as it is); with the tag-conflict list | the 20 indicator `IMicroseconds` tags are ms |
| `TYPE_FIXES` | `overrides.py` | a field whose documented samples its declared type does not fit (a wrong or missing type, or a second form the vendor sends): one named `Conversion`, field-scoped, that the decoder applies deliberately (see **Named conversions** below), each citing the sample; each is exercised by a fixture test, and one the vendor's own docs do not show goes into section 23 | every row of the conversions table below |
| `KEY_FIXES` | `overrides.py` | `{(source, declared key): (wire key, reason)}`: a field the specification declares under one key while every documented sample (the response example and the documentation sample) sends it under another that nothing declares; the field is read under the samples' key, keeping its declared type and description, and every other hand entry names it by that key. Stale once the declared key is gone, or the example no longer sends the corrected key or sends the declared one; each goes into section 23 | ticker events `event_type` → `type` (column `Type`) |
| `DESCRIPTION_FIXES` | `overrides.py` | `{(source, wire key): (vendor, text or asset classes, reason)}`: a field, or a nested object whose description every field below it carries as a note, whose vendor description is clearly wrong for its source (another asset class's or event's text copied onto it, or a statement the vendor's own documentation contradicts), or missing where a sibling source describes the same field (the entry then quotes `""`); the entry quotes the vendor's text (as plain text, after normalization) and gives the replacement, either `text` for every use of the source, or, for a source that serves several asset classes, `asset_classes`: the text a dataset of each listed class shows, every other class keeping the vendor's. The replacement is Massive's own wording for the same field of a sibling source wherever that is right and says what the field holds; it is written here only when no sibling text is adequate, and the reason says so. Only that `(source, wire key)` changes: the field keeps its name and type, and every other source keeps its own description. Stale once no generated field looks the entry up (its source or key is gone, or the key is one a `KEY_FIXES` entry replaces or only an example sends), or the vendor's text no longer matches the quote, so a rewritten description is reviewed again | the crypto last trade's and trades flat file's `price` ("actual dollar value per whole share", stock text) → the price in the pair's second currency; the stocks last trade's `y` ("when the quote was actually generated") → "when the trade was actually generated"; the futures trades' `price` ("100 contracts × $2.00 = $200.00", no multiplier) → the `futures/T` channel's text; the unified `/v3/snapshot` `last_trade.price` → the options trades' text, for `options` only; the futures snapshot's `product_code` (no description) → the `/futures/v1/contracts` text; the unified `/v3/snapshot` object `last_trade` ("The most recent quote…") → the options chain snapshot's "The most recent trade…"; its object `last_minute` ("…for this stock") → "…for this contract", for `options` only, written in the entry since no sibling has a minute aggregate |
| `UNDECLARED_FIELDS` | `overrides.py` | `{(REST path, wire key): (JSON type, description, reason)}`: a field Massive sends that its specification does not declare, seen in a live response; the generator reads it as if declared after the last declared field of its object (lexicon name, representation rules, `UNITS` for a timestamp, the object's notes), never vendor-required. Stale once the specification declares the key, the path is gone, or its object declares no field; each goes into [section 23](23-open-verification-items.md#23-open-verification-items) | options trades `decimal_size` and last trade `ds` (`DecimalSize`); forex snapshots and movers `day.vw`, `min.vw`; unified snapshot `session.vwap`, `session.last_updated`, `last_minute.last_updated`; Form 3 and Form 4 `issuer_trading_symbol` (item 76) |
| `OPTIONAL_FIELDS` | `overrides.py` | `{(source, wire key): reason}`: a field the specification marks required that a live response omitted; the generated field is not vendor-required, so its absence is no drift. Stale once the specification stops marking it required | options last trade `y` (item 76) |
| `IGNORED_KEYS` | `exclusions.py` | `(source, wire key, reason)`: an undocumented key Massive sends that carries nothing a user needs; the refresh writes it onto the source (`Source.ignored_keys`, in the schema file only where a source has some), and the decoders drop it without reporting drift ([section 19.5](#195-runtime-drift)). Stale once the source declares the key or is gone. A key seen only with a constant value, whose meaning nobody documents, stays drift instead, so a real value is never hidden | financial balance sheets and cash-flow statements `max_ticker`, `min_ticker` (item 77) |
| `SAMPLE_ERRORS` | `exclusions.py` | `(source, wire key, sample, reason)`: a documented example value that is wrong while the type is right, or a key a WebSocket example sends that the message does not declare, which the `example-type` gate skips | IPO `us_code` example `37833100`, a CUSIP that lost its leading zero |
| `TRANSPORT_ONLY` | `overrides.py` | `{(kind, group, column): reason}`: columns legitimately on one transport within one group | `(bars, stocks, AccumulatedVolume)` (WebSocket only) |
| `RESULT_KEYS` | `overrides.py` | the paths whose rows are not under `results`: another key, a dotted path of keys, or `None` for a body that is itself the rows | `/v1/last/crypto` → `last`; indicators → `results.values`; ticker events → `results.events`; `/v1/marketstatus/now` → `None` |
| `PARAM_UNITS` | `overrides.py` | `{(path, base parameter): (unit, reason)}`: the unit of every timestamp-like string parameter, and of any other string parameter documented as taking an epoch, or "not an epoch" (sent as date or date-time text) | `/v3/trades` `timestamp` → ns; indicators `timestamp` → ms; aggregate range `{from}`/`{to}` → ms; benzinga `last_updated` → not an epoch |
| `EXCLUSIONS` | `exclusions.py` | `(kind, item, reason)`, the executable rows of section 8.12 | `/stocks/filings/vX/form-5` |

**The naming policy** (owner-approved), applied mechanically by the generator, with no
per-column exceptions:

1. **The vendor's long name.** Where Massive's data carries a long field name (REST
   `/v3/trades` `participant_timestamp`, flat-file `ask_price`), the column is its PascalCase
   form (`ParticipantTimestamp`, `AskPrice`).
2. **Abbreviated keys** (WebSocket `p`, aggregate `o`, forex `a`) take the long name Massive
   itself uses for the same field elsewhere in that asset class's REST or flat-file data. The
   lexicons are exactly this mapping, one per object shape, and every entry cites the vendor
   endpoint or file it comes from. One field gets one column on every endpoint and transport:
   crypto trade `t` is `ParticipantTimestamp`, forex quote `a`/`b` are `AskPrice`/`BidPrice`.
3. **No long name anywhere** (rare: snapshot and WebSocket `av`, `dav`, `op`): the name is
   derived from Massive's own description of the field and recorded in `NAMES` with the
   description quoted. `NAMES` holds only these entries, and an entry names only the key's own
   segment; its parents are named by rules 1 and 2.
4. **Nested objects.** A child column is the parent's name followed by the child's, with no
   exceptions: `day.o` → `DayOpen`, `min.av` → `LastMinuteAccumulatedVolume` (the v2
   snapshot's `min` is the object `/v3/snapshot` and the futures snapshot call `last_minute`),
   `lastQuote.P` → `LastQuoteAskPrice`.
5. **Dataset keys.** The only renames on top of rules 1–4 are each dataset's declared key
   columns, `Symbol`, `Timestamp` (a bar's `TimestampStart`), `TimestampEnd`, `Underlying`, and
   `Date`, made by the run-time key rename. A vendor field literally named `symbol` (the
   open-close and conversion paths) is generated as `Symbol`, its own name, and the key rename
   uses it as is. The library owns `Timestamp`, every `Timestamp<Qualifier>`, and, on tick, bar,
   and daily-summary datasets, `Date`: `NAMES` may never coin one (the `column-collision` gate,
   `naming.is_key_column`), and a catalog test requires every such column of a dataset to come
   from a declared key or from the library's time columns, never from a vendor field that merely
   carries the name. The test lives with the catalog because the roles are declared in
   `_datasets`, which `_massive` cannot import. There are no `Ticker` or `Pair` stand-ins.
6. **Order.** Each generated file's field lists record the column order explicitly, set in two
   steps.

   6a. **Vendor order.** Columns first take the vendor's natural order. Massive's REST documentation lists every object's fields
   alphabetically (the specification's keys, its pages' response-attribute tables, and their
   sample responses all agree), so a REST source keeps that documented order, and a
   REST-only dataset's columns are alphabetical. The sources of each group of a dataset kind
   take their order from the group's flat-file "Example Data" headers and WebSocket messages,
   which list fields in their vendor's order (the stocks aggregate files: `ticker, volume,
   open, close, high, low, window_start, transactions`), then the group's other sources' own
   fields; every member that keeps all its fields (unprojected, or with a projection that only
   renames) takes that order. A group with no flat-file or WebSocket member keeping all its
   fields fails the refresh (`column-order`). The snapshot stores every document in the order served, never re-sorted. The
   dataset's key columns come first at run time.

   6b. **Price columns lead** (owner-approved, 2026-10-30). One rule, after the vendor order,
   orders every dataset of every asset class and transport alike, so a table opens on its
   prices, and two feeds of one kind put them in the same place:
   1. The key columns (`Symbol`, `Date`, `Timestamp*`, `OptionRoot`, `RawSymbol`) come first,
      unchanged; the generator never moves a field the library treats as a key column.
   2. A column's group is its nested-object parent, known from its long-name path: the top
      level, `Day`, `PrevDay`, `LastMinute`, `LastTrade`, `LastQuote`, `Session`,
      `UnderlyingAsset`, and so on.
   3. Each lead field moves to the start of its group, in this order: `Price`, `Size`,
      `BidPrice` or `Bid`, `BidSize`, `AskPrice` or `Ask`, `AskSize`, `Open`, `High`, `Low`,
      `Close`, `Volume`, `Vwap`, `Value`. A field leads only when its own name, without its
      parents', is exactly one of these: `DayClose` leads the `Day` group, while
      `DayDecimalVolume` and a top-level `PreviousClose` do not lead.
   4. Every other `Bid<X>` moves to just before its `Ask<X>` partner in the same group
      (`BidExchange` before `AskExchange`). Nothing else moves.

   So bars read `Open High Low Close Volume Vwap …`, trades `Price Size …`, quotes
   `BidPrice BidSize AskPrice AskSize BidExchange AskExchange …`, and snapshots
   `DayOpen DayHigh DayLow DayClose DayVolume DayVwap …`. The refresh applies the rule last
   (`naming.lead_order`): to each kind group's order, and to every REST source as generated,
   so a source in no kind follows it too.
7. **Consistency.** Within one dataset kind, one column for two concepts fails the refresh
   (the `concept-collision` gate); one concept always gets one column, by rules 1–4.

Names are never derived from descriptions except under rule 3, and the generator emits
`Timestamp` only where the vendor's long name is `timestamp`.

**Cross-table naming.** Massive sometimes spells one concept differently on different
endpoints, for example `day.open` and `session.open`, `ask` and `ask_price`, or a nested
`details.strike_price` and a top-level `strike_price`. Within one table every field has
exactly one column. Across tables, each table keeps the spelling of its own endpoint, so its
columns match Massive's documentation for it. Unifying the names across tables (about 30
concepts, 59 REST columns) and a documented "same concept as" map were both considered and
declined: the canonical names would shift with Massive's endpoint mix, some concepts look
alike but differ (a futures session is not a calendar day), and no automatic check can find
synonyms reliably.

**Refresh gates.** The refresh fails, printing the gate, the source, the key, and its
description for the reviewer. Every gate has a descriptive name, which is how the code
(`refresh/gates.py`, `Gate`), its messages, and its tests refer to it. It fails on:

- **`short-key`:** a short key (three characters or fewer, no underscore) with no lexicon,
  identity, or `NAMES` entry;
- **`column-collision`:** two keys of one source producing one column, or a `NAMES` entry
  coining a library column (`Symbol`, `Underlying`, `Date`, `Timestamp`, or any
  `Timestamp<Qualifier>`);
- **`timestamp-unit`:** a timestamp-like field left to a default. It runs on the snake case of
  the long name, after the lexicon and `NAMES` (so `serverTime` is `server_time`): a field is
  timestamp-like when it has a unit tag, a doc type of `timestamp - integer`, or a long name
  matching `timestamp`, `*_timestamp`, `updated`, `*_updated`, `window_start`, `window_end`, or
  `*_time` (the bar lexicon gives `s` as `window_start` and `NAMES` gives `e` as
  `end_timestamp`, while a trade's `s` is `size`). A timestamp-like `long` or `double` needs a `UNITS` entry (a unit,
  which only an integer may take, or "not a timestamp"); timestamp-like text with no `format`
  needs a `UNITS` "not a timestamp" entry or a `TYPE_FIXES` entry; text with a `format` is
  decided by the representation rules. A timestamp-like string REST parameter (by its base
  name) with no `PARAM_UNITS` entry fails the same gate;
- **`stale-entry`:** a stale hand entry: a lexicon shape, `NAMES`, `UNITS`, tag-conflict,
  `TYPE_FIXES`, `KEY_FIXES`, `DESCRIPTION_FIXES`, `PROJECTIONS`, `KINDS`, `RESULT_KEYS`,
  `PARAM_UNITS`, or `EXCLUSIONS` entry naming a source, field, or parameter the snapshot lacks,
  or claiming what the snapshot contradicts (a type fix whose declared type the spec no longer
  gives, a key fix whose key the example no longer sends in place of the declared one, a tag
  conflict the tag no longer contradicts, a `NAMES` or `DESCRIPTION_FIXES` quote the
  description no longer matches, an exclusion of a path the spec already marks deprecated or
  draft); an identity entry no generated object uses is stale too, and so is a
  `DESCRIPTION_FIXES` entry no generated field looked up, which is checked against the keys the
  generators looked up rather than every key a source declares or an example sends;
- **`type-agreement`:** within one kind, one column name joins to one `WireType`, unless a
  `TYPE_FIXES` entry resolves it; the message prints both keys' descriptions, because a
  conflict is first treated as a possible naming error (stocks and futures `dv` are exactly
  that);
- **`transport-agreement`:** within one `KINDS` group whose sources span two or more
  transports, every column appears on at least two transports or is in `TRANSPORT_ONLY`,
  which catches a one-sided lexicon slip (`s` becoming `Shares` while REST says `size`);
- **`venue-agreement`:** a futures kind whose venue pages differ;
- **`column-order`:** a source whose column order cannot be taken from its kind: its
  `KINDS` group has no flat-file or WebSocket member keeping all its fields to give the
  order, or two of its groups order its columns differently (naming policy, rule 6a);
- **`concept-collision` (one concept, one column):** within one kind, one column name holding
  two concepts. A concept is a field's full long-name path, each segment in snake case, so `DayClose` and
  `PrevDayClose` are two concepts, while the v2 snapshot's `lastTrade.p` and the unified
  snapshot's `last_trade.price` are one; two paths that PascalCase alike (`last.trade_price`,
  `last_trade.price`) fail. The reverse, one concept under two column names, cannot arise
  while every column is its concept's PascalCase; `PROJECTIONS`, which rename a member's
  fields, add that check.

The REST refresh also stops on: a vendor file whose SHA-256 differs from the manifest's, or a
snapshot whose files differ from the manifest's list (`snapshot`); a documented endpoint the
spec lacks, a generated path no documentation page names, a page that is not an overview yet
names no endpoint, or an `undocumented` exclusion claim the pages contradict (`docs`); an
unknown `x-polygon-*` extension on a path or operation, a parameter `oneOf`/`anyOf` that is
not a choice of date and date-time strings, or a response it cannot read (`shape`); a `UNITS`
unit that differs from the spec's unit tag without a tag-conflict entry (`units`); an
envelope key of a single-object body with a type not the envelope's (`envelope`); and a
documented example value (of the response's example, or a field's own `example`) that its
generated field refuses, or a response example with no value where the result key points
(`example-type`). A `TYPE_FIXES` entry citing the sample resolves a wrong type; a
`SAMPLE_ERRORS` entry (`exclusions.py`) records a wrong sample, such as the IPO `us_code`
example `37833100`, a CUSIP that lost its leading zero.

Descriptions differ across transports and change about twenty times as often as keys, so
`type-agreement` and `transport-agreement`, not description matching, are the mechanical
checks.

**Field descriptions.** The refresh also records the vendor's description of every field
(corrected where it is clearly wrong), so that users can read what each column means
([section 7.4](07-query-classes-and-verbs.md#74-column-descriptions)).

- **Where they come from.** REST: the OpenAPI `description` of each response property, after
  `$ref` and `allOf` are resolved, the description nearest the field winning (one written
  beside a `$ref`, an `allOf`, or a one-branch choice describes that use and replaces the
  referenced schema's own); at the 2026-10-27 snapshot, 1,597 of the 1,599 leaf response
  fields of the generated paths carry one (the two without are futures
  `/futures/v1/snapshot` `product_code` and `ticker`). WebSocket: the
  `description` of each message property in `websocket.json`, all 237 fields of the 28
  generated channels at the 2026-10-27 snapshot. Flat files: the description cell of the
  page's "Columns" table, all 178 fields of the 21 generated prefixes. A WebSocket source's
  URLs are the pages of its feed; a flat-file source's, its dataset page (one per venue for a
  futures template). The REST documentation pages are rendered from the same OpenAPI text and add
  nothing to it.
- **Notes of enclosing objects.** A REST field flattened out of a nested object also carries
  the description of every enclosing object that has one, outermost first, each appended to
  the field's own text as a sentence naming the object by its wire key:
  `` The closing price of the asset for the day. From the `session` object: <the object's
  text> ``. Each part, the field's text and every note, that ends without sentence
  punctuation is closed with a period, so no part runs into the next. Massive often
  documents a whole object's behavior only on the object: the unified snapshot's `session`
  omits its prices for a stock not yet traded, and its `open`, `high`, `low`, `close`, and
  `vwap` until the regular open; `lastQuote` and `last_trade` are returned only on some plans;
  `greeks` are left out for some contracts. Without the note, `SessionClose` or `GreeksDelta`
  would show nulls that nothing explains. The note is the object's whole normalized text,
  never shortened or filtered: no mechanical rule tells the sentence that explains a null from
  the sentence that introduces the object, and the explanation is often the last sentence. The
  cost is bounded by the vendor's own texts: at the 2026-10-28 snapshot the longest note, the
  unified snapshot's `session`, is 874 characters on its 16 fields, and 378 dataset columns
  carry a note. A `DESCRIPTION_FIXES` entry (below) under a field's key quotes and replaces
  only the field's own text, never the notes; one under an object's key corrects that
  object's note, on every field below it (the unified snapshot's `last_trade` is described as
  "the most recent quote", and its note says trade); one quoting `""` gives a note to an
  object Massive leaves undescribed. Per-asset-class texts combine: a class that the field's
  entry or a note's entry lists gets the field's text and each note for that class, the
  shared text where an entry lists none, and a note that is empty for a class adds nothing
  to it. A field that a `KEY_FIXES` entry moves out of an object keeps only the notes of the
  objects that still enclose its corrected key. A field at the top of a row, a WebSocket field,
  and a flat-file column have no enclosing object, so no note.
- **Normalization**, by four fixed rules and no hand input, so every outlet can show plain
  text: (1) an HTML link `<a href="url" …>text</a>`, with any other attributes and the `href`
  quoted or not, becomes `text (url)`; (2) then a Markdown link `[text](url)`, with or without a
  title, becomes `text (url)`; (3) a link URL that is root-relative (`/docs/…`) is resolved
  against the origin of the documentation index URL the manifest records
  (`https://massive.com`), so a reader in an IDE or the console can follow it; (4) every run of
  whitespace, newlines included, becomes one space, and the ends are trimmed. Nothing else
  changes: no entity is unescaped, no other markup is removed, nothing is shortened, and a URL
  is kept as fetched otherwise. At the 2026-10-27 snapshot 66 of the generated REST fields'
  descriptions contain a link, one of them root-relative (170 description strings of the whole
  specification do). `refresh/plain_text.py` is the one implementation, used by every
  generator. A test of the committed file fails when any link syntax survives, so a new vendor
  form shows on the refresh that brings it.
- **Corrections.** Where Massive's description is clearly wrong for its source (another asset
  class's or event's text copied onto it, or a statement Massive's own documentation of the
  same field contradicts, such as a price total that leaves out the contract multiplier), a
  `DESCRIPTION_FIXES` entry quotes the normalized text and replaces it for that
  `(source, wire key)` alone, with its reason. The replacement is Massive's own text for the
  same field of a sibling source wherever that text is right and says what the field holds;
  only when no sibling text is adequate (right but saying nothing useful, like the crypto
  WebSocket trade's "The price.", or wrong too) is it written in the entry, and the reason says
  why. A source that serves several asset classes (the unified `/v3/snapshot`) can be corrected
  per class: the entry gives the text for each listed class, and every other class keeps the
  vendor's. A class text that no dataset of its class shows (no such dataset reads the source,
  or an earlier source's text wins the column) fails a catalog test, since the refresh cannot
  see the datasets. A field Massive leaves without a description, where a sibling source
  describes the same field, gets an entry too: it quotes the empty text, so it goes stale once
  Massive adds one, and its text is the sibling's (the futures snapshot's
  `product_code` takes the `/futures/v1/contracts` text). That is the only hand input
  on descriptions; a description that is merely terse or imprecise is kept.
- **No gate.** A field without a description gets `""`, and nothing stops the refresh.
  Descriptions change about twenty times as often as keys, so a gate on them would be noise; a
  changed description is an ordinary line in the refresh diff, except where a
  `DESCRIPTION_FIXES` entry quotes it, which the `stale-entry` gate then sends back for review.
- **A separate generated file**, `spec/descriptions.json`, so that a refresh that only changes
  descriptions leaves `rest.json`, `ws.json`, and `flatfiles.json` byte for byte unchanged,
  and a reviewer sees at once that no schema moved. For the same reason the schema files record
  nothing about the snapshot they came from (no fetch date, no hash of a vendor file): the
  snapshot is named by `fetched` here. It is generated, committed, and stored like the other
  three, shipped as package data, and covered by the same parity test:

  ```
  {
   "generated_by": "python -m deephaven_massive._massive.refresh",
   "fetched": "<the snapshot's fetch date, from the manifest>",
   "urls": {"<transport>": {"<source key>": ["<each vendor URL that documents the source>"]}},
   "descriptions": {"<transport>": {"<source key>": {"<wire key>": "<plain text>"}}},
   "class_descriptions": {"<transport>": {"<source key>": {"<wire key>": {"<asset class>": "<plain text>"}}}}
  }
  ```

  A field without a description has no entry, and a source with no described field has no entry
  under `descriptions`; `class_descriptions` holds only the fields corrected per asset class,
  each asset class an `AssetClass` value. Every generated source has at least one URL, sorted
  and as fetched: for REST, every documentation page that names its path (every generated path
  has one, by the `docs` gate; a path several asset classes share, such as
  `/v3/reference/tickers`, has one page per asset class), as the snapshot's manifest records
  it. The entries are one per line, in generated order, so a reworded description is one
  changed line of the diff.
- **Joined at load.** `Field` gains `description: str = ""` and
  `class_descriptions: tuple[tuple[str, str], ...] = ()` (asset class and text, sorted by
  class), read through `Field.description_for(asset_class)`, which returns the class's own text
  or `description`, and the `spec` loader fills both from `descriptions.json` when it loads the
  schema files, under the same once-only lock. An entry that names a source or wire key the
  schema files lack raises at load: the files are generated together, so only a hand edit can
  cause it. The same load checks the URLs: a source without one, or a URL of a source the
  schema lacks, raises, so `sources` and `provenance` reject the same files. A description
  belongs to a `(source, wire key)`, so a projected field keeps the description of its wire
  key.
  `provenance() -> Provenance(fetched: date, urls: Mapping[tuple[Transport, str], tuple[str, ...]], plans: Mapping[str, PlanAccess], titles: Mapping[str, str])`
  exposes the fetch date, the URLs of each source, each vendor page's plan tables (by page
  URL: the access, recency, and history summaries, and one `PlanTerms` per plan, which the
  refresh's `plan_access` reads into `descriptions.json`), and each documenting page's title
  (its `###` heading, which the refresh's `docs.title` reads), for the dataset reference;
  it loads every transport the file gives URLs for. Each file is read once under the lock,
  whichever of `sources` and `provenance` comes first. No other name is added to the interface.

**Dataset schemas** are merged at run time, after the rename, never inferred from data.
`dataset_schema` is a method of the `Catalog` (`_datasets/catalog.py`), and its cache lives on
the catalog object: `_session.py` builds the one `CATALOG` and hands it to every core, and the
transports call `core.catalog.dataset_schema(spec)`. The merge reads the members of a kind
through `Catalog.members(kind)`, so the dataset layer needs no import of the namespace modules:

```
dataset_schema(spec):                       # cached per dataset; computed outside the lock,
                                            # published under it (first writer wins)
  members = datasets of spec.kind, ordered by the group and position of the memberships of
            their sources                         (spec alone for a dataset in no kind)
  for each group, each source s of the group in its position, read by the member m:
      fields = s.fields through the projection of s's membership of (m.kind, m's group), if any
      cols(m, s) = rename(m, s, fields)      # m's keys for s: symbol -> Symbol, time ->
                                             # Timestamp, an instrument fanout -> Underlying
                                             # (any other fanout and session keep their
                                             # columns); a key filled from the request adds
                                             # a constant column; nothing else changes
  order  = ordered union by column name of every cols(m, s):
           Symbol, then Timestamp, first where the dataset declares them; then first
           appearance in group, then source position, then field order
  schema = the columns of order that some cols(spec, s) of the dataset's own sources holds,
           or that the library fills (its key and time columns), in that order
  each column's WireType = WireType.common_type over every contribution of the kind
```

**Named conversions.** A `TYPE_FIXES` entry names one field of one source and one
`spec.types.Conversion`, and the generated `Field` records it as `conversion` (`rest.json`
writes its name). Each conversion has one meaning wherever it is used: the types the
specification may declare (`declarations`; a changed declaration makes the entry stale), the
type the field gets, whether it widens (accepts a form the declared type refuses) or only
retypes values the declared type accepts (`widens`), and exactly the forms it accepts, so no entry silently inherits a meaning nobody chose
for it. The plain converter of each `WireType` is never loosened, and no conversion is a
no-op. The decoder applies it deliberately: the WebSocket forex `x` is declared an integer
but sampled as a quoted decimal string (`"44"`), and `quoted-integer` reads either form as
the `long`; a value it cannot read (`"4.8"`, `"x"`) is
an unconvertible value like any other ([section 19.5](#195-runtime-drift)).
Every entry has a fixture test of both the conversion and the failure; a conversion that no
entry declares is coercion, which never happens. The conversions:

| `Conversion` | Declared → wire type | Accepts | Used by |
|---|---|---|---|
| `iso-text-instant` | `string` → `iso-instant` | ISO-8601 instant text | benzinga analysts and firms `last_updated`, futures market-status `timestamp`, `/v1/marketstatus/now` `serverTime`, `/v1/marketstatus/upcoming` `open` and `close` |
| `iso-date-text` | `iso-instant` → `date` | `YYYY-MM-DD` | futures products `last_updated` (sample `"2025-02-22"`) |
| `quoted-integer` | `long` → `long` | an integer, or decimal digits in text | WebSocket forex `C` `x` (sample `"44"`) |
| `text-or-integer-id` | `long` or `string` → `string` | text or an integer | the crypto trade ID, one decision: the full-market and movers snapshots `lastTrade.i`, declared text (samples `413131` and `"517478762"`), and WebSocket `XT` `i`, declared an integer (sample `14272084`); TMX `tmx_company_id`, declared an integer (sample `"2208"`) |
| `number-label` | `double` → `string` | text, or a finite number as float text (`1` gives `"1.0"`) | risk-factor `taxonomy` (sample `"1.0"`) |
| `code-list-text` | `long` → `long-array` | one integer, or integers separated by commas in text | flat-file `conditions` and stocks quotes `indicators`, one decision (sample `"12,37"`) |
| `midnight-utc-date` | `date` → `date` | `YYYY-MM-DD`, or midnight with offset `Z` or `+00:00`, giving the written date; any other clock or offset fails | indices open-close `from`, crypto open-close `day` (samples `"…T00:00:00.000Z"`); the column stays a date so the open-close sources agree |
| `chicago-midnight-date` | `date` → `date` | `YYYY-MM-DD`, or the epoch nanoseconds of a midnight in Chicago, giving that Chicago date; any other instant fails | futures snapshot `details.settlement_date` (sample `1753851600000000000`, midnight of 2025-07-30 in Chicago; `/futures/v1/contracts` sends the field as a date) |
| `integer-as-number` | `long` → `double` | any number | WebSocket crypto `XA`/`XAS` `v` (sample `951.6112`) |
| `decimal-text-number` | `double` or `long` → `double` | any number, or a finite decimal in text: an optional minus sign, digits, and an optional fraction with digits on both sides of the point; `"1e5"`, `"NaN"`, `"inf"`, spaces, and `+` fail | the futures stream's prices, one decision observed live ([section 23](23-open-verification-items.md#23-open-verification-items) item 74): `T` `p`, `Q` `bp` and `ap`, `A`/`AM` `o`, `h`, `l`, `c`; and `A`/`AM` `dv` (sample `1353.1`, sent live as text). A column of such texts is read in one pass: one pattern test over the joined texts and the builtin `float` |

The `example-type` gate converts every documented example value through its generated field:
the specification's response example (106 of the 109 generated REST paths carry one), each
WebSocket channel's message example, each flat-file page's example rows, and each field's own
`example`. A key a WebSocket example sends that its message does not declare stops the
refresh too, until the message declares it or a `SAMPLE_ERRORS` entry says the example is
wrong (the futures trade example's `z`). A type the vendor's samples contradict stops the
refresh instead of
failing at run time. It also retires fixes: a `TYPE_FIXES` entry is stale once no example
value of the fields it names (all entries sharing one decision, such as the two crypto
snapshots, one decision from one `TypeFix` object) is a value the declared type refuses. A
conversion that only retypes values its declared type accepts (`iso-text-instant`) is
exempt: the declared-type check retires
it once the specification changes the type. So is an entry whose evidence is a live response
rather than a documented sample (`TypeFix.observed_live`, such as the futures stream's text
prices): no sample shows the form, so none can retire it, and its open item in
[section 23](23-open-verification-items.md#23-open-verification-items) tracks it. A `SAMPLE_ERRORS` entry is stale once the specification no
longer shows its sample, the field accepts it, or the field is gone; samples compare as JSON
text, so `1`, `1.0`, and `true` are three samples.

**Keys are declared per source.** For each source it uses, a `DatasetSpec` declares its key
columns as `KeySpec(column, from_request=False, instrument=True, required=True)`, grouped
per source in a
`SourceKeys`, one optional declaration per role (`symbol`, `time`, `end`, `fanout`, `date`):

- every source of a dataset in a kind declares `symbol` and `time`;
- every source of any other instrument dataset declares `symbol`. A dataset is an instrument
  dataset when it takes symbols: it has channels, flat files, `per_symbol`, a symbol
  filled from the request, or a `symbol_param` that filters a single-ticker field. A
  `symbol_param` over a list-valued field (the generated `tickers` field of a filing or an
  article is `string-array`) does not make one: those rows' `Symbol` is the library's,
  filled from the request ([section 10](10-symbols-and-all.md#10-symbols-and-all)). The field is the row field of the parameter's base
  name, singular or plural, read from the generated path, never from a hand list. Its asset
  class alone does not make a dataset one either: `futures.exchanges` and
  `futures.schedules` keep `FUTURES` for the session-day rule and take no symbols;
- a `symbol` key from the rows is a `string` field: a list of tickers is never `Symbol`;
- any other dataset declares only the keys its rows carry, possibly none: `reference.exchanges`,
  `conditions`, `market_status`, `market_holidays`, and `ticker_types`, `futures.exchanges`,
  and `benzinga.firms` declare none, and `economy.*` declares none either: it keeps its `Date`
  column (its axis is `time_axis` on `date`);
- a `time` key is an instant: its field (after the projection) is an `epoch` or an
  `iso-instant`, so `Timestamp` (a bar's `TimestampStart`) is always an `Instant`; so is an
  `end` key, the WebSocket bars' `e`, which becomes `TimestampEnd`. A dataset whose row time is a
  calendar date keeps that date column, ISO text, under its own name and declares no `time`
  key;
- a `date` key is a `date` field, the row's trading day as the source sends it (the futures
  `session_end_date`, the open-close `from` and crypto `day`), and becomes `Date`; only a
  dataset whose rows have a `Date` (`row_times` other than `NONE`) declares one, and a daily
  summary (`DAY`) declares one on every source;
- `fanout`, `date`, and `end` are declared only on the sources that carry them; only a bars
  dataset declares `end`;
- `instrument` says whether a `fanout` key names an instrument: True, the default, renames it
  to `Underlying` (the options underlying, the related company asked about); False keeps its
  generated column (`filer_cik` stays `FilerCik`). Only a `fanout` key may set it False, and
  such a key is a column of the rows, never `from_request`;
- `required` (default True) says whether every row must carry the key. False is for a symbol
  Massive documents as present only "if available" or marks optional, such as the `ticker` of
  the stocks `filings.index`, the TMX corporate events, and the Benzinga `analyst_insights`,
  `ratings`, `bulls_bears_say`, `guidance`, and `earnings` (whose only documented
  analyst-insights row has no `ticker`): a row without it keeps a null `Symbol` instead of
  failing the request as a missing required key. It is also for an underlying only some rows
  carry: the unified snapshot's `fanout` key `UnderlyingAssetTicker`, which only its options
  rows have. Only a `symbol` or `fanout` key read from a REST path's rows may set it False;
  flat files and streams filter and route rows by symbol, and the `time`, `end`, and `date`
  keys are always required. A dataset with an optional symbol key cannot put
  `Symbol` in its `poll_keys`, whatever its mode (`DatasetSpec` refuses it): a row without a
  symbol would have a null poll key, which fails the `poll()` table in every mode, so such a
  dataset declares other `poll_keys`.

A key is `from_request` when the source does not carry it in its rows: the symbol of a source
that takes it as a path parameter or holds it outside the result key
([section 10](10-symbols-and-all.md#10-symbols-and-all)). The library fills a
`from_request` key as a constant column. `to_batch(..., required=)` and `ReadPlan.required`
(per request) and `WsClient.set_required(channel, keys, *, symbol)` (per channel, before its first
subscription, [section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol)) receive exactly the
source's required keys (declared, not `from_request`, and not marked `required=False`), so a futures WebSocket row without
`session_end_date` is not a missing key, while a futures flat file without it is. A member of
a kind keeps only the columns its own sources fill: crypto's `ParticipantTimestamp` is renamed
to `Timestamp`, so crypto trades have no `ParticipantTimestamp`, while stocks keeps its
`ParticipantTimestamp` column. The columns the members share keep the kind's type and order,
so their tables merge on them. A non-tick
dataset with several role paths (`options.contracts` list and per-contract) is the union of its
role paths under the same join.

**Column descriptions.** Each column of a dataset schema is a
`Column(name, type, description, supplied_by, wire_keys)`: `supplied_by` is the transports of
the dataset's own sources that fill the column (a key filled from the request counts), in the
order `REST`, `FLAT_FILES`, `WS`, never empty (a generated test checks every dataset); `wire_keys` maps each of those transports to the distinct wire
keys behind the column, before any projection, in source order. `dataset_schema` picks one
description per column of each **dataset**, never per kind: a kind's union spans asset classes,
and one class's text is wrong for another (the stocks trades' `price`, "dollar value per whole
share", for a crypto trade). Picking per dataset keeps another member's text off a dataset, but
it cannot correct a dataset's own source when Massive copied the wrong text onto it: the crypto
REST last trade and trades flat file carry that same stock `price` text. The refresh corrects
those through `DESCRIPTION_FIXES` (the hand-input table above), so every source already carries
a description right for it when the rules below pick one. Each field's description in the
rules is the one for the dataset's asset class, `Field.description_for(spec.asset_class)`: a
source that serves several classes may carry a per-class correction (`options.snapshots`
shows the options text of the unified snapshot's last trade, as `options.chain` does), and
a dataset of `ClassFromPrefix.ANY` takes the field's own description.

1. **A column the dataset's own sources supply** takes the first non-empty description among
   them, trying the transports in the order REST, flat files, WebSocket and, within one
   transport, the dataset's sources in their declared order (role paths, then channels, then
   prefixes). REST comes first because its text is the most complete.
2. **The library's own columns**: `Symbol`, `Timestamp`, and `Underlying` when they are
   declared key columns of the dataset, and the time columns the library fills (`Date`,
   `TimestampStart`, `TimestampEnd`, `TimestampInsert`), get a library sentence ("The
   instrument, in the library's canonical symbol form.", "The row's primary time, as an
   Instant.", "The instrument Massive links the row to, in the library's canonical symbol
   form; for an option, the linked instrument, not what the contract delivers."; a bar's
   `TimestampStart` "The start of the bar's window, as an Instant." and, for every class with a
   day zone, where a bar of a day or longer starts, since the library's start is not the
   end-of-day stamp some sources' descriptions quote; `TimestampEnd`, `Date`, and
   `TimestampInsert` what [section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries) says of them for
   the dataset's row times and class, the join caveats included). The key columns, `Date`, and
   `TimestampStart` go on with one
   sentence that starts "From" and lists, per transport in the order of rule 1 and spelled as in
   [section 7.4](07-query-classes-and-verbs.md#74-column-descriptions), the wire key each source renames and its quoted
   field description (`` REST `sip_timestamp`: "The nanosecond accuracy SIP Unix Timestamp…" ``)
   or, for a key filled from the request, the request parameter it came from
   (`` REST: the request's `stocksTicker` parameter ``). The items are joined by "; ", so a
   lowercase label such as `flat files` never starts a sentence, and the sentence ends with a
   period unless its last quoted description already ends one inside the quote:
   `` The instrument, in the library's canonical symbol form. From REST `T`: "…"; flat files
   `ticker`: "…"; WebSocket `sym`: "The ticker symbol for the given stock." ``

A library column lists every transport of the dataset in `supplied_by`, since the library
fills it on each. A dataset's note on a column (`DatasetSpec.column_notes`) follows the
description chosen by these rules: the split-adjustment note of every price and volume column
of `stocks.bars`, `options.bars`, and both daily open-close datasets
([section 12](12-timestamp-units-and-day-boundaries.md#12-timestamp-units-and-day-boundaries), "Price adjustment"), since the same column
holds either basis depending on the query and no vendor text says so. A column whose sources
carry no description gets `""`, and nothing fails. A column of nested values kept as JSON text
(`json-text`) ends its description with "The column holds Massive's nested value as JSON text."
The sentences of rule 2 and the notes
are the library's statements about its own columns, not hand input about Massive,
so they live in `_datasets/column_descriptions.py`, not in `refresh/`.

**Derived columns.** Two columns no source carries are computed by every transport from the
symbols it reads, and follow the leading key columns with the library's own description and
the transports and wire keys of `Symbol`: `OptionRoot`, on every options dataset with a
`Symbol` and on a dataset of every class whose rows include options contracts
(`DatasetSpec.option_rows`, the unified snapshot), is the symbol without `O:` and its last 15
characters (`symbols.option_roots`, after the OCC grammar check), null for a symbol that is no
options contract; `RawSymbol`, in a session whose `unreadable_symbols` is `LOG` (the default)
or `IGNORE` and only on a dataset that reads its symbol from rows, holds the text of a symbol
the library could not read and is null elsewhere. It is the one column whose presence depends
on a setting, and `columns()` and the column reference say so.
`Catalog.dataset_schema(spec, raw_symbol=...)` caches each
shape, and `SessionCore.dataset_schema` picks the session's, so a result, its `columns()`, and
every transport plan agree. Every outlet of
[section 7.4](07-query-classes-and-verbs.md#74-column-descriptions) reads these `Column`s and nothing else.

**Catalog gates** (`tests/datasets/test_catalog.py`, with negative tests on synthetic `DatasetSpec`s):

- **Declared keys:** every key a dataset declares for a source is either a field of that source
  after the projection of the dataset's membership (the `indices.values` time key on
  `/v3/snapshot/indices` is `timestamp` only after it), or `from_request`; every source of a
  dataset in a kind declares `symbol` and `time`, and every source of an instrument dataset
  (one that takes symbols) declares `symbol`; a `time` key is an `epoch` or `iso-instant`
  field; a dataset's `fanout` keys agree on `instrument` across its sources and across the
  members of its kind. Its negative tests cover an undeclared kind key, an undeclared
  instrument symbol, a declared key the source lacks, a date as the time key, and reference
  datasets that declare none and pass, one of them with an asset class (`futures.exchanges`);
- **Unique columns:** after the rename, no source has a duplicate column;
- **Member types:** across members, one column name gets one `WireType`;
- **Kind membership:** each kind dataset reads only sources holding a membership of its
  `(kind, group)`, and the datasets of one `(kind, group)` together read each of those sources
  exactly once (a group split over a stream dataset and its one-row dataset,
  [section 7.5](07-query-classes-and-verbs.md#75-one-row-datasets));
- **Wire parameters:** every declared wire name (time-axis pair, symbol parameter, path
  placeholders) is a parameter of its `RestPath`;
- **Append keys:** a dataset whose default `poll()` mode is `APPEND` declares its own
  `poll_keys`, not the default `Symbol`. The gate checks only that the key is explicit; that it
  is unique per row is checked by the dataset's own tests. (`DatasetSpec` refuses empty
  `poll_keys` on an `APPEND` dataset, an empty identity (no `poll_keys` and no
  `revision_keys`) on any dataset, since an explicit `APPEND` could not tell rows apart, and
  `revision_keys` that repeat a poll key.)
- **Feed axis:** a dataset whose default `poll()` mode is `APPEND` declares a time axis, since
  its `poll()` needs `start`;
- **Nullable keys:** a dataset with `poll_keys` has at least one that is not in
  `nullable_poll_keys` (`DatasetSpec` refuses a nullable key that is no poll key, or one without
  a reason);
- **Poll keys:** every `poll_keys` and `revision_keys` column is a column of the dataset's
  schema;
- **Generated sources:** every path, channel, and prefix a dataset uses is generated; a miss names
  the dataset and says "deprecated or removed in the snapshot";
- **Coverage:** generated = used ⊎ deferred: every generated source is read by some dataset,
  or deferred with a reason and an issue
  ([section 8.12](08-namespace-tree.md#812-excluded-endpoints)).

`KINDS` overlaps the catalog by design: the refresh checks names before any dataset is
declared, and the kind-membership gate keeps the two in step. A test loads `spec/` with `refresh/`
absent, as in an installed wheel; the CI build job checks that the wheel holds no `refresh/`.

### 19.5 Runtime drift

Data on the wire can differ from the committed schema until the next refresh, and Massive's
data drifts in ways no schema predicts. The rule: a valid request returns its data whenever it
can, and serves only what is right. One odd value never costs its row, and one odd row never
costs its call or its stream: a value the library cannot read is null and a row it cannot place
is dropped, each with a WARNING, the way a missing field already reads as null. Wrong data is
worse than an error, so nothing is ever coerced or guessed, and only a payload in which no row
can be placed fails.

**Detection** is in `_massive`: each decoder compares a payload with its generated source and
returns a `DriftReport(extra, missing, unconvertible, statuses, dropped_rows, dropped_reason,
samples)` (`missing`: only vendor-`required` fields that some row lacked or held as null;
`unconvertible`: the fields outside the key columns that held, in some row, a value that does
not fit, decoded as null; `statuses`: REST envelope statuses outside the documented ones;
`dropped_rows`: the rows dropped because a key could not be read, with the first one's reason;
`samples`: for each extra, missing, or unconvertible key, the first row's symbol as the wire
spells it, known to the WebSocket decoder only, and the value of an extra or unconvertible key,
so a WARNING can quote them; `dropped_reason` and `samples` are outside equality and the
`repr`). `DriftReport.merge` combines the reports of two payloads read as one. A decoder never
raises for drift or for a row. **Policy** is in the library: the session's `DriftRegistry` in
`_tables/diagnostics.py` (a leaf lock, WARNINGs logged after release) and the routing in
`_ingest/rest.py`, `_ingest/rest_rows.py`, `_tables/poll.py`, `_ingest/ws_connection.py`,
`_tables/stream.py`, and `_ingest/flatfiles.py`.

A **key** here is a key the dataset declares for the source, not filled from the request and
not marked optional (`KeySpec.required`; `source_columns.required_fields`): the row's symbol,
its time column, and where declared its end, fan-out, and date (the futures session). A dataset
that declares no key has none.

| Case | REST (`rest/batch.py`) | WebSocket (`ws/protocol.py`) | Flat files (`flatfiles/reader.py`) | Library |
|---|---|---|---|---|
| **Extra field** | key not in the path's fields, unless the path ignores it (`Source.ignored_keys`, from `IGNORED_KEYS`: dropped, never reported) | key not in the channel's fields, unless the channel ignores it | header not in the documented headers | Dropped. WARNING once per (dataset, transport, field), naming where it was seen, the first symbol, and the first value ([section 17.1](17-diagnostics.md#171-the-log)). Shown in `SchemaDrift`. |
| **Unknown envelope status** | a `status` outside `OK`, `DELAYED`, `success`, `ERROR`, `NOT_AUTHORIZED`: the rows are kept, `Page.drift.statuses` | | | WARNING once per (path, status). Never a failure. |
| **Key that cannot be read** (absent, null, or a value that does not fit) | the row is dropped and counted (`dropped_rows`), the whole row logged at DEBUG (first 2,000 characters) | the row is dropped as a `Reject(channel, symbol or None, WireError, raw)`, logged whole at DEBUG; the library counts it in the frame's `dropped_rows` | a file without a key header: `WireError` before the first block; a row whose key field is empty or does not parse: dropped and counted | The row is dropped. One WARNING per dataset per session for the first dropped row, naming the transport, where, and the reason (the key, the expected type, and the value); later drops log the dataset's running count at DEBUG, at most once a minute, and every live table that lost rows counts them in its `SchemaDrift` (`REST:3 rows dropped`). Never a failure, unless every row of a payload is dropped (below). |
| **Malformed line** (a flat-file line that does not split into the header's fields: too few, too many, or a quote that does not close) | | | skipped and counted (`FlatFile.malformed_lines`) through Arrow's `invalid_row_handler`, with the first one's row number and field count (`malformed_reason`), never its text; the rest of its block is read | One WARNING per file naming the count and the first line's row and field count. A file whose every line is malformed fails (`MassiveError` naming the file). |
| **Missing other field** | null | null | a null column of the declared type | A vendor-required field or a CSV header: null, WARNING once, `SchemaDrift`. Any other field: null, silently, because JSON omits absent values and the vendor marks almost nothing required. |
| **Unconvertible value** outside the key columns (the type, the `int64` range, `bool` given as a number, a `long-array` element, `int64` for `long[]`, a `string-array` element that is not text, a nested object that is not an object) | null, the field in `unconvertible` with its first value, the whole row logged at DEBUG | the same | every header is read as text and converted afterwards, a typed value's surrounding spaces trimmed as Arrow's typed parsing does, so a value that does not parse is null, the header in `unconvertible` with its first text; the odd values are found by vectorized pattern, range, and calendar tests, never a Python call per value | Null. WARNING once per (dataset, transport, field), quoting the first value; shown in `SchemaDrift` as `field?`. Never coerced: no undeclared conversion is applied. A lattice widening and a named `TYPE_FIXES` conversion ([section 19.4](#194-generated-schemas), for example forex `x` from a decimal string to `long`) are not coercion; a value the named conversion cannot parse is unconvertible. |
| **Whole number beyond 2<sup>53</sup> in a widened column** (a `long` field in a `double` column, by the lattice join) | the library, after decoding (`source_columns.widen`, `RoundedFields`) | the same | the same | Served rounded, as the lattice join means it; never nulled or changed further. The cast is cast back and compared with the original (and a value reaching 2<sup>63</sup> counts as rounded); the field goes in `DriftReport.rounded` with its first value: WARNING once per (dataset, transport, field), and `SchemaDrift` as `field~`. |
| **Timestamp out of [1990, 2100], or overflow** | in `_datasets/timestamps.py` (`masked_instants`), in `_ingest/rest_rows.py` | in `_datasets/timestamps.py`, in the flush thread as a vectorized mask | the time header in the reader, through `ReadPlan.to_ns`; the others in `_ingest/flatfiles.py` | In the time column, a key that cannot be read: the row is dropped and counted, its reason naming the dataset, the column, the unit, and the value. In any other column, an unconvertible value: null, `unconvertible`, WARNING once. |
| **Undocumented value of an interpreted field** (`timeframe`) | `_ingest/recency_labels.py` | | | Never read as delayed: the host decision streams from the real-time feed with a WARNING naming the field and the value ([section 13.2](13-host-resolution-and-connection-protocol.md#132-host-decision)). |
| **Symbol spelling the library cannot read** (`canonical_from_wire` returns None) | `RowDataError` naming the spelling | the row is dropped and fails every stream of the dataset on the channel | `MassiveError` naming the spelling | Under `unreadable_symbols=LOG`, the default, the row is kept with a null `Symbol` and the text in `RawSymbol`, ERROR once per spelling with the library-bug text (at most 100); under `IGNORE` the same at DEBUG; under `RAISE`, a loud failure whose message names the setting and the bug report. |

**When every row is dropped.** One dropped row is drift; a payload in which no row can be
placed is the sign of a change in Massive's format (a renamed key, a new timestamp unit), and
serving it would quietly return an empty table. So a loud failure, a defect of the library
(`ErrorCause.LIBRARY_DEFECT`), names the count and the first row's reason when:

- **REST**: a page that had rows keeps none of them, its error rows included (`RowDataError`
  from `rest_rows.RowConverter.convert`). A one-row page whose row is dropped is such a page.
- **Flat files**: a file holds rows of the requested symbols, and every one of them is
  dropped (`WireError` from the reader after the file's last block, then `MassiveError` naming
  the file), or every line of the file is malformed. Rows the symbol filter removes do not
  count. The rule is per file, never per block: a block can hold a single row of an illiquid
  symbol, and its one odd row must not fail the call.
- **WebSocket**: `DROPPED_RUN_LIMIT` (100) rows of one dataset on one channel of a connection
  have been dropped in a row, across flushes and symbols, with no row of the dataset published
  between them (`Flusher._counted`, counting the client's drops and the flush's). A flush is
  no unit here: a quiet stream's flush often holds one row, so a per-flush rule would let one
  odd row fail it. A published row starts the count again. Each symbol also has a run of its
  own (`Flusher._counted_symbols`), which other symbols' published rows do not end: after
  `DROPPED_RUN_LIMIT` of one symbol's rows in a row are dropped with none of its rows
  published, the streams that name that symbol fail, so it never freezes them without an
  error. A `dm.ALL` stream is never failed for one symbol: its `m.streams()` `Error` says,
  without failing it, `rows for ESH5 are being dropped (<first reason>); its last values may
  be stale` (the first three such symbols and a count of the rest), and the note goes once a
  row of the symbol is published again, so a trader never sees a frozen row without a mark.

**A loud failure** depends on the verb: static verbs (`fetch()`, `history()`) raise
`MassiveError` through `from_wire` or `RowDataError`; `poll()` fails its table through
`publish_failure`; a WebSocket failure fails, through `publish_failure`, every stream of the
dataset on the channel. A row dropped for its key is never published, and fails no stream; the
rest of the batch is published. A drift failure goes through `SessionCore.fail_stream`, like an
entitlement failure.

**`SchemaDrift`** is a text column of both diagnostics tables ([section 17](17-diagnostics.md#17-diagnostics)),
sorted and truncated at 200 characters: `m.streams()` uses `transport:field+` for extra,
`transport:field-` for missing, `transport:field?` for a field that held a value that does
not fit, and `transport:field~` for a `long` field whose widening into a `double` column
rounded a value, with the transport spelled `WebSocket` or `REST`, and `m.connections()` uses
`channel:field+`, `channel:field-`, `channel:field?`, and `channel:field~`. Dropped rows add
one entry per transport or channel with the owner's running count, `REST:3 rows dropped`.
The count entries, sorted, come before every field entry, so a text cut at 200 characters
keeps each of them: after its one WARNING, a dropped row stays visible on every live table
that lost it. The count counts reads, not distinct rows: a `poll()` that reads the same bad
row every cycle adds one to it every cycle.

**The library supplies Massive's data and never repairs it.** Whatever it cannot serve as
Massive sent it is reported where a user looks, never absorbed: a row it drops is counted in
`SchemaDrift`, a value it changes (a whole number a `double` rounds) logs a WARNING, a futures
one-second bar's `e` that is not its start plus one second is served as sent with a WARNING,
a symbol a `poll()` cycle cannot read
is listed in `Error`, a gap in a live table's rows is kept in `Gaps`, and a flat-file day taken
for a holiday is named in the table's description.

### 19.6 Refresh and drift

```
uv run python -m deephaven_massive._massive.refresh            # regenerate spec/*.json from the snapshot
uv run python -m deephaven_massive._massive.refresh --fetch    # replace the snapshot with the live sources
uv run python -m deephaven_massive._massive.refresh --check    # live: report drift; changes nothing
```

The command runs from a source checkout; `refresh/` is not in the wheel. The parity test in
`tests/massive/refresh/test_generate.py` regenerates `spec/*.json` from the snapshot and
requires it byte for byte, and checks documented = generated ⊎ excluded ⊎ (deprecated ∪
draft).

**The snapshot is stored canonically**, in `vendor/massive/`, and `refresh/snapshot.py` is the
one owner of its format: the layout names, the manifest, the staged write and atomic swap, and
the verified load. `fetch.py` puts each download in canonical form: JSON with `indent=1` and a
final newline, keeping the order in which the vendor served each object's keys (the documented
column order, never re-sorted), and text with trailing whitespace stripped and a final newline,
so refresh diffs read line by line and no hook exception is needed. `manifest.json` records the
fetch date and, per file, its URL and the SHA-256 of the fetched bytes and of the stored file.
Every refresh re-hashes every file against the manifest and requires the files on disk to be
exactly the manifest's, so a hand-edited vendor file stops it; the generated files record
only the fetch date (`descriptions.json`); the manifest committed beside them holds every hash,
and the parity test binds the two. Besides the
specs, `--fetch` stores every REST, WebSocket, and flat-file page `llms.txt` links, under
`vendor/massive/rest/`, `websocket/`, and `flat-files/`, because `llms.txt` names pages, not
paths: each REST page's `**Endpoint:**` line is what ties the documentation to an OpenAPI
path, each WebSocket page names its feed's cluster and channel, and the flat-file overview
and dataset pages are the flat-file schemas. It also stores, under
`vendor/massive/knowledge-base/<slug>.md`, each knowledge-base article the library relies on
(`refresh/knowledge_base.py`, `ARTICLES`: a short, reviewed list of slugs, each with what the
library takes from the article). Articles are published as HTML only, so the fetch extracts
the title and the `<article>` element of each page as Markdown with the standard library's
HTML parser; the manifest records the page's URL and the hash of the HTML as fetched. The
first is "What is the request limit for Massive's RESTful APIs?", the source of the Basic 429
wording and of the `other` bucket. The documentation host rate-limits a burst of page requests
with HTTP 429, which the fetch waits out (`Retry-After`, capped at 60 s, else 2 to 32 s, five
times); any other failure stops it and leaves the old snapshot in place, and the new snapshot
is swapped in through a backup directory (`vendor/massive-incoming/` and
`vendor/massive-previous/`, both gitignored). The snapshot (about 4 MB, 243 files, fetched
2026-10-30 UTC) is Massive's published content, vendored as generation input and not covered
by the repository's license, so it ships in neither the sdist nor the wheel.

**`--check`** downloads the live `openapi.json`, `websocket.json`, `llms.txt`, documentation
pages, and knowledge-base articles, compares them semantically with the snapshot (`refresh/drift.py`), prints a
report, changes nothing, and exits non-zero on drift. It reports REST paths new, removed, or
newly deprecated (including deprecated paths still in `rest.json`); fields and parameters
added, removed, or retyped; WebSocket channels and keys; flat-file pages, headers, and overview
rows; knowledge-base articles whose text changed, naming what the library takes from each;
changed unit tags; the keys the gates would stop on; and **unaccounted items**:
documented REST endpoints, channels, and flat-file pages neither generated nor excluded, plus,
when S3 credentials are set, bucket prefixes from `FlatFileClient.prefixes()` that nothing
names.

**The drift job.** `.github/workflows/spec-drift.yml` runs `--check` weekly and on demand. With
drift it keeps one standing "Massive API drift" issue (found by title and the `spec-drift`
label, which it creates when missing; reopened if closed; never a duplicate) listing each item
with its action; with no drift it does nothing. GitHub notifies no one of an edited issue
body, so when the report differs from the open issue's body the job also posts one short
comment giving the new and the previous item counts, with no vendor text; an unchanged report
posts nothing, and opening or reopening the issue, which notifies, adds no comment. Its permissions are `contents: read` and
`issues: write` and nothing else: the checkout needs read access because the repository is
private, and the issue and its label need write access to issues. It never pushes, never opens
a pull request, and is never a gate.

**Closing the loop.** A person runs `--fetch` and the plain refresh, then reviews one PR holding
the snapshot diff, the `spec/*.json` diff, and any hand-input lines, and `check.sh` runs every
refresh gate and the catalog gates. What each change does:

| Change at Massive | At run time | In the refresh |
|---|---|---|
| Field added | Dropped, one WARNING, `SchemaDrift`. | It gets a name (`short-key` asks for a lexicon or `NAMES` line for a short key) and joins its kind (`type-agreement` checks the type, `transport-agreement` the transports). |
| Field removed | Null; a required field or CSV header warns; rows without a required key are dropped, and a payload with none left fails loudly. | It leaves its source; the declared-keys gate fails if it was a declared key. |
| Field renamed | A removal plus an addition. | The column is renamed in the diff, or stays the same if the long form is unchanged; `transport-agreement` catches a one-sided rename. |
| Type changed | A value that no longer converts is null with a WARNING; in a key, its row is dropped, and a payload with none left fails loudly. | `type-agreement` or the member-types gate fails on disagreement, resolved as a naming error first, then by a vendor fix or a `TYPE_FIXES` entry. |
| Unit changed | The overflow check or the bound drops every row, and the payload, or a run of `DROPPED_RUN_LIMIT` WebSocket rows, fails loudly. | `--check` reports the changed tag, and the `UNITS` entry is re-verified. |
| New endpoint, channel, or file | Nothing. | `--check` reports it as unaccounted, the refresh generates it, and the coverage gate fails until a `DatasetSpec`, a `DEFERRED` entry, or an `EXCLUSIONS` entry is added. |
| Endpoint deprecated or removed | `MassiveError` with the HTTP status. | It leaves the generated set, and the generated-sources gate names every dataset that uses it. |
