## 1. Purpose and scope

`deephaven-massive` turns Massive's three transports into Deephaven tables:

- **WebSocket** streams (`wss://<host>/<cluster path>`) become live tables.
- **REST** endpoints (`https://api.massive.com`) become static tables, or live tables that
  re-run a request on an interval.
- **Flat files** (gzip CSV on S3 at `files.massive.com`) become static tables.

The library runs inside an existing Deephaven worker. It never starts a JVM and never embeds a
Deephaven server. It does not use Massive's Python SDK. It speaks Massive's documented REST,
WebSocket, and S3 flat-file protocols directly, through its own clients, and it generates every
schema from vendored, pinned vendor specs. All wire access lives in one private package,
`deephaven_massive._massive`, behind one small interface
([section 19](19-wire-clients-and-the-spec-snapshot.md#19-wire-clients-and-the-spec-snapshot)). The runtime dependencies are
`deephaven-core`, `pyarrow`, `boto3`, `botocore`, `urllib3`, `websockets`, and `certifi`.

There are no existing users. Backward compatibility with the previous code base is not a goal,
and nothing in this design exists to avoid breaking a user: no compatibility shim, no
deprecation path, and no rule that rests on the chance of breaking someone.
