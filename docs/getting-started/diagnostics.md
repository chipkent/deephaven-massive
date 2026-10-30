# Reading `m.streams()` and `m.connections()`

The library reports what it is doing in three live tables. `m.streams()` has one row for each
stream, poll, and background download you started: is it running, how many rows has it
delivered, and if it stopped, why. `m.connections()` has one row for each connection to
Massive, and for each decision about which of Massive's servers to use. `m.rate_limits()` shows
how your REST requests are being paced. When a table looks wrong, start with `m.streams()`.

```python
import deephaven_massive as dm

m = dm.MassiveSession()
trades = m.stocks.trades(["AAPL", "MSFT"]).last_by()

streams = m.streams()
connections = m.connections()
limits = m.rate_limits()
```

Each call returns a new live table. Like any live table, it updates while you hold it, and it
keeps its last rows after the session closes. In the console, rows written during a command
appear once the command finishes, as with every live table.

## `m.streams()`

One row per stream (each distinct WebSocket subscription you opened), per `poll()` table, and
per `history()` or `fetch()` started with `wait=False`. Two tables over one query share a
stream, so they share a row. A plain `fetch()` or `history()` returns before you could look,
so it has no row.

| Column | What it holds |
|---|---|
| `StreamId` | A number that identifies the row. |
| `Kind` | `stream` for a WebSocket stream, `poll` for a `poll()` table, `read` for a background download. |
| `Dataset` | The dataset, such as `stocks.trades`. |
| `Query` | The query that opened it, such as `<StreamHistoryQuery stocks.trades symbols=('AAPL', 'MSFT')>`, so two streams of one dataset can be told apart. Long text is cut at 200 characters. |
| `SymbolCount` | How many symbols it covers now. For a watchlist, this follows the table. |
| `Symbols` | The symbols, cut short when there are many. |
| `HeldResults` | How many of the tables you got from it the library still tracks. It drops as you release tables. |
| `Deferred` | Options contracts waiting for room under the 1,000-contract limit of one connection. |
| `Transport` | `WebSocket` for a stream, `REST` for a poll, `REST` or `flat files` for a download. |
| `Host`, `Path` | Where the data comes from, such as `delayed.massive.com` and `/stocks`. |
| `RowsPublished` | Rows delivered so far. |
| `RequestsPerCycle` | For a poll: requests each cycle sends, one per symbol for datasets that take one symbol per request. |
| `State` | Where it is in its life; see below. |
| `Error` | Why it stopped, or a warning that does not stop it; empty when all is well. |
| `CreatedAt` | When it started. |
| `SchemaDrift` | Fields Massive sent that the library did not expect (`WebSocket:zz+`), or did not send that it expected (`REST:size-`); see below. |

### States

| `State` | Meaning | What to do |
|---|---|---|
| `RUNNING` | Delivering rows, or waiting for the next one. | Nothing. A stream outside market hours is `RUNNING` with no new rows. |
| `RELEASED` | You no longer hold any table from it, but a table derived from one, or a panel open in the web UI, still uses it, so it keeps running. | Nothing; it stops when the last user goes. Close the panel or delete the derived table to stop it sooner. |
| `FAILED` | It stopped with an error. Its tables keep their rows but no longer update. | Read `Error`. Fix the cause and call the verb again; a new call opens a fresh stream. |
| `CLOSED` | The session was closed. | Create a new session for new tables. |
| `LOADING` | A background download is still running; its table is empty. | Wait. |
| `DONE` | A background download finished; its table holds the whole result. | Nothing. |

The first four states belong to streams and polls, the last two (with `FAILED`) to background
downloads.

### What `Error` can say

- **A failure**, with `State` `FAILED`: the same message the table's error carries, such as a
  plan refusal (see [Plans and entitlement errors](plans.md)), a rejected key, or a row
  Massive sent that does not fit the dataset.
- **A watchlist row in the wrong spelling**: the value, how many such rows there are, and
  how to fix it. The stream keeps running without that row; see
  [Reactive watchlists](watchlists.md).
- **Deferred options contracts**: the cap was reached and some contracts wait. They subscribe
  when other contracts leave.
- **Waiting for a connection slot**: another program is using your key's one connection to
  this Massive server. The stream resumes when the slot frees. Stop the other program, or let
  them share one session.
- **A missed futures session**: the futures calendar could not place a row; the stream keeps
  running.
- **A poll cycle that failed**: a temporary failure, such as a network error. The table keeps
  its rows and the next cycle tries again.

### `SchemaDrift`

Massive occasionally adds a field or stops sending one. The library keeps working: an extra
field is dropped and logged once, a missing one leaves its column empty. `SchemaDrift` lists
what differed, as `transport:field+` for an extra field and `transport:field-` for a missing
one. It is information, not a failure; it means the library may need an update to show the
new field.

## `m.connections()`

Two kinds of row share this table, told apart by `Transport`:

- a `WebSocket` row for each connection to one of Massive's streaming servers, one per asset
  class and kind of channel;
- a `REST` row for each decision about which server to stream from, made with one REST
  request the first time you stream a class.

| Column | What it holds |
|---|---|
| `Transport` | `WebSocket` for a connection, `REST` for a server decision. |
| `AssetClass` | `stocks`, `options`, `futures`, `indices`, `forex`, or `crypto`. |
| `Family` | `TICK` for trades, quotes, and bars; `FMV` for the fair-market-value streams, which have a server of their own. |
| `Host`, `Path` | The server and path, such as `socket.massive.com` and `/stocks`. |
| `Recency` | `REALTIME`, `DELAYED` (15 minutes late), or `UNKNOWN`. |
| `Evidence` | Why the library chose this server, such as `last_trade.timeframe=DELAYED` (what your plan's snapshot reported) or `ws_hosts` (you chose it in the configuration). |
| `State` | Where the connection is; see below. |
| `Topics` | Subscriptions the connection should hold. |
| `AckedTopics` | Subscriptions Massive has confirmed. |
| `PendingTopics` | Subscriptions sent and not yet confirmed. |
| `NoAckTopics` | Subscriptions Massive never confirmed, even after the library asked twice. |
| `BufferedRows` | Rows received that have not yet reached their tables. Normally near zero. |
| `Reconnects` | How many times the connection has been re-established. |
| `LastMessageAt` | When Massive last sent anything. |
| `NextReconnectAt` | While waiting to reconnect, when the next attempt is due. |
| `UpdatedAt` | When the row last changed. |
| `LastError` | The latest error on the connection, with the HTTP status and Massive's request id for a REST failure. |
| `SchemaDrift` | Unexpected or missing fields, as `channel:field+` or `channel:field-`. |

### States

| `State` | Meaning | What to do |
|---|---|---|
| `RESOLVING` | Deciding which server to use. | Nothing; this takes one request. |
| `CONNECTING` | Opening the connection and signing in. | Nothing. |
| `CONNECTED` | Open and delivering. | Nothing. |
| `RECONNECTING` | The connection dropped and will be retried at `NextReconnectAt`. Waits start at 30 seconds and double up to two minutes. | Usually nothing: streams resume on their own. If it keeps happening, check your network and `LastError`. |
| `WAITING_SLOT` | Massive refused the connection because your key already has one open to this server, from another program or another computer. | Stop the other client, or wait: the library retries. |
| `FAILED` | The connection cannot work, for example because Massive rejected the key. Its streams fail. | Read `LastError`. |
| `CLOSED` | The session was closed, or no stream needs the connection any more. | Nothing. |

### Reading the signs

- **Rows arrive 15 minutes late.** `Recency` is `DELAYED`: your plan sees this class delayed.
- **Topics stay pending.** `PendingTopics` or `NoAckTopics` above zero for more than a few
  seconds means Massive is not answering subscriptions; streams on those topics may get no
  rows. Check `LastError` and Massive's status page.
- **`BufferedRows` keeps growing.** Rows arrive faster than the server applies them. The
  library logs a WARNING above a million. Subscribe to fewer symbols, or use a `ring()` or
  `last_by()` instead of `append_only()`.
- **`LastMessageAt` stops moving during market hours.** Massive has sent nothing on this
  connection since then. Outside market hours that is normal. During them, check `LastError`
  and Massive's status page; `Reconnects` counts each time the connection was re-established.

## `m.rate_limits()`

One row per REST rate-limit bucket: one per subscription (`stocks`, `options`, `indices`,
`currencies`, `futures`), plus `other` for reference data and the datasets that belong to no
subscription, which Massive counts on their own.

| Column | What it holds |
|---|---|
| `Bucket` | The subscription, or `other`. |
| `State` | `READY` (a request starts at once), `PACED` (requests wait their turn under the per-minute limit), or `PAUSED` (Massive refused a request for coming too fast, and requests wait until `PausedUntil`). |
| `RequestsPerMinute` | 5 while paced; empty when there is no limit. |
| `LimitSource` | `DECLARED` (you named the subscription in `rest_basic_subscriptions`) or `LEARNED` (Massive's refusal named the limit). |
| `InFlight` | Requests sent and not yet answered. |
| `Waiting` | Requests waiting for their turn or for a pause to end. |
| `RecentRequests` | Requests in the last minute. |
| `Attempts` | Every request sent, retries included. |
| `Refusals` | Requests Massive refused for coming too fast. |
| `PausedUntil` | While paused, when requests may resume. |
| `LastRefusal` | Massive's latest refusal: status, path, request id, and message. |
| `UpdatedAt` | When the row last changed. |

Sessions in one server that use the same key share their limits, since Massive counts their
requests together, so they show the same rows. Programs elsewhere that use the key count
against the same limit without appearing here: a `PAUSED` bucket on a subscription you already
declared often means another program is using it.

Next: [Releasing tables and closing a session](releasing.md).
