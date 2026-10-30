# Plans and entitlement errors

The same code runs on every Massive plan. The library never asks you which plan you hold, and
never guesses: it asks Massive, uses what your key can reach, and when something is out of
reach, tells you in an error that names what was refused and what you can do instead. This
page explains what plans change and how to read those errors.

## How Massive sells data

Massive sells one subscription per asset class: **Stocks**, **Options**, **Indices**,
**Currencies** (forex and crypto together), and **Futures**. Each subscription has its own
plan, from the free **Basic** plan up through paid plans that Massive names Starter,
Developer, Advanced, and Business (not every class offers every level). One account can hold
any mix, for example Currencies on a paid plan and everything else on Basic, and you can
upgrade during the month. Reference data, economic data, and news come with every account;
partner datasets, such as Benzinga's, are sold separately.

Massive's [pricing page](https://massive.com/pricing) is the authority on what each plan
includes, and each dataset's page in Massive's documentation has a "Plan Access" table. The
summary below is a guide to what you will notice in the library, not a price list.

## What a plan changes

**Which transports you can use.** Basic plans read over REST only: no WebSocket streams and
no flat files. Paid plans add WebSocket streams, and flat files with S3 keys.

**How fast you can ask.** A Basic subscription allows five REST requests a minute; paid plans
have no request limit. The library spaces its requests to fit the limit, so a large download
on Basic is slow but completes. Requests to reference data and the other datasets that belong
to no subscription share a limit of their own.

**How fresh the data is.** Lower stock, option, and index plans see the market 15 minutes
late; higher plans see it in real time. Currencies plans are real time. The library connects
to Massive's delayed or real-time servers to match your plan, and `m.connections()` shows
which one (its `Recency` column: `REALTIME`, `DELAYED`, or `UNKNOWN`). A delayed stream works
exactly like a real-time one; its rows are simply 15 minutes old.

**Which kinds of data you can see.** For stocks and options, the lower paid plans stream bars;
individual trades and then quotes need higher plans, and the fair-market-value streams need
Business. Snapshots need a paid plan.

**How far back you can read.** Each plan keeps a fixed window of history, two years on every
Basic plan.

On a free plan, then, you can read historical bars, previous-day bars, reference data, news,
economic data, and indicators over REST, and make them live with `poll()`. Streams and flat
files need a paid plan for their class.

## What the library does when something is out of reach

Every refusal is a `dm.EntitlementError`, a kind of `dm.MassiveError`. A static verb
(`fetch()` or `history()`) raises it. A live table fails with it: the table shows the error in
the web UI, the library logs it, and `m.streams()` shows the stream as `FAILED` with the
message in its `Error` column. Only the refused table fails; your other tables keep running.

Each message says what was refused: the subscription, the transport, the request, and
Massive's own words. Where the library has another way to get the data, the message names
it. The common cases:

| What you see | What it means | What to do |
|---|---|---|
| `stocks plan has no WebSocket access` (or `options`, `indices`, `futures`) | The plan for that class reads over REST only. | Use a REST dataset with `poll()`, such as `m.stocks.snapshots(symbols).poll(15)`, or `history()` for the past. |
| `... refused T.AAPL: <Massive's message>; the plan may not include this data on this host` | Massive refused that one stream: the plan includes some streams of the class but not this one. | Stream a dataset the plan includes, such as bars instead of trades, or use the dataset's REST counterpart. |
| `HTTP 403 ...` with Massive's message, from `fetch()`, `poll()`, or `history()` | The plan does not include this REST dataset. | Use another dataset; the message names one where the library has it. |
| `Your plan doesn't include this data timeframe` | The range starts before the plan's history window. | Ask for a later start. |
| `the plan does not include these flat files` | Your S3 keys do not reach the flat files for this data. | Pass `source=dm.HistorySource.REST` to `history()`. |
| `the plan does not include CME futures flat files ...` | Futures flat files are sold per exchange. | Pass `venue=` naming an exchange your plan includes. |

A futures plan sold per exchange works the same way: `venue=dm.FuturesVenue.CME` (or `CBOT`,
`NYMEX`, `COMEX`) on a futures dataset reads that exchange only.

### Rate limits

When Massive refuses a request for coming too fast (HTTP status 429), the library pauses that
subscription's requests and retries, and logs one WARNING that explains it. On a Basic
subscription the WARNING says the requests are now paced at five a minute, and suggests
declaring the subscription in `MassiveConfig(rest_basic_subscriptions=...)` so the next
session paces from its first request. `m.rate_limits()` shows each subscription's pace and
pauses as a live table. A read that cannot get through within ten minutes of pauses gives up
with a `MassiveError`.

### Bad credentials

A rejected key is a `dm.AuthenticationError`, also a kind of `dm.MassiveError`. A rejected API
key fails every stream on the connection; rejected S3 keys fail the `history()` call, with a
message naming `s3_access_key_id` and `s3_secret_access_key`.

## Handling errors in code

Catch `dm.MassiveError` to handle every failure the library or Massive reports, or one of its
two kinds to handle just that case. Each error carries `status`, the HTTP status when the
error came from an HTTP response (such as 403 or 429), and `request_id`, Massive's id for the
request, which Massive's support can look up.

```python
import deephaven_massive as dm

m = dm.MassiveSession()

try:
    bars = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2010-01-04", "2010-01-29")
except dm.EntitlementError as error:
    print(f"not on this plan ({error.status}, request {error.request_id}): {error}")
```

Mistakes in the call itself, such as a misspelled symbol or a plain string where an enum
member belongs, are not `MassiveError`s. They raise `ValueError` or `TypeError` at once, before
anything is sent.

Next: [Reading `m.streams()` and `m.connections()`](diagnostics.md).
