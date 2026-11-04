# Plans

Your Massive plans decide which datasets you can read, how fast, and how fresh the data is. Tell
the library which subscriptions are on the free Basic plan and which you accept delayed. It
then paces your requests and picks the live feed to match. When you ask for something
your plan lacks, you get `dm.EntitlementError`. For a specific error message and its fix, see
[troubleshooting](troubleshooting.md#error-messages).

<a id="plans-and-errors"></a>

## See what your plan includes

Massive sells one subscription for each asset class: Stocks, Options, Indices, Currencies (forex
and crypto together), and Futures. Each subscription has its own plan, from the free Basic plan
up to paid plans such as Stocks Starter or Options Developer. Massive's
[pricing page](https://massive.com/pricing) lists them all.

You can hold Stocks on Basic and Options on Starter at the same time, and you can upgrade one
mid-month. After an upgrade, a call your plan refused before works the next time you make it.

Some data doesn't depend on a subscription. Reference data, economic data, and news come with
every account, and partner datasets are sold separately.

The free plan and the paid plans differ in four ways. In the table, REST is Massive's request
API, which answers each request with rows, and WebSocket is its streaming API:

| | Basic, the free plan | Paid plans |
|---|---|---|
| How you read | REST only, with no live streams | REST and, on most plans, WebSocket streams. With [S3 keys](install.md#s3-keys), flat files too. |
| Pace | Five REST requests a minute for each subscription, one every 12 seconds. A download of 100 requests takes about 20 minutes. | No REST limit |
| Freshness of stocks, options, indices, and futures | End of day | 15 minutes behind on Starter and Developer plans (10 minutes for futures), and real time on Advanced and Business plans |
| Kinds of data and history | Fewer kinds, and shorter history. Stocks Basic keeps two years, and Indices Basic keeps all of it. | More kinds, such as streamed trades and quotes, and longer history |

Each dataset has its own plan table, with exceptions to that pattern. Stocks Starter has no
trades at all, for example, and Stocks Business streams trades only with Massive's Expansion
add-on. Each dataset's page in the [dataset reference](../datasets/README.md) shows which plans
include it, how fresh its data is on each, and how far back its history goes.

## Tell the library your plans

The library can't learn two facts about your plans by itself, so you give them to it as
settings:

| Setting | What to list in it | What it changes |
|---|---|---|
| `rest_basic_subscriptions` | Each subscription you hold on the free Basic plan | The library spaces its requests to Basic's five a minute from the first one, instead of waiting for Massive to refuse one |
| `delayed_subscriptions` | Each subscription whose live data you accept 15 minutes late | Its streams read Massive's delayed feed. Leave a delayed plan's subscription out, and its streams raise `dm.EntitlementError` (see [real-time or delayed](#real-time-or-delayed)). |

This example creates a session for an account with Stocks Basic and Options Starter. Paste it
into the console of the Deephaven IDE:

```python
import deephaven_massive as dm

config = dm.MassiveConfig(
    rest_basic_subscriptions={dm.Subscription.STOCKS},
    delayed_subscriptions={dm.Subscription.OPTIONS},
)
m = dm.MassiveSession(config)
```

The console shows nothing, and `m` is ready to use. It spaces its stock requests for the Basic
plan, and it streams options from Massive's delayed feed.

<a id="real-time-or-delayed"></a>

## Choose real-time or delayed

Massive streams stocks, options, indices, and futures from two feeds, its two sources of live
data. The real-time feed is current, and the delayed feed runs 15 minutes behind the market, or
10 minutes for futures. Starter and Developer plans stream from the delayed feed, Advanced and
Business plans from the real-time feed, and Basic plans don't stream at all.

You choose the feed for each subscription with `delayed_subscriptions`. A stream stays on the
feed it starts on.

Every subscription is real-time unless you list it. This example creates one session of each
kind and opens one stream from each, in the console:

```python
import deephaven_massive as dm

# Real-time for everything: on a delayed Stocks plan, the stream below raises an error.
real_time = dm.MassiveSession()
aapl = real_time.stocks.trades("AAPL").last_by()

# Options listed as delayed, so an Options Developer plan can stream them.
mixed = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS}))
spy_call = mixed.options.trades("O:SPY271217C00700000").last_by()
```

On Stocks Advanced, `aapl` fills with Apple's latest trade as it trades. On Stocks Developer,
the `aapl` line raises `dm.EntitlementError`, and its message ends with the line to add to your
settings. On Options Developer, `spy_call` streams that contract's trades 15 minutes late.

### Leave a subscription real-time

You never get delayed prices you didn't ask for. If you leave a subscription out of
`delayed_subscriptions`, each kind of read checks what Massive sends:

| What you read | What happens when your plan's data is delayed |
|---|---|
| A stream | It raises `dm.EntitlementError`, with what Massive reported and the line to add |
| Current data over REST: snapshots, last trades and quotes, movers, and the option chain | A `fetch()` or `poll()` fails the same way when Massive labels its answer `DELAYED`, and the message names the tickers |
| An indicator's latest value, which comes from the latest bars | Checked like current data: its `poll()`, and its `fetch()` with no `end` or an `end` of today or later |
| [`m.snapshots()`](../datasets/snapshots.md), which mixes asset classes | The library checks each row against the subscription of its own asset class. A delayed stock row fails unless `STOCKS` is listed, even when `OPTIONS` is. |

A stream learns that your data is delayed from a snapshot of its asset class, which it reads
before it connects. When Massive's snapshot carries no delayed label, the stream connects to the
real-time feed and fails there if Massive refuses it.

### List a subscription as delayed

A stream of a subscription you list reads the delayed feed, even when your plan is real-time.
Massive's request API has no delayed option, so a `fetch()` or `poll()` of current data returns
what your plan serves, real-time or delayed.

### Know what the setting doesn't affect

Some data has only a real-time feed, and the library never checks past data:

- Forex and crypto are real-time on every plan, so `dm.Subscription.CURRENCIES` can't be listed.
- Fair-market-value streams, on Business plans, are real-time only. Stream them from a session
  whose `delayed_subscriptions` doesn't list their subscription, because a listed one makes them
  raise `ValueError`.
- Past data, whose rows are final, is never checked. That covers `history()`, and the `fetch()`
  and `poll()` of the previous day's bar, a day's open and close, dividends, splits, and every
  other dataset that isn't current data. Massive labels some of them `DELAYED` on Basic and
  Starter plans.

### Check which feed a table uses

The `Recency` column of `m.streams()` shows whether each live table's rows are real-time, delayed,
or not yet known, as `REALTIME`, `DELAYED`, or `UNKNOWN`. A stream shows the feed it reads, or
`UNKNOWN` when you pinned its host in `websocket_hosts`. A `poll()` table shows `UNKNOWN` until
Massive labels its data, and `REALTIME` only once that label says real time.

Each table also carries a description, `table.attributes()["TableDescription"]`. It names the
dataset and says how fresh the data is. On a `history()` table, it also names the source, REST
or flat files.

<a id="errors"></a>

## Handle a refusal

Two exceptions mean that your key or your plan stopped a read. Both are kinds of
`dm.MassiveError`:

| Exception | When |
|---|---|
| `dm.EntitlementError` | Your plan doesn't include what you asked for |
| `dm.AuthenticationError` | No API key is set, or Massive rejected your API key or your S3 keys |

A `fetch()` or `history()`, which returns a finished table, raises the exception. A stream that
your plan serves only delayed raises it when you open the stream. A refusal that comes after a
live table has started stops that table instead. It shows the error, its row in `m.streams()`
reads `FAILED` with the message, and your other tables keep running.

The message names what was refused and quotes Massive's answer. When another dataset has the
data, the message names that dataset too.

Each exception has three attributes you can branch on:

| Attribute | What it holds |
|---|---|
| `status` | The HTTP status, such as 401 or 403, or `None` when no HTTP response was involved |
| `request_id` | Massive's id for the request, which Massive's support can look up, or `None` when there was no response |
| `cause` | Whose problem it is, as a `dm.ErrorCause` member |

`cause` takes one of three values:

| `cause` | Meaning |
|---|---|
| `dm.ErrorCause.EXPECTED` | A refusal, a limit, a network failure, or a closed session |
| `dm.ErrorCause.USER_DEFECT` | A mistake in your code or input. The message names the fix. |
| `dm.ErrorCause.LIBRARY_DEFECT` | A bug in the library, or data from Massive it can't read. [Report it](troubleshooting.md#report-a-bug). |

This example catches a refusal and prints it, so your script carries on. Run it in the console
with a key on Stocks Basic, whose history reaches back only two years, so Massive refuses the
January 2004 request:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
try:
    old = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2004-01-05", "2004-01-30")
except dm.EntitlementError as error:
    print(f"not on this plan ({error.status}, request {error.request_id}): {error}")
```

The console prints one line that starts `not on this plan (403, request`, followed by Massive's
request id and the refusal's message. On a plan whose history reaches 2004, it prints nothing,
and `old` holds the bars.
