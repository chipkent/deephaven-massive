# Plans

Your Massive plans decide what the library can read for you. Look here to see what a plan
changes, how to choose between real-time and delayed live data, and which exceptions tell you
that something is outside your plan. For a specific error message and its fix, see
[troubleshooting](troubleshooting.md#error-messages).

<a id="plans-and-errors"></a>

## What your plan allows

Massive sells one subscription for each asset class, each at a plan level of its own. The
subscriptions are Stocks, Options, Indices, Currencies (forex and crypto together), and Futures.
The plans run from the free Basic plan up to paid plans such as Stocks Starter or Options
Developer, and Massive's [pricing page](https://massive.com/pricing) lists them all. One account
can hold any mix of subscriptions and plans, and can upgrade mid-month. Reference data, economic
data, and news come with every account, and partner datasets are sold separately.

A plan changes how you can read, how fast, and how fresh and how deep the data is:

| | Basic, the free plan | Paid plans |
|---|---|---|
| How you read | REST only | REST, and on most plans WebSocket streams and, with [S3 keys](install.md#s3-keys), flat files |
| Pace | Five REST requests a minute for each subscription. A large download completes, but slowly. | No REST limit |
| Freshness of stocks, options, indices, and futures | End of day | 15 minutes behind on Starter and Developer plans (10 minutes for futures), and real time on Advanced plans |
| Kinds of data and history | Fewer kinds, and shorter history. Stocks Basic keeps two years, while Indices Basic keeps all of it. | More kinds, such as streamed trades and quotes, and longer history |

These are the general shape, and each dataset has its own plan table. Stocks Starter has no
trades at all, for example, and Stocks Business streams trades only with Massive's Expansion
add-on. Each dataset's page in the [dataset reference](../datasets/README.md) shows which plans
include it, how fresh its data is on each, and how far back its history goes.

### Tell the library about your plans

The library doesn't know your plans. It reads what you ask for, and when your plan doesn't
include it, the call or the table tells you. Two settings tell the library what it can't learn
by itself:

| Setting | What to list in it | What it changes |
|---|---|---|
| `rest_basic_subscriptions` | Each subscription you hold on the free Basic plan | The library paces its requests to Basic's five a minute from the first one, instead of waiting for Massive to refuse one |
| `delayed_subscriptions` | Each subscription whose live data you accept 15 minutes late | Its streams read Massive's delayed feed (see [real-time or delayed](#real-time-or-delayed)) |

This session is for an account with Stocks Basic and Options Starter:

```python
import deephaven_massive as dm

config = dm.MassiveConfig(
    rest_basic_subscriptions={dm.Subscription.STOCKS},
    delayed_subscriptions={dm.Subscription.OPTIONS},
)
m = dm.MassiveSession(config)
```

`m` paces its stock requests for the Basic plan and streams options from Massive's delayed
feed.

<a id="errors"></a>

## Exceptions a plan causes

Two exceptions mean that Massive refused a request because of your plan or your key. Both are
kinds of `dm.MassiveError`:

| Exception | When |
|---|---|
| `dm.EntitlementError` | Your plan doesn't include what you asked for |
| `dm.AuthenticationError` | Massive rejected your API key |

A static verb, `fetch()` or `history()`, raises the exception. A live table stops and shows it
instead, and its row in `m.streams()` reads `FAILED` with the message. Only that table stops.
Each message names what was refused and gives Massive's own words. Where the library knows
another way to get the data, the message names it too.

Each exception also carries three attributes that help you handle it in code:

| Attribute | What it holds |
|---|---|
| `status` | The HTTP status, such as 403 or 429 |
| `request_id` | Massive's id for the request, which Massive's support can look up |
| `cause` | Whose problem it is, as a `dm.ErrorCause` member (see the next table) |

| `cause` | Meaning |
|---|---|
| `dm.ErrorCause.EXPECTED` | A refusal, a limit, a network failure, or a closed session |
| `dm.ErrorCause.USER_DEFECT` | A mistake in your code or input. The message names the fix. |
| `dm.ErrorCause.LIBRARY_DEFECT` | A bug in the library. [Report it](troubleshooting.md#report-a-bug). |

This example catches a refusal instead of letting it stop your script. On Stocks Basic, whose
history starts two years back, it prints the refusal:

```python
import deephaven_massive as dm

m = dm.MassiveSession()
try:
    old = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2004-01-05", "2004-01-30")
except dm.EntitlementError as error:
    print(f"not on this plan ({error.status}, request {error.request_id}): {error}")
```

A mistake in the call itself raises `ValueError` or `TypeError` before anything is sent to
Massive. Such mistakes include a symbol not spelled as Massive spells it, such as `aapl` or
`EUR/USD`, text with stray spaces, and text where an enum member belongs.

<a id="real-time-or-delayed"></a>

## Real-time or delayed

Massive streams stocks, options, indices, and futures from two feeds. The real-time feed is
current, and the delayed feed runs 15 minutes behind the market, or 10 minutes for futures.
Starter and Developer plans get the delayed feed, and Advanced plans get real time. You choose the
feed for each subscription with `delayed_subscriptions`, and a stream never moves from one feed
to the other.

The default is real time for every subscription, and this example shows it beside a session
that takes options from the delayed feed:

```python
import deephaven_massive as dm

# Every subscription real-time. On a delayed plan, a stream raises an error that names the
# setting to add.
real_time = dm.MassiveSession()

# Stocks in real time, and options from Massive's delayed feed, 15 minutes behind.
mixed = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS}))
```

### A subscription you leave out is real-time

You never get delayed prices you didn't ask for. If you leave a subscription out of
`delayed_subscriptions` and your plan's data for it is delayed, each kind of read fails:

| What you read | What happens |
|---|---|
| A stream | The stream verb raises `dm.EntitlementError`. The message gives what Massive reported and the exact line to add. |
| The market's current state over REST: snapshots, last trades and quotes, movers, and the option chain | A `fetch()` or `poll()` fails the same way when Massive labels its answer `DELAYED`, and the message names the tickers |
| [`m.snapshots()`](../datasets/snapshots.md), which mixes asset classes | Each row is held to its own class's subscription |

### A subscription you list is delayed

A stream of a subscription you list reads the delayed feed, even when your plan is real-time.
REST can't ask for delayed data, so a `fetch()` or `poll()` of the current state returns
whatever your plan serves, and the table's description says which.

### What the setting never affects

Some data has only one feed, or is past data that can't be stale:

- Forex and crypto are real-time on every plan, so `dm.Subscription.CURRENCIES` can't be listed.
- Fair-market-value streams, on Business plans, are real-time only.
- Past data is never checked. That covers `history()`, and the `fetch()` and `poll()` of the
  previous day's bar, a day's open and close, dividends, splits, and every other dataset that
  isn't the market's current state. Massive labels some of them `DELAYED` on Basic and Starter
  plans, but their rows are final.
- Indicators are the exception to past data. An indicator's latest value comes from the latest
  bars, so its `poll()`, and a `fetch()` with no `end` or an `end` of today or later, are checked
  like current data.

### How to tell what you got

The `Recency` column of `m.streams()` shows each live table's feed as `REALTIME`, `DELAYED`, or
`UNKNOWN`. Each live table's description, `table.attributes()["TableDescription"]`, says what
the library knows of it. A stream is real-time by its feed. A `poll()` table counts as real-time
only once Massive labels its data real-time, and shows `UNKNOWN` until then.
