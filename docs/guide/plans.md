# Plans

Examples on this page assume `import deephaven_massive as dm` and `m = dm.MassiveSession()`.

This page explains what your Massive plan lets you read, how to choose real-time or delayed live
data, and the errors you get when something is outside your plan.

<a id="plans-and-errors"></a>

## What your plan allows

Massive sells one *subscription* per asset class, each at a *plan* level:

- **Subscriptions:** Stocks, Options, Indices, Currencies (forex and crypto together), and
  Futures.
- **Plans:** from the free Basic plan up to paid plans, by Massive's names, such as Stocks
  Starter or Options Developer. Massive's [pricing page](https://massive.com/pricing) lists them.

One account can hold any mix of subscriptions and plans, and upgrade mid-month. Reference data,
economic data, and news come with every account; partner datasets are sold separately.

The library does not know your plans. It reads what you ask for, and when your plan does not
include it, the call or the table tells you. Two settings tell it what it cannot learn by itself:

| Setting | List in it | Why |
|---|---|---|
| `rest_basic_subscriptions` | Each subscription you hold on the free Basic plan | The library paces its requests to Basic's limit from the first one |
| `delayed_subscriptions` | Each subscription whose live data you accept 15 minutes late | See [real-time or delayed](#real-time-or-delayed) |

```python
config = dm.MassiveConfig(
    rest_basic_subscriptions={dm.Subscription.STOCKS},
    delayed_subscriptions={dm.Subscription.OPTIONS},
)
m = dm.MassiveSession(config)
```

This session paces its Stocks requests for a Basic plan and streams options from Massive's
delayed feed.

What a plan changes:

| | Basic (free) | Paid plans |
|---|---|---|
| How you read | REST only | REST, live WebSocket streams, and, with [S3 keys](install.md#s3-keys), flat files |
| Pace | Five REST requests a minute per subscription. A large download completes, slowly. | No REST limit |
| Freshness of stocks, options, indices, and futures | End of day | Delayed on Starter and Developer plans, real-time on Advanced and Business plans |
| Kinds of data and history | Fewer kinds, and a shorter history: Stocks Basic keeps two years, while Indices Basic keeps all of it | More kinds, such as streamed trades and quotes, and longer history |

Each dataset's page in the [dataset reference](../datasets/README.md) has Massive's plan table:
which plans include the dataset, how fresh its data is, and how far back it goes.

<a id="errors"></a>

## Errors a plan causes

| Error | When |
|---|---|
| `dm.EntitlementError` | Your plan does not include what you asked for |
| `dm.AuthenticationError` | Massive rejected your API key |

Both are kinds of `dm.MassiveError`.

- **A static verb** (`fetch()`, `history()`) raises the error.
- **A live table** stops and shows the error. Its row in `m.streams()` reads `FAILED`, with the
  message, and only that table stops.

Each message names what was refused, gives Massive's own words, and, where the library knows
one, another way to get the data. Each error also carries:

| Attribute | What it holds |
|---|---|
| `status` | The HTTP status, such as 403 or 429 |
| `request_id` | Massive's id for the request, which Massive's support can look up |
| `cause` | Whose problem it is: `dm.ErrorCause.EXPECTED` (a refusal, a limit, a network failure, a closed session), `USER_DEFECT` (your code or input; the message names the fix), or `LIBRARY_DEFECT` (a bug in the library; please report it) |

```python
try:
    old = m.stocks.bars("AAPL", dm.Timespan.DAY).history("2004-01-05", "2004-01-30")
except dm.EntitlementError as error:
    print(f"not on this plan ({error.status}, request {error.request_id}): {error}")
```

On a plan whose history starts after 2004, this prints the refusal instead of raising.

A mistake in the call itself raises `ValueError` or `TypeError` before anything is sent. Such
mistakes include a symbol not spelled as Massive spells it (`aapl`, `EUR/USD`), text with stray
spaces, and text where an enum member belongs.

<a id="real-time-or-delayed"></a>

## Real-time or delayed

Massive streams stocks, options, indices, and futures from two feeds:

- a **real-time** feed;
- a **delayed** feed, 15 minutes behind the market (10 minutes for futures).

Starter and Developer plans get the delayed feed; Advanced and Business plans get real time.
You choose the feed for each subscription with `delayed_subscriptions`. The library never
chooses for you, and a stream never moves from one feed to the other.

```python
# The default: every subscription real-time. On a delayed plan, a stream raises an error
# that names the setting to add.
real_time = dm.MassiveSession()

# Stocks in real time; options from Massive's delayed feed, 15 minutes behind.
mixed = dm.MassiveSession(dm.MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS}))
```

### A subscription you leave out: real-time

- **Streams:** when Massive shows that your plan's data is delayed, the stream verb raises
  `dm.EntitlementError`. The message gives what Massive reported and the exact line to add.
- **Current data over REST:** a `fetch()` or `poll()` of the market's current state (snapshots,
  last trades and quotes, movers, the option chain) fails the same way when Massive labels its
  answer `DELAYED`, and names the tickers. You never get delayed prices you did not ask for.
- **[`m.snapshots()`](../datasets/snapshots.md)**, which mixes asset classes, holds each row to
  its own class's subscription.

### A subscription you list: delayed

- **Streams** read the delayed feed, even when your plan is real-time.
- **Current data over REST:** REST cannot ask for delayed data, so `fetch()` and `poll()` return
  whatever your plan serves, and the table's description says so.

### What the setting never affects

- **Forex and crypto** are real-time on every plan, so `dm.Subscription.CURRENCIES` cannot be
  listed.
- **Fair-market-value streams** (Business plans) are real-time only.
- **Past data** is never checked: `history()`, and the `fetch()` and `poll()` of the previous
  day's bar, a day's open and close, dividends, splits, and every other dataset that is not the
  market's current state. Massive labels some of them `DELAYED` on Basic and Starter plans, but
  their rows are final.
- **Indicators** are the exception to past data: an indicator's latest value comes from the
  latest bars, so its `poll()`, and a `fetch()` with no `end` or an `end` of today or later, are
  checked like current data.

### How to tell what you got

- The `Recency` column of `m.streams()` shows each live table's feed: `REALTIME`, `DELAYED`, or
  `UNKNOWN`.
- Each live table's description, `table.attributes()["TableDescription"]`, says what the library
  knows of it.

Nothing shows as real-time without proof. A stream is real-time by its feed; a `poll()` table
is real-time only once Massive labels its data real-time, and shows `UNKNOWN` until then.
