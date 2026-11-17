# 125. Adjusted has no effect on a crypto pair, so its request leaves it out

Date: 2026-11-16

Status: accepted

## Question

`m.indicators.sma(watchlist, adjusted=False).poll(60)` over a watchlist holding AAPL and
X:BTCUSD left the pair out of every poll, and a `fetch()` of the same arguments raised
`ValueError`, because Massive's crypto indicator path declares no `adjusted` parameter. Should
the library refuse `adjusted=False` for a crypto pair, or leave it out of the pair's request?

## Options

1. Keep refusing `adjusted=False` for a crypto pair, and give that refusal in a `poll()` its own
   error setting.
2. Leave `adjusted` out of a crypto pair's request whatever its value, as was already done for
   `True`, and read the pair.

## Ruling

Option 2, decided by the owner in the coordinating chat session on 2026-11-16: the library
ignores `adjusted` for a crypto pair whatever its value, and leaves it out of the pair's
request.

## Reason

`adjusted` chooses whether prices are adjusted for splits, and a crypto pair has no splits, so
`True` and `False` mean the same thing for it.

## Consequences

An indicator call with `adjusted=False` reads crypto pairs beside stocks. Any other parameter
a path lacks, whose absence would change the data, is still refused.

## Rule

The indicator methods' `adjusted` argument, and `shared_params` in
`src/deephaven_massive/_ingest/rest_requests.py`.
