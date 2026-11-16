# 111. Error settings are one policy, set for the session and overridden per call

Date: 2026-11-15

Status: accepted

## Question

A user whose session skips bad watchlist rows wants one call to fail on them instead. The five
error settings (`noncanonical_symbols`, `unreadable_symbols`, `unavailable_symbols`,
`malformed_rows`, `refused_venues`) were flat fields of the session's configuration, and only the
futures methods took a per-call `refused_venues` argument, so no other call could change one.

## Options

1. Keep the flat settings and add a per-call argument for each cause.
2. Gather the causes into one policy object, set for the session and passed to any data method,
   each cause unset in the call falling back to the session, then to the defaults.

## Ruling

Option 2, decided by the owner in the coordinating chat session: the layered policy and the
argument name `on_error` on 2026-11-14, and on 2026-11-15 a positional first argument that sets
every cause the call doesn't name, and the removal of the futures methods' `refused_venues`
argument, whose job `on_error` now does. Every cause keeps its default: `LOG`, except
`refused_venues`, which stays `RAISE`.

## Reason

One place to set what a problem does, at the session or for one call, in place of a setting per
cause and a per-call argument for one of them.

## Consequences

Code that set a flat field or passed `refused_venues=` must pass `dm.ErrorPolicy` instead; no
compatibility is kept before 0.1.0. A stream whose call sets a different policy is its own
stream. This replaces decision 107's per-call `refused_venues` argument; the rest of 107 holds.

## Rule

The `dm.ErrorPolicy` docstring, and `docs/configuration.md`, "on_error".
