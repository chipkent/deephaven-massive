# 118. Bars that Massive doesn't stream return a query without stream methods

Date: 2026-11-15

Status: accepted

## Question

Massive streams only 1-second and 1-minute bars. Since 2026-11-12, every `bars()` call
returned a `StreamHistoryQuery`, so `m.stocks.bars("AAPL", dm.Timespan.DAY).blink()` passed a
type checker and tab completion showed `blink()`, but the call raised `ValueError`. Should a
query of bars that don't stream have stream methods at all?

## Options

1. Keep one class, its stream methods documented to raise `ValueError` for bars that don't
   stream: no type checker catches the mistake.
2. Pick the class at run time and declare `HistoryQuery`: stream methods never type-check on a
   bars query, even for minute bars.
3. Separate streaming-bar methods, one per asset class: six new dataset methods.
4. Pick the return type from the timespan with typed overloads: the return type depends on the
   argument values.
5. Declare `StreamHistoryQuery | HistoryQuery` and return the class that fits the call: a type
   checker rejects stream methods until the code checks the class with `isinstance`.

## Ruling

Option 5, which the owner proposed. `bars()` takes any timespan and declares that it returns
either a `StreamHistoryQuery` or a `HistoryQuery`. At run time it returns `StreamHistoryQuery`
for 1-second and 1-minute bars at `multiplier=1`, which Massive streams, and `HistoryQuery`,
with `history()` only, for every other bar. No overloads tie the return type to the argument
values. Its docstring says which class each call returns. The owner ruled on 2026-11-15 in the
coordinating chat session.

## Reason

A type checker then catches a stream method called on bars that might not stream, and the
documentation states the behavior. A return type shouldn't depend on argument values.

## Consequences

Code that calls a stream method on a `bars()` result checks it with
`isinstance(q, StreamHistoryQuery)` first to pass a type checker. At run time, a stream method
on bars that don't stream raises `AttributeError`, not `ValueError`.

## Rule

The `bars()` docstrings and the `deephaven_massive.queries` package docstring.
