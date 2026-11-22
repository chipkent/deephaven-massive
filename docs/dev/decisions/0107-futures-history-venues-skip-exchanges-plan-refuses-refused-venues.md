# 107. Futures `history()` reads every exchange the plan allows; a named one follows `refused_venues`

Date: 2026-11-13

Status: partly superseded: the per-call `refused_venues` argument, by [111](0111-error-policy-on-error-session-and-per-call-settings.md)

Massive publishes futures flat files per exchange, and a plan may include only some. The futures
`trades`, `quotes`, and `bars` take `venues: AbstractSet[FuturesVenue] | None = None`, so a read of
`dm.ALL` or a long range can download less. Left out, a read takes every exchange the plan allows: a
`history()` that REST cannot serve (`dm.ALL`, or `source=FLAT_FILES`) skips every file of an
exchange S3 refuses, over the whole range, so the table has no holes, logs one WARNING naming the
skipped exchanges with Massive's answer, and names them in the table's description (or, for a table
that follows a live symbol table, in each symbol's `Transport` in `m.symbol_reads()`). An exchange
the call names follows `refused_venues: dm.OnError | None = None`, whose `None` means the argument
was left out and `MassiveConfig.refused_venues` (`RAISE` by default) applies, because naming an
exchange the plan lacks is a bug in the caller. A call whose every exchange is refused raises either
way, and a list of tickers REST can serve still reads REST when S3 refuses its files. The library
learns which exchanges a plan includes only from Massive's refusals: it encodes no plan tier and
never guesses a ticker's exchange, since Massive can change its plans. A problem with one value in
the data stays a `MassiveConfig` setting, `LOG` by default. This supersedes decision 93's wording
that one bad row never fails a table: `RAISE` fails it when a user chooses it.
`canonicalize_symbols` keeps its `LOG` default (decision 93); the `OnError` docstring that said
`RAISE` was wrong.
