# 136. Does Massive's futures trades path read a combo whose ticker holds `:` or a space?

Date: 2026-11-16

Status: open

A run read no trade from `/futures/v1/trades/{ticker}` for any of 21 combos whose ticker holds
`:` or a space (`CL:BZ H7-M7`), though the snapshot showed each trading minutes before; combos
without them read their trades. The library sends such a ticker percent-encoded
(`CL%3ABZ%20H7-M7`), and Massive's docs do not say how a path ticker is encoded. If another
spelling reads the trades, the library sends that spelling, and `history()` of such a combo
gets its rows. If none does, Massive's path lacks these combos, and the evidence goes to Massive
in a report. Until it is read, the library sends the percent-encoded ticker, and `history()` of
such a combo may give no rows.

Read by `tests/ingest/test_futures_calendar_integration.py::test_spaced_combo_trades_by_path_spelling`,
on a key with futures trades, while futures trade. It reads one recently traded such combo's
trades with its ticker spelled four ways, a combo without `:` or a space as the control (as the
library sends it, and with its `-` escaped), and `history()` of the combo over the same hour,
and records each read's trades.
