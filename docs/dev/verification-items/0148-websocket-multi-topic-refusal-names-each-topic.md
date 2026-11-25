# 148. Does Massive name each topic when it refuses a subscribe frame of several topics?

Date: 2026-11-25

Status: open

Unknown: whether the status Massive sends when it refuses a subscribe frame of two topics names
the topics it refuses, as the fake server's "not authorized to access T.AAPL" does. Every
refusal recorded so far answered a frame of one topic (`FMV.SPY`, `AD.*`) and said only "not
authorized", so the recorded text cannot tell a refusal of the step as a whole from a refusal
per topic.

Meanwhile the library attributes a refusal that names topics to those topics, and one that
names none to the single pending topic when exactly one awaits an answer. When several topics
await and the refusal names none, it fails nothing and the topics stay unacknowledged
(`unacknowledged_topics`). If Massive names each refused topic, a stream of several symbols
the key lacks can be refused symbol by symbol from the text. If it does not, that case keeps
the unacknowledged path, and the fake server's text is corrected to Massive's.

Settled by `test_a_refusal_of_two_topics_in_one_step_names_them` in
`tests/ingest/test_ws_integration.py`. It streams `m.stocks.fmv` of `AAPL` and `MSFT`, which
subscribes both fair market value topics to the business host in one frame, and records every
status object Massive sends on that connection and which of the two symbols each message
names. It skips, quoting the statuses, when Massive acknowledges both topics: only a key
without fair market value gives a refusal to read.
