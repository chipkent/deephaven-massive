# 148. Does Massive name each topic when it refuses a subscribe frame of several topics?

Date: 2026-11-25

Status: settled 2026-11-25: no; Massive answers a refused step of several topics with "not authorized" statuses that name none of them

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

Settled by the owner's live run of 2026-11-25 (`live-evidence-20261125T214724Z.log`):
subscribing `FMV.AAPL` and `FMV.MSFT` together on `business.massive.com` drew four status
objects `status 'error': 'not authorized'`, none naming either symbol, and neither topic was
acknowledged. Left on the unacknowledged path, a refused topic would be re-sent forever. So
the library now takes such a refusal as the step's and finds the refused topics by sending the
unanswered ones again in parts, one part at a time (`_ingest/subscriptions.py`,
`_ingest/refusal_search.py`); a topic it finds refused is final and never re-sent. The test,
`test_a_refusal_of_two_topics_in_one_step_names_neither_and_the_library_finds_both` in
`tests/ingest/test_ws_integration.py`, streams `m.stocks.fmv` of `AAPL` and `MSFT`, records
every status object and the stream's state, and fails if a refusal names a symbol or the stream
does not fail naming both subscriptions. It skips when Massive acknowledges both topics: only a
key without fair market value gives a refusal to read.
