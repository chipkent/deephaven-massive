# 149. Does the unified snapshot of a stock and an index label the envelope `DELAYED`, or only rows?

Date: 2026-11-25

Status: open

Unknown: where Massive puts the `DELAYED` label when one `/v3/snapshot` request names tickers
of two asset classes and the key's data of one class is delayed. The envelope's `status` would
then say `DELAYED` for the whole page with no ticker to blame, or each row would carry its own
`timeframe` and the envelope would say `OK`. No recorded answer covers a mixed request.

Meanwhile the library reads the labels of an answer in both places, an envelope `DELAYED`
first and then each row's timeframe fields (`read_labels`), and treats an envelope `DELAYED`
as evidence about every class the request named. If Massive labels the envelope, that branch
is exercised and pinned; if it labels rows only, the branch stays as one case of the closed
set of label places, covered by a test on the fake server.

Settled by `test_a_mixed_class_snapshot_labels_rows_not_the_envelope` in
`tests/namespaces/test_stocks_integration.py`. It sends one `/v3/snapshot` request for `AAPL`
and `I:SPX` and records the envelope's `status`, each row's ticker, `type`, `market_status`,
`error`, and recency labels, and the labels read as the library reads them. It skips when no
label says `DELAYED`, since the question has no subject for a key whose data is real-time.
