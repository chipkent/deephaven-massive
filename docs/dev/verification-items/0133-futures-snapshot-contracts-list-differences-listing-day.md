# 133. Which futures tickers does the snapshot hold that the contracts list lacks, and the reverse?

Date: 2026-11-16

Status: open

How many tickers traded in the current session the contracts list lacks for its latest day is
unknown, and so is how many contracts it lists that the snapshot lacks. The question matters
most on a day that lists thousands of new contracts (2026-11-02 listed 3,035 combos and 1,631
singles). If the two lists hold the same tickers, either one can name a product, and the second
only covers gaps. If they differ, the calendar needs both lists, in the order that misses
fewer. Until it is read, the calendar finds products in the contracts list only.

Read by `tests/ingest/test_futures_calendar_integration.py::test_snapshot_and_contracts_list_differences_on_a_listing_day`, on a key with
futures data, best on a listing Monday. It reads both lists and asks each for the other's
tickers it lacked, up to a cap (beyond it, a share spread evenly, said to be a lower bound). It
also counts the contracts active that day that were first traded on it, so the evidence says
whether it ran on such a day (a lower bound when the contracts read stop at their page cap).
