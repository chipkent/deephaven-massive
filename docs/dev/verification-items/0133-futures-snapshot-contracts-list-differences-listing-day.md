# 133. Which futures tickers the snapshot holds and the contracts list lacks, and the reverse

Date: 2026-11-16

Status: open

Unknown: how many tickers traded in the current session the contracts list lacks for its
latest day, and how many contracts it lists that the snapshot lacks, most of all on a day that
lists thousands of new contracts (2026-11-02 listed 3,035 combos and 1,631 singles).

Meanwhile the calendar finds a ticker's product only in the contracts list.

Settled by `tests/ingest/test_futures_calendar_integration.py::test_snapshot_and_contracts_list_differences_on_a_listing_day`, which reads
both lists and asks each for the other's tickers it lacked, up to a cap (beyond it, a share
spread evenly and said to be a lower bound). It also counts the contracts active that day that
were first traded on it, so the evidence says whether it ran on such a day (a lower bound when
the contracts read stop at their page cap).
