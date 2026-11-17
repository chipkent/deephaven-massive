# 140. Is the `root` of the options open interest and unusual trades always the ticker's OCC root?

Date: 2026-11-16

Status: open

Unknown: whether the `root` field of `/options/v1/open-interest` and `/options/v1/unusual-trades`
always equals the root of the row's own ticker, the OCC symbol without its `O:` prefix and its
last 15 characters. Massive describes `root` as "The OCC root symbol of the contract (e.g.,
SPXW). Differs from the underlying for weekly roots and for contracts adjusted after a corporate
action (e.g., AMCR1).", which is how every other options dataset's `OptionRoot` is read from
`Symbol`, and the samples agree (`SPXW` for `O:SPXW261218C06000000`).

Meanwhile the two datasets give Massive's `root` the name `OptionRoot`, in place of the column
the library reads from `Symbol` on every other options dataset. If a row's `root` differs from
its ticker's root, the two are different fields: `root` takes a name of its own and the
library's `OptionRoot` returns beside it.

Verified by `test_root_is_the_ticker_s_occ_root` in
`tests/namespaces/test_options_integration.py`, which reads one page of open interest of each of
a standard root (`SPY`), a weekly root (`SPXW`), and an adjusted root (`AMCR1`), each root on
its own, and one page of the latest unusual trades of any root. It asserts that `SPY` and `SPXW`
have rows and that every row's `root` is its ticker's root, recording the roots and counts it
saw; an adjusted root with no row that day is recorded, not failed.
