# 142. Is an unusual options trade's `price` per share of the underlying?

Date: 2026-11-16

Status: open

Unknown: whether `price` of `/options/v1/unusual-trades` is per share of the underlying, as an
options trade's price is elsewhere, or per whole contract, as its field description says ("The
actual dollar value per whole contract of this trade."). Massive's page for the endpoint says a
trade qualifies when "its notional premium (price x size x 100) is $100,000 or more", and its
sample trade of 120 contracts at 15.25 carries `block_premium` only if the price is per share
(15.25 x 120 x 100 = $183,000).

Meanwhile the column's description is Massive's own text for the options trade price, per share
of the underlying, through a `DESCRIPTION_FIXES` entry. If the price is per contract, the entry
goes and the vendor's text returns.

Verified by `test_unusual_trade_prices_are_per_share` in
`tests/namespaces/test_options_integration.py`, which reads one page of unusual trades and
asserts that every trade flagged `block_premium` has price x size x 100 of $100,000 or more and
every other trade less, recording the counts.
