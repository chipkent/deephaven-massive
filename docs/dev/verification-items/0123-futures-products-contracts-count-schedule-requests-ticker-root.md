# 123. How many futures products and active contracts Massive lists, what loading every schedule costs, and whether a single contract's ticker root is its product code

Date: 2026-11-15

Status: open

Loading every futures product's schedule when a stream of every contract opens would date each
contract's first rows without a lookup per product; its cost depends on how many products and
active contracts Massive lists and how many schedule events a day holds, which nothing
documents. Separately, the futures calendar finds a contract's product by asking the contracts
list; whether a single contract's ticker, less its month letter and year, always equals its
`product_code` is unverified, so the library never derives the product from the ticker. Settled
by `tests/namespaces/test_futures_integration.py::test_count_of_futures_products_contracts_and_schedule_pages`,
which counts the products, the active contracts, and one day's schedule events, and compares each
single contract's ticker root with its product code.
