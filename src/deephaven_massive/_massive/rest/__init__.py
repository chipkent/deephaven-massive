"""The REST client of the Massive wire package.

It speaks Massive's REST protocol and nothing else: it knows paths, parameters, envelopes, and
fields from `spec`, and returns plain pages and Arrow batches; datasets, tables, and sessions
are the library's. It may import only `net` and `spec` from `_massive`.

- `RestClient` (`rest.client`): one per session, shared by every thread. It sends GET requests
  with bearer authentication, retries transient failures per `net.retry`, takes a permit of
  the governor before every attempt, and follows `next_url` pages on its own host only.
- `RequestGovernor` (`rest.governor`): one per API key and REST origin, shared by the clients
  that use them. It paces each subscription at the limit the user declared or the one it
  learned from Massive's refusal, and pauses a subscription Massive refused with HTTP 429;
  `BucketState` is one subscription's snapshot.
- `Page` (`rest.response`): one decoded 200 response: its envelope `status` and `request_id`,
  its rows, and its body. Decoding is strict; a malformed body raises `WireError`.
- `to_batch` (`rest.batch`): a page's rows as one Arrow batch of the path's generated fields,
  with the `DriftReport` of what the rows had extra or lacked.
- `rest.query` builds request targets, encoding each value by its `ParamKind` with
  `spec.values.encode_value`, and checks every `next_url`.

Example:
    ::

        from deephaven_massive._massive import RequestGovernor, RestClient, to_batch

        governor = RequestGovernor(("stocks", "other"))
        client = RestClient(
            "my-key", base_url="https://api.massive.com", max_concurrency=8, governor=governor
        )
        page = client.get(trades, {"stockTicker": "AAPL"}, {"limit": 1000}, bucket="stocks")
        batch, drift, marks = to_batch(page.rows, trades, required={"SipTimestamp"})
        client.close()

    where `trades` is the `RestPath` of `/v3/trades/{stockTicker}`.
"""

from deephaven_massive._massive.rest.batch import to_batch
from deephaven_massive._massive.rest.client import RestClient
from deephaven_massive._massive.rest.governor import BucketState, RequestGovernor
from deephaven_massive._massive.rest.response import Page

__all__ = ["BucketState", "Page", "RequestGovernor", "RestClient", "to_batch"]
