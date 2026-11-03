## 15. Options contract cap

Massive allows "1,000 option contracts" per `/options` connection. The library counts
**topics** per connection: `T.X` and `Q.X` count as two. Whether the server counts contracts or
topics is **UNVERIFIED**, so the stricter count is used (fixture test) until the live check
(`test_the_options_cap_counts_contracts_or_topics`, 1,000 contracts on `T` and `Q`) shows that
Massive counts contracts. Counting contracts without that evidence could send a frame past
the server's limit, and a server that answers by closing the socket would reconnect, and cut,
every stream on the connection in a loop. Wildcards count zero.
Every wanted explicit topic counts, also one covered by an acknowledged wildcard and so off the
wire meanwhile ([section 13.3](13-host-resolution-and-connection-protocol.md#133-connection-and-subscription-protocol)): the wildcard may be
removed at any time, and a static contract the verb accepted must never be deferred then.

Because the library knows the count before sending, a frame that would cross the cap is never
sent:

- **Static input.** `len(new topics) + in use > 1000` raises `ValueError` at verb time,
  synchronously.
- **Reactive `Table` input.** Static topics are admitted first (their verb checked them);
  a reactive table's adds take the room left, in sorted order, and contracts admitted before
  keep their place, across reconnects too. The rest are deferred: not subscribed, not failed.
  `m.streams()` shows `State=RUNNING`, `Deferred=<n>` (the stream's own deferred topics), and
  `Error="over capacity: 1234/1000; 234 deferred"`, with a WARNING on each transition. When
  removals free capacity, deferred contracts subscribe in the same order. Rows for deferred
  contracts are absent, never stale, except while a wildcard on the connection delivers them
  anyway.

Since a frame never crosses the cap, frame chunking has no interaction with it. How the
server answers a subscription past its own count (refusing the topics past it one by one, or
closing the socket) is **UNVERIFIED**.
