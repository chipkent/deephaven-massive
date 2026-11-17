# 115. Request rates come from Massive's refusal or from the user; the library holds none

Date: 2026-11-15

Status: accepted

## Question

How does a session know a subscription's REST request limit? The library paced a subscription at
a built-in 5 requests a minute. It did so once the user listed the subscription in
`rest_basic_subscriptions`, or once a 429 said "exceeded the maximum requests per minute".
Decision 114 bars a plan's rate from the code.

## Options

1. Keep the built-in 5. It is wrong for any other limit and puts a plan fact in the code.
2. Learn the limit from Massive's answers, and let the user declare it:
   `rest_requests_per_minute`, a mapping of subscription to requests per minute, replaces
   `rest_basic_subscriptions`, with no alias, since nothing is released.

## Ruling

Option 2, approved by the owner in the coordinating chat session on 2026-11-15, as designed.
The library holds no rate. A subscription's limit is the one the user declares. Without one, it
is the number of requests Massive answered in the clock minute of its refusal. That number is
learned when the refusal's pause ends, later refusals may lower it, and it never rises by
itself. The owner asked that the README be considered for explaining this.

## Reason

Massive sets each limit and changes its plans over time (decision 114). A built-in limit would
also slow every user whose plan allows more, which the owner raised. An undeclared subscription
therefore runs at full speed until Massive says otherwise.

## Consequences

A limited subscription the user doesn't declare runs at full speed until its first 429, pauses
a minute, and then keeps to the learned count. If another client of the account used part of
that minute, the count is low, and it stays low until the user declares the limit. Users list
each limited subscription with its number. The README says how to declare a limit so reads are
paced from the first request.

## Rule

`src/deephaven_massive/_massive/rest/governor.py`, "Learning", and
`MassiveConfig.rest_requests_per_minute`.
