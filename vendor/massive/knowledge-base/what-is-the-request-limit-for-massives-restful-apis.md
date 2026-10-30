# What is the request limit for Massive's RESTful APIs?

Massive counts requests per asset class, not per account. Every asset class you have not subscribed to sits on its free Basic tier, which allows 5 requests a minute, and every asset class you pay for has no request limit. So an account with a paid Options plan makes unlimited options requests and is still capped at 5 a minute for stocks, and that mismatch is the most common reason people contact us about rate limits.

## Applies to

- Plans: the free Basic tier of each product allows 5 requests a minute; every paid tier of a product is unlimited for that product.
- Endpoints: every REST endpoint under `https://api.massive.com`. WebSocket connections are counted separately and are not requests.
- Asset classes: stocks, options, indices, currencies and futures, each counted on its own.

## What your account actually allows

What your account allows is one limit per asset class. A product you have never bought is not absent from that list, it is present at its free tier:

| The request is for | Your limit |
|---|---|
| An asset class you have a paid plan for | No limit |
| An asset class you have not bought | 5 a minute, that product's Basic tier |
| Reference and cross-asset endpoints | Counted in their own bucket rather than under one asset class |

Creating more API keys does not change any of this. The limits belong to the account's subscriptions, which is why customers who generate a second key see exactly the same behavior.

## If you upgraded and are still limited

If you upgraded and are still limited, work through these in order. The first is the answer far more often than the rest combined:

1. **Check which asset class the request is for.** A paid Options plan does nothing for a request about `AAPL`, and a paid Stocks plan does nothing for `X:BTCUSD`. An ETF such as SPY is a stock; an index option is Options; forex and crypto are both Currencies.
2. **Check that the subscription is for the product you meant to buy**, on your [subscriptions page](https://massive.com/dashboard/subscriptions).
3. **Do not regenerate the key.** A new key inherits the same subscriptions, and it breaks anything already using the old one.
4. **If the request really is for a product you pay for**, start a chat at [massive.com/contact](https://massive.com/contact) with the `request_id` from the 429 response. That identifies the exact request, including which asset class we counted it under.

## Why it works this way

It works this way because a subscription is per product rather than per account: you buy Stocks, or Options, or Currencies, and each one carries its own tier. There is no account-level plan to lift a limit globally, so an account is best understood as five independent subscriptions that happen to share a login and a key. That is also why an upgrade needs no new key: the entitlement is attached to the account's products and every existing key reads the same set.

## If you see an error

`{"status":"ERROR","error":"You've exceeded the maximum requests per minute, please wait or upgrade your subscription to continue. https://massive.com/pricing"}` with HTTP 429 means this request's asset class is on its Basic tier. Check the asset class before anything else.

`{"status":"NOT_AUTHORIZED","message":"Your plan doesn't include this data timeframe..."}` with HTTP 403 is a different problem: you are asking for data more recent than that product's tier covers. The same request against an older date will usually succeed, which is the quickest way to confirm it.

A 429 on an asset class you definitely pay for is worth reporting rather than retrying. Send the `request_id`.

## Related

- [What are the different Massive subscriptions I can use?](https://massive.com/knowledge-base/article/what-are-the-different-massive-subscriptions-i-can-use)
- [Can I create multiple Massive API keys?](https://massive.com/knowledge-base/article/can-i-create-multiple-massive-api-keys)
- [Why am I not authorized (NOT_AUTHORIZED) when calling Massive's REST API or WebSockets?](https://massive.com/knowledge-base/article/why-am-i-not-authorized-when-attempting-to-connect-to-massives-websockets)
- [How can I upgrade my Massive account?](https://massive.com/knowledge-base/article/how-can-i-upgrade-my-massive-account)
