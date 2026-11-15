# 60. A REST rate governor per subscription

Date: 2026-10-28

Status: partly superseded: its giving up, its `other` bucket, and what is learned by [62](0062-rest-read-gives-up-after-pauses-reference-bucket-basic-plan.md); its `rest_basic_subscriptions` and its learning of a Basic plan from a 429 by [114](0114-code-never-knows-massive-plans-learns-from-answers.md)

One bucket per `dm.Subscription` plus an unpaced `other` bucket; `rest_basic_subscriptions` paces a
subscription from its first request, and a 429 naming the per-minute limit teaches the bucket it is
Basic. A 429 pauses (honoring `Retry-After` up to 10 minutes), then one probe goes alone. *Revised
by 62.*
