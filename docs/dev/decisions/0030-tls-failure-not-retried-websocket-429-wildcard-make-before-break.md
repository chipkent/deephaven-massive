# 30. Wire behavior

Date: 2026-10-26

Status: partly superseded: its REST 429 part by [60](0060-rest-rate-limit-per-subscription-429-retry-after.md) and [62](0062-rest-read-gives-up-after-pauses-reference-bucket-basic-plan.md)

A TLS certificate failure is never retried, a 429 at the WebSocket upgrade is retried, and wildcard
transitions make before they break, accepting a brief duplicate rather than a gap. *Its REST 429
part is superseded by 60 and 62.*
