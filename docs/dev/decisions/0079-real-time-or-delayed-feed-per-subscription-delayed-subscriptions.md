# 79. The user says which subscriptions stream delayed data; the rest are real-time

Date: 2026-10-31

Status: accepted

## Question

Massive serves real-time and delayed feeds, and plans include one or the other. Who chooses
which feed a stream reads: the library, by inference, or the user?

## Options

1. The library infers the feed from the plan's answers.
2. The user lists the subscriptions that stream delayed data; every other one is real-time.

## Ruling

`MassiveConfig(delayed_subscriptions={dm.Subscription.OPTIONS})` streams options from Massive's
delayed feed; every other subscription uses the real-time feed, and when the plan cannot serve
it, Massive's refusal fails the stream with an error naming this setting. Decision 37's check
raises that error before connecting. The library never infers the feed from the plan. The owner
ruled on 2026-10-31 that a user should ask for current or delayed data explicitly, with
real-time the default; recorded in PR #514. This absorbs decision 15.

## Reason

Nothing is shown as real-time without proof, and a stream never moves between feeds.

## Consequences

A user on a delayed plan must set `delayed_subscriptions`; until then Massive refuses the
real-time stream, and the error names the setting.

## Rule

`docs/configuration.md`, `delayed_subscriptions`.
