# 114. No code or live test names, probes, or branches on Massive's plan tiers

Date: 2026-11-15

Status: accepted

Neither the library nor its tests may name Massive's plan tiers (Basic, Starter, Developer, Advanced, Business), probe for them, or branch on them. That includes the live-test harness, its markers and request caps, and the messages either one writes. Everything must work whatever plans the owner or a user holds, and keep working when they change plans. Whether a key may read a dataset is Massive's answer. A request is sent, and an entitlement refusal (HTTP 403, `NOT_AUTHORIZED`) is reported as Massive's refusal, with Massive's own text. A live test that gets one skips and quotes it.

The owner gave three reasons:

- On 2026-10-29: "The plan needs to work no matter what I buy, and I may scale up what I'm buying at any given point."
- On 2026-11-11: "it is problematic for the software to know massive's plan tiers. Massive can change these in time. I've seen data vendors keep old users on a legacy plan while moving new or monthly users to a new plan. I'm concerned that the changes I think you are proposing will be extremely brittle in practice."
- On 2026-11-15, after the live harness's plan probe aborted a run on a Saturday because Massive's forex snapshot came back empty: "We have REPEATEDLY discussed that this is not acceptable."

User documentation may still describe what Massive offers. On 2026-10-30 the owner said: "I think you want to show what massive supports with a plan, that is fine. Just make it easy to update."

The live-test harness on `main` still encodes tiers when this is recorded. It has the plan probe in `tests/_live/probe.py`, `needs("<class>:<tier>")` markers, per-tier caps in `tests/_live/pacing.py`, and `MASSIVE_LIVE_PLANS`. Its redesign brings it in line with this decision. The library's existing settings that mention a tier, `rest_basic_subscriptions` and the documentation of delayed subscriptions, are inventoried in that redesign. Each goes to the owner before it changes.
