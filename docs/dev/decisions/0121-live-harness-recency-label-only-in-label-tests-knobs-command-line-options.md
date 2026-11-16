# 121. Live tests read the recency label only when it is their question; knobs are options

Date: 2026-11-16

Status: accepted

## Question

Some open questions ask what Massive sends a key whose data it labels delayed, real-time, or
not at all, such as whether a real-time key may stream from the delayed feed. Knowing no plan,
a live test learns the label only by asking Massive. May it? And which environment variables may
the live harness read, now that it no longer reads plans?

## Options

1. Read the label, one request per subscription per run, only inside the tests whose question
   is the label, recorded as evidence and never saved to disk.
2. Never read it: those tests record the label and the outcome on any key, and a person judges.
3. Take the owner's `delayed_subscriptions` for the label: no request, but a delayed key without
   the setting passes the real-time question without testing it.

For the variables: keep the harness's own variables, or turn each into a command-line option.

## Ruling

Option 1: a live test reads Massive's recency label only when the label is its question, and
nothing else in the run depends on it. The harness reads only the three key variables,
`DEEPHAVEN_MASSIVE_CONFIG_FILE`, and the two debug-log variables; every other knob of the
harness is a command-line option, and the second, delayed key and its checks are removed. The
owner ruled both on 2026-11-16, in the chat session that removed plans from the live harness.

## Reason

The owner gave no reason. The design behind option 1 noted that the label is Massive's own
answer, while a label read that decides what the run does would be a plan probe.

## Consequences

The checks that needed a second, delayed key no longer run, and their questions stop gathering
live evidence. A tester or the owner passes a run's knobs on its command line.

## Rule

`tests/_live/recency.py` and `docs/live-tests.md`, "Set your keys".
