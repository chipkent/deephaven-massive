# 82. Whether Massive acknowledges `AD.*` on `socket.massive.com` for Options Advanced, what its admin messages carry, and what it answers a plan without them

Date: 2026-11-10

Status: open

The channel is read as documented: real-time only, every exchange at once, the text as sent.
Verified by `tests/namespaces/test_options_integration.py`, which needs a key Massive streams the
admin messages to, and one it refuses them.
