# 1. No SDK: the library owns its REST and WebSocket clients

Date: 2026-10-26

Status: accepted

## Question

Should the library build on Massive's Python SDK, `massive-com/client-python`, or speak
Massive's documented protocols itself?

## Options

1. Use the SDK, with workaround layers around its defects.
2. Use a fork of the SDK that fixes them.
3. Own REST and WebSocket clients on `urllib3` and `websockets`.

## Ruling

The library has its own REST and WebSocket clients, kept in one private package, and never
builds on `massive-com/client-python` or the owner's fork of it. The owner asked on 2026-10-26
whether to drop the SDK and use the REST and WebSocket APIs directly, in one package; recorded
in PR #514.

## Reason

The SDK needed about fourteen workarounds and still dropped data, and a fixed fork could not be
a PyPI dependency.

## Consequences

The library maintains its own clients and follows Massive's protocol changes itself; it never
waits on, or sends, a fix to the SDK (decision 25's rule).

## Rule

`AGENTS.md`, "Layers": all Massive access is the wire package `_massive`.
