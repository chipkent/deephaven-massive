"""The build-time refresh of the wire schemas (a marker for now, with no contents).

It is the home of `python -m deephaven_massive._massive.refresh`, which regenerates
`spec/*.json` from a pinned snapshot of Massive's published specifications. It runs from a
source checkout only and is not shipped in the wheel. It may import any part of `_massive`, and
nothing but itself and its own tests imports it.
"""
