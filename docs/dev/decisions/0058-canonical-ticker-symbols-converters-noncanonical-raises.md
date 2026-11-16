# 58. Canonical symbols only, with explicit converters

Date: 2026-10-28

Status: partly superseded: its failure defaults by [66](0066-bad-symbol-onerror-raise-log-ignore-default-log.md)

Every symbol in and out is Massive's exact REST ticker; a non-canonical one raises `ValueError`
naming the canonical form. `canonical_symbol`, per-class and per-convention converters, and
`canonicalize_symbols` convert explicitly. A crypto pair is split only from Massive's crypto ticker
list. *Its failure defaults are superseded by 66.*
