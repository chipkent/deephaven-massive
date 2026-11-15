# 2. Every schema is generated from the vendor's specs

Date: 2026-10-26

Status: accepted

The refresh generates `spec/rest.json`, `spec/ws.json`, and `spec/flatfiles.json` from the pinned
vendor snapshot; hand input is naming, grouping, and verification only. No schema is hand-written or
inferred from data.
