# 40. REST rate limits and host resolution are shared

Date: 2026-10-26

Status: accepted

REST rate limits and host resolution are shared: one `RequestGovernor` per API key and REST origin
across the process's sessions, and one in-flight host decision per (class, family).
