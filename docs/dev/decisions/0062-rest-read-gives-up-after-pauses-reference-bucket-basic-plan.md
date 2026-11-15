# 62. Each read gives up on its own; reference data has its own bucket; learned Basic lives with its governor

Date: 2026-10-29

Status: partly superseded: its buckets learning a Basic plan by [114](0114-code-never-knows-massive-plans-learns-from-answers.md)

A read gives up after about 10 minutes of pauses (about 90 s under the update-graph lock) with an
error that names the cause and remedies; the bucket never gives up. Every reference endpoint is
charged to `other`, which learns Basic like any bucket. What a governor learned is forgotten with
it.
