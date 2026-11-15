# 3. No seeding

Date: 2026-10-23

Status: accepted

`last_by()` never seeds from REST and there is no `seed_since`; the recipe is
`merge([q.history(start, end), q.append_only()])`.
