# 27. `MassiveError` carries `status` and `request_id`

Date: 2026-10-26

Status: accepted

`MassiveError` carries `status` and `request_id`, read-only and `None` when the error did not come
from an HTTP response.
