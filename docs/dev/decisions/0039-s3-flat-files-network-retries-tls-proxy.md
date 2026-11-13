# 39. S3 follows the shared network rules

Date: 2026-10-26

Status: accepted

S3 follows the shared network rules: the shared CA bundle and proxy, botocore retries off in favor
of the shared schedule, TLS failures never retried.
