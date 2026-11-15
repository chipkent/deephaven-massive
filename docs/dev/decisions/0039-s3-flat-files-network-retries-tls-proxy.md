# 39. S3 follows the shared network rules

Date: 2026-10-26

Status: partly superseded: its "TLS failures never retried" by [116](0116-tls-handshake-retried-unless-certificate-failure.md)

S3 follows the shared network rules: the shared CA bundle and proxy, botocore retries off in favor
of the shared schedule, TLS failures never retried.
