# 116. A failed TLS handshake is retried, unless the certificate failed

Date: 2026-11-15

Status: accepted

## Question

Should a failed TLS handshake be retried, when some failures, such as a bad certificate, can
never succeed?

## Options

1. Never retry a TLS failure, as decision 39 said for the flat-file client.
2. Retry every TLS failure.
3. Fail a certificate failure at once and retry every other TLS failure.

## Ruling

TLS is the encryption handshake that opens every connection. A certificate failure, including a
name mismatch from a proxy or a wrong host on the user's path, fails at once as `USER`, since
only the user's environment can fix it: the error names the certificate problem, and an
untrusted authority is fixed with `SSL_CERT_FILE`. Every other TLS failure is retried like a
dropped connection and reported as `NETWORK` when the retries run out. One rule covers REST,
WebSocket, and flat files. Asked on 2026-11-15 whether to fix the name-mismatch bug and keep
this rule on all three transports, the owner chose the recommendation and asked for a clean,
obvious design; recorded in PR #673. This replaces decision 39's TLS rule.

## Reason

A certificate does not change between attempts, and other TLS failures cannot be told apart
without a list of OpenSSL reasons.

## Consequences

A settings mismatch costs a few seconds of retries before it fails, labeled a network failure
with OpenSSL's reason in the message.

## Rule

`docs/troubleshooting.md`, Setup, and the wire layer's errors module docstring.
