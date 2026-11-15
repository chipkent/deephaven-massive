# 116. A failed TLS handshake is retried unless the certificate failed

Date: 2026-11-15

Status: accepted

The REST, WebSocket, and flat-file clients share one rule for a failed TLS handshake. A
certificate failure fails at once, as the user's to fix with `SSL_CERT_FILE` (decision 30): a
certificate that is untrusted, expired, self-signed, or issued for another name will not change
between attempts. Every other TLS failure is retried on the shared schedule, as a reset or a
timeout in the same handshake is, and reported as no answer (`NETWORK`, decision 105) when the
retries run out. One function in the wire layer's errors module decides which is which for all
three clients.

A name mismatch counts as a certificate failure however the client checks the name. OpenSSL
checks it for the REST and WebSocket clients and raises `ssl.SSLCertVerificationError`; botocore
turns OpenSSL's check off, and urllib3 then checks the name itself and raises its own
`CertificateError`. Until this decision the flat-file client missed the second, retrying a name
mismatch four times and reporting it as a network failure.

Python's exception types tell a certificate failure, an EOF, and an I/O error apart from the
rest, and nothing more: a settings mismatch (TLS version, cipher, server name, a port that does
not speak TLS) and a transient alert from the server both arrive as a plain `ssl.SSLError` that
differs only in OpenSSL's reason text. Telling them apart would mean keeping a list of OpenSSL's
reason names in the library, and research found such a list hard to keep right. Retrying a
settings mismatch costs a few seconds and a network label on a message that quotes OpenSSL's
reason; failing a transient alert at once would end every live table reconnecting at that
moment. The whole Python HTTP stack, and websockets, draw the same line.

This replaces decision 39's "TLS failures never retried", which held only for the flat-file
client, and only until it adopted the REST client's rule. The owner, on 2026-11-15, asked
whether the library retried a failure that can never succeed ("If so, that is dumb"); research
found one, the flat-file name mismatch, and the owner chose this rule with its fix: "Question C:
do your recommended (a). Make sure the design is clean and obvious."
