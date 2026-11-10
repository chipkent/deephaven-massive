r"""Keeping the live run's secrets out of its results: the filter and the scan before packing.

The live-run program (`live_run`) reads four secrets from the environment, the API key, the
optional delayed key, and the S3 pair, and must never write one to disk. Two guards stand
between them and the results folder:

- `SecretFilter.clean`, applied to every line a child prints and every text file the program
  writes: it replaces each secret's exact value, also URL-encoded, with `***`, and the value of
  any `apiKey=` query parameter and of any `Authorization:` or `X-Amz-` header, whatever it is.
- `scan`, which `pack` runs over every file of the results folder before it writes the zip,
  Parquet files and the library's own debug logs included: it looks for every secret plain,
  URL-encoded, and base64-encoded (at each of the three byte alignments, standard and URL-safe
  alphabets), each also spelled with JSON escapes (`\u0050` for `P`, `\/` for `/`), and names
  the file and the line or byte offset of each find.

The library's own redaction replaces a secret that a reply spells with JSON escapes, so the
files should never hold one; the scan still looks, since it is the last guard before the zip
leaves the machine. It reads the escapes as JSON does, left to right, into a copy of each line
that holds a backslash, and searches that copy for every form: one pass finds a secret with
any of its characters escaped, in any case of hex digit, a surrogate pair included.

An empty or unset variable is no secret and is ignored. Every function is pure except `scan`,
which reads files.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
import urllib.parse
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from pathlib import Path

SECRET_VARIABLES: Final = (
    "MASSIVE_API_KEY",
    "MASSIVE_DELAYED_API_KEY",
    "MASSIVE_BASIC_API_KEY",
    "MASSIVE_S3_ACCESS_KEY_ID",
    "MASSIVE_S3_SECRET_ACCESS_KEY",
)
"""The environment variables whose values are secrets."""

S3_VARIABLES: Final = ("MASSIVE_S3_ACCESS_KEY_ID", "MASSIVE_S3_SECRET_ACCESS_KEY")
"""The two S3 credentials, which are set together or not at all."""

PROXY_VARIABLES: Final = ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy")
"""The proxy variables, whose URL may carry a password."""

REDACTED: Final = "***"
"""What a secret becomes in cleaned text."""

_QUERY_KEY: Final = re.compile(r"(?i)(apiKey=)[^&\s\"']+")
"""An `apiKey=` query parameter and its value."""

_HEADER: Final = re.compile(r"(?im)^(\s*(?:Authorization|X-Amz-[A-Za-z0-9-]+)\s*:\s*)\S.*$")
"""An `Authorization:` or `X-Amz-*` header line, from its value to the end of the line."""

_INLINE_HEADER: Final = re.compile(
    r"(?i)((?:'|\")(?:Authorization|X-Amz-[A-Za-z0-9-]+)(?:'|\")\s*:\s*)(?:'[^']*'|\"[^\"]*\")"
)
"""An `Authorization` or `X-Amz-*` header inside a printed mapping: `'Authorization': '...'`."""

_JSON_ESCAPE: Final = re.compile(
    rb"\\(?:u([dD][89abAB][0-9a-fA-F]{2})\\u([dD][c-fC-F][0-9a-fA-F]{2})"
    rb"|u([0-9a-fA-F]{4})|([\"\\/bfnrt]))"
)
"""One JSON escape: a surrogate pair of `u` escapes, one `u` escape, or a short escape."""

_SHORT_ESCAPES: Final = {
    b'"': b'"',
    b"\\": b"\\",
    b"/": b"/",
    b"b": b"\b",
    b"f": b"\f",
    b"n": b"\n",
    b"r": b"\r",
    b"t": b"\t",
}
"""The character each short JSON escape spells, by the letter after its backslash."""

_SURROGATE_BASE: Final = 0x10000
_HIGH_SURROGATE: Final = 0xD800
_LOW_SURROGATE: Final = 0xDC00
_SURROGATE_BITS: Final = 10


@dataclass(frozen=True)
class SecretFilter:
    """The run's secrets, and the cleaning and the forms that keep them out of the results.

    Created once per command from the environment (`from_environment`); immutable, so every
    thread may share it.

    Example:
        ::

            secrets = SecretFilter(("k3y",))
            assert secrets.clean("GET /v3?apiKey=k3y") == "GET /v3?apiKey=***"

    Attributes:
        values: The secrets' exact values, longest first, so a secret that contains another is
            replaced whole.
    """

    values: tuple[str, ...]

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] = os.environ) -> SecretFilter:
        """Read the secrets from the environment, ignoring unset and blank variables.

        Besides the four secret variables, the password of a proxy URL (`HTTPS_PROXY`,
        `HTTP_PROXY`, either case, as `http://user:password@host`) is a secret too, as written
        and percent-decoded.

        Args:
            environ: The environment; the process's own by default.

        Returns:
            The filter.
        """
        found = {environ.get(name, "").strip() for name in SECRET_VARIABLES}
        for name in PROXY_VARIABLES:
            password = urllib.parse.urlsplit(environ.get(name, "").strip()).password or ""
            found.add(urllib.parse.unquote(password))
            found.add(password)
        return cls(tuple(sorted(found - {""}, key=lambda value: (-len(value), value))))

    def clean(self, text: str) -> str:
        """Return text with every secret, `apiKey=` value, and credential header replaced.

        Args:
            text: Any text the program is about to print or write.

        Returns:
            The text with each secret's value, plain or URL-encoded, and the values of
            `apiKey=` parameters and `Authorization:` and `X-Amz-` headers, replaced by `***`.
        """
        for value in self.values:
            text = text.replace(value, REDACTED)
            encoded = urllib.parse.quote(value, safe="")
            if encoded != value:
                text = text.replace(encoded, REDACTED)
        text = _QUERY_KEY.sub(rf"\g<1>{REDACTED}", text)
        text = _HEADER.sub(rf"\g<1>{REDACTED}", text)
        return _INLINE_HEADER.sub(rf"\g<1>'{REDACTED}'", text)

    def forms(self) -> list[bytes]:
        """Return every byte string whose presence in a file betrays a secret.

        Returns:
            Each secret plain and URL-encoded, and the base64 text that encodes it inside any
            longer data, at each of the three byte alignments, in the standard and the
            URL-safe alphabets.
        """
        found: set[bytes] = set()
        for value in self.values:
            raw = value.encode()
            found.add(raw)
            found.add(urllib.parse.quote(value, safe="").encode())
            for pad in range(3):
                inner = _base64_inner(raw, pad)
                if inner:
                    found.add(inner)
                    found.add(inner.translate(bytes.maketrans(b"+/", b"-_")))
        return sorted(found)


def _base64_inner(raw: bytes, pad: int) -> bytes:
    """Return the base64 characters that depend only on a secret, at one alignment.

    Base64 turns each 3 bytes into 4 characters, so a secret inside longer data encodes
    differently by its offset modulo 3. Prefixing `pad` bytes reproduces each offset; the
    characters that also carry bits of the prefix or of what follows are dropped.

    Args:
        raw: The secret's bytes.
        pad: How many bytes precede it, 0 to 2.

    Returns:
        The characters fully determined by `raw`; empty for a secret too short to have any.
    """
    encoded = base64.b64encode(b"\0" * pad + raw)
    first = -(-8 * pad // 6)
    last = 8 * (pad + len(raw)) // 6
    return encoded[first:last]


def _spelled(match: re.Match[bytes]) -> bytes:
    """Return the UTF-8 bytes of the character one JSON escape spells.

    Args:
        match: A match of `_JSON_ESCAPE`.

    Returns:
        The character's UTF-8 bytes; a lone surrogate's bytes as `surrogatepass` writes them,
        which match no secret.
    """
    high, low, single, short = match.groups()
    if short is not None:
        return _SHORT_ESCAPES[short]
    if high is not None:
        code = (
            _SURROGATE_BASE
            + ((int(high, 16) - _HIGH_SURROGATE) << _SURROGATE_BITS)
            + (int(low, 16) - _LOW_SURROGATE)
        )
        return chr(code).encode()
    return chr(int(single, 16)).encode("utf-8", "surrogatepass")


def _escaped_finds(data: bytes, forms: Sequence[bytes]) -> set[int]:
    """Find the lines that spell a secret's form with JSON escapes.

    Args:
        data: A file's bytes.
        forms: The forms to look for (`SecretFilter.forms`).

    Returns:
        The byte offset of the start of each line whose escapes, read as JSON reads them,
        spell one of the forms.
    """
    found: set[int] = set()
    if b"\\" not in data:
        return found
    start = 0
    for line in data.split(b"\n"):
        if b"\\" in line:
            plain = _JSON_ESCAPE.sub(_spelled, line)
            if any(form in plain for form in forms):
                found.add(start)
        start += len(line) + 1
    return found


def scan(root: Path, secrets: SecretFilter, *, skip: Iterable[Path] = ()) -> list[str]:
    r"""Search every file under a folder for the secrets, in every form `forms` gives.

    Each form is searched as it is and, on every line that holds a backslash, as JSON escapes
    spell it, so a secret written as `\u0050LANTED` is found as `PLANTED` would be.

    Args:
        root: The results folder.
        secrets: The run's secrets.
        skip: Files not to read, such as earlier zips (whose members were scanned when packed)
            and the run's lock.

    Returns:
        One line per find, such as `"evidence/2026-11-03-1/debug.log: line 12"`, or `"... :
        byte offset 4096"` for a file that is not UTF-8 text (for a form spelled with escapes,
        the offset of the start of its line); one per line of text however many forms it
        holds; empty when nothing was found.
    """
    forms = secrets.forms()
    skipped = {path.resolve() for path in skip}
    finds: list[str] = []
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        if path.resolve() in skipped:
            continue
        data = path.read_bytes()
        offsets = sorted(
            {i for form in forms if (i := data.find(form)) >= 0} | _escaped_finds(data, forms)
        )
        if not offsets:
            continue
        name = path.relative_to(root).as_posix()
        try:
            data.decode("utf-8")
        except UnicodeDecodeError:
            finds.extend(f"{name}: byte offset {offset}" for offset in offsets)
        else:
            lines = sorted({data.count(b"\n", 0, offset) + 1 for offset in offsets})
            finds.extend(f"{name}: line {line}" for line in lines)
    return finds


def fingerprints(environ: Mapping[str, str], salt: str) -> dict[str, str]:
    """Return a salted HMAC of each secret variable that is set, never the value itself.

    `run` stores these with its salt, so `pack` can prove the terminal it runs in holds the
    same secrets the run used: a scan for different secrets, or none, would prove nothing.

    Args:
        environ: The environment.
        salt: A random salt, kept in the run's state.

    Returns:
        Each set secret variable's SHA-256 HMAC under the salt, in hex, by name.
    """
    found = {}
    for name in SECRET_VARIABLES:
        value = environ.get(name, "").strip()
        if value:
            found[name] = hmac.new(salt.encode(), value.encode(), hashlib.sha256).hexdigest()
    return found
