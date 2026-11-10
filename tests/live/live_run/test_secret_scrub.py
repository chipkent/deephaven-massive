"""Tests of the live run's secret filter and its scan (`tests/_live/live_run/secret_scrub.py`).

Planted fake secrets go through `SecretFilter.clean`, which must leave none of them in text,
and into files under a results folder, where `scan` must find each in every form `forms`
names: plain, URL-encoded, and base64 at each byte alignment in both alphabets, each also
spelled with JSON escapes.
"""

from __future__ import annotations

import base64
import urllib.parse
from typing import TYPE_CHECKING

import pytest

from tests._live.live_run import secret_scrub
from tests._live.live_run.secret_scrub import REDACTED, SecretFilter, scan

if TYPE_CHECKING:
    from pathlib import Path

KEY = "PLANTED-key/with+chars=1"
S3 = "PLANTEDs3secret"


def test_the_filter_reads_only_set_variables_longest_first() -> None:
    secrets = SecretFilter.from_environment(
        {
            "MASSIVE_API_KEY": f" {KEY} ",
            "MASSIVE_DELAYED_API_KEY": "  ",
            "MASSIVE_S3_SECRET_ACCESS_KEY": S3,
            "UNRELATED": "x",
        }
    )
    assert secrets.values == (KEY, S3)


def test_the_filter_hides_the_basic_key() -> None:
    secrets = SecretFilter.from_environment({"MASSIVE_BASIC_API_KEY": "PLANTED-basic-key"})
    assert secrets.clean("key=PLANTED-basic-key") == f"key={REDACTED}"


def test_clean_replaces_secrets_plain_and_url_encoded() -> None:
    secrets = SecretFilter((KEY,))
    encoded = urllib.parse.quote(KEY, safe="")
    assert secrets.clean(f"a {KEY} b {encoded} c") == f"a {REDACTED} b {REDACTED} c"


def test_clean_skips_an_encoding_equal_to_the_value() -> None:
    assert SecretFilter(("plainkey",)).clean("x plainkey") == f"x {REDACTED}"


@pytest.mark.parametrize(
    ("text", "cleaned"),
    [
        ("GET /v3?apiKey=other&x=1", f"GET /v3?apiKey={REDACTED}&x=1"),
        ("Authorization: Bearer other", f"Authorization: {REDACTED}"),
        ("  x-amz-security-token: abc", f"  x-amz-security-token: {REDACTED}"),
        ("{'Authorization': 'Bearer other'}", f"{{'Authorization': '{REDACTED}'}}"),
        ('{"X-Amz-Date": "2026"}', f"{{\"X-Amz-Date\": '{REDACTED}'}}"),
        ("nothing to hide", "nothing to hide"),
    ],
)
def test_clean_hides_credential_parameters_and_headers(text: str, cleaned: str) -> None:
    assert SecretFilter(()).clean(text) == cleaned


def test_forms_leave_out_an_alignment_with_no_character_of_the_secret_alone() -> None:
    # One byte after one byte of padding shares every base64 character with its neighbors.
    forms = SecretFilter(("a",)).forms()
    assert b"a" in forms
    assert b"" not in forms
    assert len(forms) == 3  # plain, and the alignments 0 and 2 (their URL-safe forms equal)


@pytest.mark.parametrize("pad", [0, 1, 2])
@pytest.mark.parametrize("urlsafe", [False, True])
def test_scan_finds_base64_at_every_alignment(tmp_path: Path, pad: int, urlsafe: bool) -> None:
    encode = base64.urlsafe_b64encode if urlsafe else base64.b64encode
    planted = "PLANTED?>?>value~~"
    data = encode(b"z" * pad + planted.encode() + b"tail")
    (tmp_path / "blob.txt").write_bytes(b"first line\n" + data + b"\n")
    assert scan(tmp_path, SecretFilter((planted,))) == ["blob.txt: line 2"]


def test_scan_reports_lines_offsets_and_skips_files(tmp_path: Path) -> None:
    secrets = SecretFilter((KEY,))
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "debug.log").write_text(f"ok\nok\n{urllib.parse.quote(KEY, safe='')}\n")
    (tmp_path / "rows.parquet").write_bytes(b"\xff\xfe" + KEY.encode())
    (tmp_path / "clean.txt").write_text("nothing\n")
    (tmp_path / "old.zip").write_bytes(KEY.encode())
    finds = scan(tmp_path, secrets, skip=[tmp_path / "old.zip"])
    assert finds == ["logs/debug.log: line 3", "rows.parquet: byte offset 2"]


def test_a_proxy_password_is_a_secret_as_written_and_decoded() -> None:
    environ = {"HTTPS_PROXY": "http://alice:PLANTED%2Fproxy%40pw@proxy.local:3128"}
    secrets = SecretFilter.from_environment(environ)
    assert set(secrets.values) == {"PLANTED%2Fproxy%40pw", "PLANTED/proxy@pw"}
    assert secrets.clean("via PLANTED/proxy@pw and PLANTED%2Fproxy%40pw") == "via *** and ***"
    assert SecretFilter.from_environment({"https_proxy": "http://proxy.local:3128"}).values == ()


def test_fingerprints_name_the_set_secrets_and_never_hold_a_value() -> None:
    environ = {"MASSIVE_API_KEY": " PLANTED-fp-key ", "MASSIVE_DELAYED_API_KEY": ""}
    found = secret_scrub.fingerprints(environ, "salt")
    assert list(found) == ["MASSIVE_API_KEY"]
    assert "PLANTED" not in found["MASSIVE_API_KEY"]
    assert found == secret_scrub.fingerprints({"MASSIVE_API_KEY": "PLANTED-fp-key"}, "salt")
    assert found != secret_scrub.fingerprints({"MASSIVE_API_KEY": "PLANTED-fp-key"}, "other")
    assert found != secret_scrub.fingerprints({"MASSIVE_API_KEY": "PLANTED-fp-kez"}, "salt")


def _escaped(text: str, every: int) -> str:
    """Spell every `every`-th character of text as a JSON `u` escape.

    Returns:
        The text with those characters escaped, their hex digits in upper case.
    """
    return "".join(f"\\u{ord(c):04X}" if i % every == 0 else c for i, c in enumerate(text))


@pytest.mark.parametrize("every", [1, 2, 5])
def test_scan_finds_a_secret_spelled_with_json_escapes(tmp_path: Path, every: int) -> None:
    (tmp_path / "reply.json").write_text(f'ok\n{{"error": "bad key {_escaped(KEY, every)}"}}\n')
    assert scan(tmp_path, SecretFilter((KEY,))) == ["reply.json: line 2"]


def test_scan_finds_short_escapes_and_a_surrogate_pair(tmp_path: Path) -> None:
    # The key's `/` written `\/`, as JSON encoders may; and a character beyond the BMP.
    (tmp_path / "a.json").write_text(KEY.replace("/", "\\/") + "\n")
    (tmp_path / "b.json").write_text("PLANTED\\ud83d\\ude00key\n")
    assert scan(tmp_path, SecretFilter((KEY, "PLANTED\U0001f600key"))) == [
        "a.json: line 1",
        "b.json: line 1",
    ]


def test_scan_finds_escaped_base64_and_names_binary_lines_by_offset(tmp_path: Path) -> None:
    planted = "PLANTED???value"  # `???` encodes as `Pz8/`, whose `/` JSON may escape
    escaped = base64.b64encode(planted.encode()).decode().replace("/", "\\/")
    assert "\\/" in escaped
    (tmp_path / "rows.parquet").write_bytes(b"\xff\xfe\n" + escaped.encode() + b"\n")
    assert scan(tmp_path, SecretFilter((planted,))) == ["rows.parquet: byte offset 3"]


def test_scan_reads_escapes_as_json_does(tmp_path: Path) -> None:
    # An escaped backslash before `u0050` is a backslash and the text `u0050`, not a `P`; a
    # lone surrogate spells nothing; a line without a backslash is not decoded at all.
    (tmp_path / "quoted.json").write_text("\\\\u0050LANTED-key \\ud83dPLANTED\nplain line\n")
    assert scan(tmp_path, SecretFilter(("PLANTED-key",))) == []
    (tmp_path / "plain.txt").write_text("no escapes here\n")
    assert scan(tmp_path, SecretFilter(("PLANTED-key",))) == []


def test_scan_names_a_line_once_however_many_forms_it_holds(tmp_path: Path) -> None:
    (tmp_path / "log.txt").write_text(f"plain {KEY} and escaped {_escaped(KEY, 1)}\n")
    assert scan(tmp_path, SecretFilter((KEY,))) == ["log.txt: line 1"]
