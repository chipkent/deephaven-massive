"""Tests of the live run's secret filter and its scan (`tests/_live/secret_scrub.py`).

Planted fake secrets go through `SecretFilter.clean`, which must leave none of them in text,
and into files under a results folder, where `scan` must find each in every form `forms`
names: plain, URL-encoded, and base64 at each byte alignment in both alphabets.
"""

from __future__ import annotations

import base64
import urllib.parse
from typing import TYPE_CHECKING

import pytest

from tests._live.secret_scrub import REDACTED, SecretFilter, scan

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
