import pytest

from deephaven_massive._massive.refresh import lexicon


def test_every_shape_names_a_lexicon() -> None:
    assert set(lexicon.SHAPES.values()) <= set(lexicon.LEXICONS)


def test_every_lexicon_is_used_by_a_shape() -> None:
    assert set(lexicon.LEXICONS) == set(lexicon.SHAPES.values())


def test_lexicons_are_keyed_by_their_names() -> None:
    assert all(name == lex.name for name, lex in lexicon.LEXICONS.items())


@pytest.mark.parametrize("lex", lexicon.LEXICONS.values(), ids=lambda lex: lex.name)
def test_every_lexicon_entry_is_a_short_key_to_a_snake_case_long_name(lex: lexicon.Lexicon) -> None:
    assert lex.reason
    for short, long_name in lex.entries.items():
        assert len(short) <= 3, short
        assert "_" not in short, short
        assert long_name == long_name.lower(), long_name
        assert " " not in long_name, long_name


def test_every_identity_entry_is_short_and_has_a_reason() -> None:
    for key, reason in lexicon.IDENTITY.items():
        assert len(key) <= 3, key
        assert "_" not in key, key
        assert reason, key


def test_one_short_key_means_different_things_in_different_shapes() -> None:
    entries = {name: lex.entries for name, lex in lexicon.LEXICONS.items()}
    assert entries["aggregate"]["c"] == "close"
    assert entries["trade"]["c"] == "conditions"
    assert entries["aggregate"]["t"] == "window_start"
    assert entries["trade"]["t"] == "sip_timestamp"
    assert entries["crypto_trade"]["t"] == "timestamp"
    assert entries["quote"]["s"] == "bid_size"
    assert entries["trade"]["s"] == "size"


def test_a_lexicon_copies_and_freezes_its_entries() -> None:
    source = {"p": "price"}
    lex = lexicon.Lexicon(name="t", reason="test", entries=source)
    source["p"] = "changed"
    assert lex.entries == {"p": "price"}
    with pytest.raises(TypeError):
        lex.entries["s"] = "size"  # type: ignore[index]  # the read-only view is the point
