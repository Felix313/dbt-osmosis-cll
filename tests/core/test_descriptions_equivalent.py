"""Unit tests for descriptions_equivalent normalisation logic."""

from dbt_osmosis_cll.osmosis_propagation.annotations import descriptions_equivalent


# ── baseline / null handling ──────────────────────────────────────────────────


def test_both_none():
    assert descriptions_equivalent(None, None)


def test_both_empty():
    assert descriptions_equivalent("", "")


def test_one_none_one_text():
    assert not descriptions_equivalent(None, "something")
    assert not descriptions_equivalent("something", None)


def test_one_empty_one_text():
    assert not descriptions_equivalent("", "something")
    assert not descriptions_equivalent("something", "")


# ── whitespace normalisation (pre-existing behaviour) ─────────────────────────


def test_line_wrap_equivalent():
    a = "Database comment that came back as a single long line with no breaks"
    b = "Database comment that came back\nas a single long line\nwith no breaks"
    assert descriptions_equivalent(a, b)


def test_extra_spaces():
    assert descriptions_equivalent("hello   world", "hello world")


def test_leading_trailing_whitespace():
    assert descriptions_equivalent("  hello  ", "hello")


# ── case folding ──────────────────────────────────────────────────────────────


def test_case_insensitive():
    assert descriptions_equivalent("Gueltig ab Vertragsbeginn", "gueltig ab vertragsbeginn")


def test_mixed_case():
    assert descriptions_equivalent("CONTRACT_ID", "contract_id")


# ── umlaut / ASCII equivalence ────────────────────────────────────────────────


def test_ae_umlaut_equivalent():
    assert descriptions_equivalent("gueltig", "gültig")


def test_oe_umlaut_equivalent():
    assert descriptions_equivalent("groesse", "Größe")


def test_ue_umlaut_equivalent():
    assert descriptions_equivalent("uebertrag", "Übertrag")


def test_ss_sharp_s_equivalent():
    assert descriptions_equivalent("strasse", "Straße")


def test_mixed_umlaut_and_case():
    assert descriptions_equivalent(
        "Gueltig ab Vertragsabschluss",
        "gültig ab vertragsabschluss",
    )


def test_full_umlaut_sentence():
    # Simulates SALESBI source (umlauts) vs ASCII staging description
    assert descriptions_equivalent(
        "Kennzeichnet ob der Vertrag gültig ist und zur Abrechnung freigegeben wurde.",
        "Kennzeichnet ob der Vertrag gueltig ist und zur Abrechnung freigegeben wurde.",
    )


# ── genuine differences still detected ───────────────────────────────────────


def test_different_words_not_equivalent():
    assert not descriptions_equivalent("gueltig", "ungueltig")


def test_different_content_with_umlauts():
    assert not descriptions_equivalent("gültig ab Beginn", "gültig bis Ende")


def test_extra_word_not_equivalent():
    assert not descriptions_equivalent("Vertrag aktiv", "Vertrag inaktiv")
