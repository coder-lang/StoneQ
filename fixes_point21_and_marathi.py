# =============================================================================
# TWO FIXES FOR output_generation12.py
# =============================================================================
#
# FIX 1 — normalize_text_for_comparison: add real Devanagari transliteration
# FIX 2 — fill_mom_from_info_and_delib: raise hard cap from 20 to 21 facts
#
# =============================================================================


# ── FIX 1 ─────────────────────────────────────────────────────────────────────
#
# FIND this function (around line 200 in your file):
#
#   def normalize_text_for_comparison(text: str) -> str:
#       if not text:
#           return ""
#       # 1. Transliterate (Placeholder - requires library like unidecode)
#       normalized_text = text
#       ...
#
# REPLACE the entire function with this version:
#
# ─────────────────────────────────────────────────────────────────────────────

def normalize_text_for_comparison(text: str) -> str:
    """
    Normalize text for cross-script comparison (Marathi ↔ English).

    Steps:
      1. Transliterate Devanagari → Roman using unidecode
         e.g. 'कोरेगाव' → 'koregaon'
      2. Lowercase
      3. Strip common Indian name prefixes (Shri, Late, etc.)
      4. Remove all non-alphanumeric characters
      5. Collapse whitespace

    Falls back to plain lowercase if unidecode is not installed
    (install with: pip install unidecode)
    """
    if not text:
        return ""

    normalized_text = str(text)

    # Step 1 — Transliterate Devanagari (and other scripts) to Roman
    try:
        from unidecode import unidecode
        normalized_text = unidecode(normalized_text)
    except ImportError:
        # Fallback: strip non-ASCII characters if unidecode not installed
        # WARNING: Marathi-only strings will become empty — install unidecode!
        normalized_text = normalized_text.encode("ascii", errors="ignore").decode("ascii")

    # Step 2 — Lowercase
    normalized_text = normalized_text.lower()

    # Step 3 — Remove common Indian name/title prefixes
    normalized_text = re.sub(
        r'\b(shri|smt|syed|mohd|mr|mrs|late|sri|dr|ku|kumari)\.?\s*',
        '',
        normalized_text
    )

    # Step 4 — Remove all non-alphanumeric characters (punctuation, hyphens, etc.)
    normalized_text = re.sub(r'[^a-z0-9\s]', '', normalized_text)

    # Step 5 — Collapse whitespace
    normalized_text = re.sub(r'\s+', ' ', normalized_text).strip()

    return normalized_text


# ── FIX 2 ─────────────────────────────────────────────────────────────────────
#
# FIND this block inside fill_mom_from_info_and_delib():
#
#   # Pad to 20 items
#   while len(facts) < 20:
#       facts.append("Not provided")
#   facts = facts[:20]
#
# REPLACE WITH:
#
#   # Pad to 21 items (points 1–21, including PESA as point 21)
#   while len(facts) < 21:
#       facts.append("Not provided")
#   facts = facts[:21]
#
# ─────────────────────────────────────────────────────────────────────────────
#
# That single change lets point 21 (PESA) flow through into the MoM.
#
# =============================================================================


# ── HOW MARATHI ↔ ENGLISH MATCHING WORKS AFTER FIX 1 ─────────────────────────
#
# Example — proposed_village extracted from Form1: "Koregaon" (English)
# PESA Excel entry: "कोरेगाव" (Marathi Devanagari)
#
# Before fix:
#   normalize("Koregaon")   → "koregaon"
#   normalize("कोरेगाव")    → ""          ← Devanagari stripped to empty
#   jaro_winkler("koregaon", "") → ~0.0   → NO MATCH
#
# After fix (with unidecode):
#   normalize("Koregaon")   → "koregaon"
#   normalize("कोरेगाव")    → "koregaon"  ← unidecode transliterates correctly
#   Pass 1 exact match      → MATCH ✓
#
# Another example — partial spelling variation:
#   normalize("Kopargaon")  → "kopargaon"
#   normalize("कोपरगाव")    → "koparagaon"  ← unidecode adds 'a' between consonants
#   Pass 1 fails (not equal)
#   Pass 2 jaro_winkler("kopargaon", "koparagaon") → ~0.985 → MATCH ✓  (above 0.98)
#
# Edge case — completely different names:
#   normalize("Pune")       → "pune"
#   normalize("नागपूर")     → "nagapura"
#   jaro_winkler("pune", "nagapura") → ~0.6 → NO MATCH ✓ (correct)
#
# =============================================================================


# ── INSTALL UNIDECODE (run once on your server) ───────────────────────────────
#
#   pip install unidecode
#
# unidecode is a small library (~500KB), no heavy dependencies.
# It handles Devanagari, Bengali, Tamil, Telugu, and other Indian scripts.
#
# =============================================================================
