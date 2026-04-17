# ============================================================================
# PESA VILLAGE CHECK — ADD TO output_generation12.py (inside gen_delib_sheet)
# ============================================================================
#
# HOW TO INTEGRATE:
#
# 1. Find this existing block inside gen_delib_sheet():
#
#       all_jsons["ESZ_Check"] = esz_message
#       logger.info(f"ESZ Check Result: {esz_message}")
#
# 2. Paste the ENTIRE block below IMMEDIATELY AFTER those two lines.
#
# 3. Then scroll down to the deliberation_prompt string.
#    Find the ESZ line inside "Key Highlights" (point 14) and add point 15 for PESA.
#    See Section B at the bottom of this file.
#
# ============================================================================


# ── SECTION A — PASTE THIS BLOCK after `all_jsons["ESZ_Check"] = esz_message` ──

    # ─────────────────────────────────────────────────────────────────────
    # PESA VILLAGE CHECK
    # Uses fuzzy matching to handle Marathi / English mixed village names.
    # proposed_village is already extracted above during the ESZ check.
    # ─────────────────────────────────────────────────────────────────────
    PESA_VILLAGES_RAW: list = []   # keeps original names for display
    PESA_VILLAGES_NORM: list = []  # normalized versions for matching

    try:
        pesa_excel_path = "/home/eytech/ai_mh/API_Stonequarry/data/PESA_list.xlsx"
        df_pesa = pd.read_excel(pesa_excel_path, sheet_name=0)

        # ── Detect village column flexibly ──────────────────────────────
        # Tries common column names; falls back to first column if none match.
        village_col = None
        for col in df_pesa.columns:
            if col.strip().lower() in ("village", "village name", "gram", "gaon"):
                village_col = col
                break
        if village_col is None:
            village_col = df_pesa.columns[0]   # fallback: first column
            logger.warning(f"PESA: 'Village' column not found; using first column '{village_col}'")

        PESA_VILLAGES_RAW = [
            str(v).strip()
            for v in df_pesa[village_col].dropna()
            if str(v).strip()
        ]
        # Pre-normalize every entry once for fast comparison later
        PESA_VILLAGES_NORM = [
            normalize_text_for_comparison(v) for v in PESA_VILLAGES_RAW
        ]
        logger.info(f"PESA: Loaded {len(PESA_VILLAGES_RAW)} villages from Excel")

    except Exception as e:
        logger.error(f"Failed to load PESA Excel: {e}")

    # ── Match proposed_village against PESA list ─────────────────────────
    PESA_FUZZY_THRESHOLD = 0.88   # high threshold — village names are short

    pesa_message = "PESA status: Village not provided or PESA data unavailable."
    pesa_matched_village = None   # the original name from the Excel (for display)

    if proposed_village and PESA_VILLAGES_RAW:
        proposed_norm = normalize_text_for_comparison(proposed_village)

        # Pass 1 — exact match on normalized forms (handles case + script noise)
        if proposed_norm in PESA_VILLAGES_NORM:
            idx = PESA_VILLAGES_NORM.index(proposed_norm)
            pesa_matched_village = PESA_VILLAGES_RAW[idx]

        # Pass 2 — fuzzy match using jaro_winkler (handles transliteration gaps)
        if not pesa_matched_village and proposed_norm:
            best_score = 0.0
            best_idx = -1
            for i, norm_v in enumerate(PESA_VILLAGES_NORM):
                if not norm_v:
                    continue
                score = jaro_winkler(proposed_norm, norm_v)
                if score > best_score:
                    best_score = score
                    best_idx = i

            if best_score >= PESA_FUZZY_THRESHOLD:
                pesa_matched_village = PESA_VILLAGES_RAW[best_idx]
                logger.info(
                    f"PESA: Fuzzy matched '{proposed_village}' → "
                    f"'{pesa_matched_village}' (score={best_score:.3f})"
                )
            else:
                logger.info(
                    f"PESA: No match for '{proposed_village}' "
                    f"(best score={best_score:.3f} < threshold={PESA_FUZZY_THRESHOLD})"
                )

        # ── Build human-readable message ──────────────────────────────────
        if pesa_matched_village:
            pesa_message = (
                f"The proposed mine village '{proposed_village}' is listed in the "
                f"PESA (Panchayats Extension to Scheduled Areas) village list "
                f"(matched as '{pesa_matched_village}'). "
                f"Special PESA compliance and tribal consent may be required."
            )
        else:
            pesa_message = (
                f"The proposed mine village '{proposed_village}' is NOT listed in "
                f"the PESA (Panchayats Extension to Scheduled Areas) village list."
            )
    elif proposed_village and not PESA_VILLAGES_RAW:
        pesa_message = (
            f"PESA status: PESA village data could not be loaded. "
            f"Manual verification required for village '{proposed_village}'."
        )

    all_jsons["PESA_Check"] = pesa_message
    logger.info(f"PESA Check Result: {pesa_message}")

    # ── END OF PESA BLOCK ─────────────────────────────────────────────────


# ── SECTION B — UPDATE THE deliberation_prompt ──────────────────────────────
#
# Inside gen_delib_sheet, find the deliberation_prompt string.
# In the "Key Highlights" section, the last item is currently:
#
#   14. ESZ statement: Write statement like "The proposed mine village '...'
#       is not included in the List of Villages (Maharashtra) falling under
#       the Western Ghats Eco-Sensitive Zone (ESZ)..."
#
# REPLACE that line with the two lines below (14 stays, add 15):
#
# ── FIND (inside Key Highlights in deliberation_prompt) ──
#
#   14. ESZ statement: Write statement like "The proposed mine village '...' is not included in the List of Villages (Maharashtra) falling under the Western Ghats Eco-Sensitive Zone (ESZ), as per 6th Draft Notification dated 31st July 2024"
#
# ── REPLACE WITH ──
#
#   14. ESZ statement: Write statement like "The proposed mine village '...' is not included in the List of Villages (Maharashtra) falling under the Western Ghats Eco-Sensitive Zone (ESZ), as per 6th Draft Notification dated 31st July 2024"
#   15. PESA statement: {pesa_message}
#
# ── ALSO inside "Deliberation and observation of the committee" ──
# Find point 20 (currently the last item) and add point 21 after it:
#
#   21. PESA Coverage: {pesa_message}
#
# ── NOTE ──
# Both {pesa_message} references are inside an f-string already (deliberation_prompt
# uses f""" ... """), so they will be interpolated automatically. No extra quotes needed.
