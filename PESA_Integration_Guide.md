# PESA Village Check — Integration Guide

---

## What this adds

A PESA village check inside `gen_delib_sheet()` that mirrors the existing ESZ check.
Since PESA village names in the Excel can be in Marathi, English, or a mix of both,
the matching runs in two passes:

**Pass 1 — Normalized exact match**
Strips punctuation, case, common prefixes (Shri, Late, etc.) and compares.
Catches pure script differences without needing fuzzy logic.

**Pass 2 — Jaro-Winkler fuzzy match**
Runs only if Pass 1 fails. Uses the same `jaro_winkler` function already
imported in your file. Threshold is set to 0.88 — high enough to avoid
false positives on short village names.

No new imports needed. `pd`, `jaro_winkler`, `normalize_text_for_comparison`,
`logger`, and `re` are all already present in the file.

---

## STEP 1 — Open output_generation12.py

Find the `gen_delib_sheet` function.

---

## STEP 2 — Locate the anchor line

Inside `gen_delib_sheet`, find these two lines:

```python
all_jsons["ESZ_Check"] = esz_message
logger.info(f"ESZ Check Result: {esz_message}")
```

---

## STEP 3 — Paste the PESA block

Copy the entire block from **SECTION A** of `pesa_check_addition.py`
(from the `# PESA VILLAGE CHECK` comment down to `# END OF PESA BLOCK`)
and paste it **immediately after** those two lines.

After pasting, the sequence should look like:

```python
    all_jsons["ESZ_Check"] = esz_message
    logger.info(f"ESZ Check Result: {esz_message}")

    # ─────────────────────────────────────────────────────────────────────
    # PESA VILLAGE CHECK
    # ...
    all_jsons["PESA_Check"] = pesa_message
    logger.info(f"PESA Check Result: {pesa_message}")
```

---

## STEP 4 — Verify the Excel column name

Open your `PESA_list.xlsx` file and check what the village column is named.

| If column is named... | Action needed |
|---|---|
| `Village`, `village`, `Village Name` | Nothing — auto-detected |
| `Gram`, `Gaon` | Nothing — auto-detected |
| Anything else (e.g. `गाव`, `ग्राम`) | Open `pesa_check_addition.py`, find the `for col in df_pesa.columns` loop, and add your column name to the list inside the `if col.strip().lower() in (...)` check |

---

## STEP 5 — Update the deliberation prompt (Key Highlights)

Inside `gen_delib_sheet`, find the `deliberation_prompt` f-string.

Scroll to the **Key Highlights** section. The last item currently reads:

```
14. ESZ statement: Write statement like "The proposed mine village '...' is not included
    in the List of Villages (Maharashtra) falling under the Western Ghats Eco-Sensitive
    Zone (ESZ), as per 6th Draft Notification dated 31st July 2024"
```

Add point 15 directly after it:

```
14. ESZ statement: Write statement like "The proposed mine village '...' is not included in the List of Villages (Maharashtra) falling under the Western Ghats Eco-Sensitive Zone (ESZ), as per 6th Draft Notification dated 31st July 2024"
15. PESA statement: {pesa_message}
```

Also update the count in the format note. Change:
```
Format as: ["1. ...", "2. ...", "3. ..."] etc.
```
No change needed here — it's generic.

---

## STEP 6 — Update the deliberation prompt (Deliberation section)

In the same `deliberation_prompt`, find the numbered list under
**"Deliberation and observation of the committee"**. Point 20 currently reads:

```
20. Non-Coverage of Proposed Project Land in Western Ghat Ecological Sensitive Area:
    ESZ statement: Write statement like "..."
```

Add point 21 directly after it:

```
21. PESA Coverage: {pesa_message}
```

---

## STEP 7 — Adjust fuzzy threshold if needed (optional)

The threshold is set at the top of the PESA block:

```python
PESA_FUZZY_THRESHOLD = 0.88
```

| Scenario | Recommended action |
|---|---|
| Getting false positives (wrong villages matching) | Raise to `0.92` |
| Missing obvious matches (same village not detected) | Lower to `0.82` |
| Names are heavily transliterated (very different spelling) | Lower to `0.75` |

---

## What the output looks like

When village IS in PESA list:
```
The proposed mine village 'Koregaon' is listed in the PESA (Panchayats Extension
to Scheduled Areas) village list (matched as 'कोरेगाव'). Special PESA compliance
and tribal consent may be required.
```

When village is NOT in PESA list:
```
The proposed mine village 'Pune' is NOT listed in the PESA (Panchayats Extension
to Scheduled Areas) village list.
```

When Excel failed to load:
```
PESA status: PESA village data could not be loaded. Manual verification required
for village 'Koregaon'.
```

---

## Notes

- `proposed_village` is extracted during the ESZ block that runs before PESA.
  The PESA block reuses it directly — no duplication.
- The matched village name from the Excel is shown in the output message so
  reviewers can see exactly which entry was matched (useful for Marathi ↔ English cases).
- `all_jsons["PESA_Check"]` is stored the same way as `all_jsons["ESZ_Check"]`,
  so it flows into the LLM prompt and the final Deliberation Sheet automatically.
