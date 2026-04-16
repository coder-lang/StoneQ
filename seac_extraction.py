# functions/seac_extraction.py
#
# Standalone extraction module for SEAC (State Expert Appraisal Committee) documents.
# Follows the same pattern as entity_extraction.py but kept separate — do not modify
# doc_intelligence.py or entity_extraction.py.
#
# Flow:
#   1. unzip_seac_pdfs()        → extract all PDFs from uploaded ZIP
#   2. extract_seac_document()  → Azure DI → LLM extraction for a single PDF
#   3. extract_all_seac_docs()  → runs step 2 for every PDF, returns {filename: json}

import logging
import os
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

# ── Prompt directory (mirrors entity_extraction.py convention) ────────────────
PROMPT_DIR = Path(__file__).parent.parent / "prompt"


# ─────────────────────────────────────────────────────────────────────────────
# PROMPT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _default_seac_prompt() -> str:
    """
    Fallback prompt used when prompt/seac_prompt.txt is not present.
    Replace / extend this via seac_prompt.txt in the prompt/ directory.
    """
    return """You are an expert document parser for environmental clearance files in India.

Extract ALL key information from the given SEAC (State Expert Appraisal Committee) document.

Fields to extract (use null if not found):
- meeting_number          : SEAC meeting serial number
- meeting_date            : Date of the SEAC meeting (DD-MM-YYYY)
- proposal_id             : Proposal / Application reference ID
- project_name            : Full name of the project
- project_type            : Type / category of project (e.g., Mining, Quarry, Industry)
- proponent_name          : Name of the project proponent / applicant
- proponent_address       : Address of the proponent
- project_location        : Village, Taluka, District, State
- lease_area_ha           : Total lease area in hectares
- mining_area_ha          : Mining / quarrying area in hectares
- mineral_type            : Type of mineral / material
- production_capacity     : Proposed production capacity (with units)
- consultant_name         : Name of the EIA/EMP consultant firm
- nabet_accreditation     : NABET accreditation number and validity of consultant
- committee_members       : List of SEAC committee members present
- observations            : Key observations / concerns raised by the committee
- decisions               : Final decisions or recommendations of the committee
- conditions_imposed      : List of conditions or stipulations imposed
- pending_information     : Any information / documents deferred or still required
- presentation_summary    : Summary of the proponent's presentation (if available)
- additional_remarks      : Any other critical facts or remarks

Return ONLY a valid JSON object with the above keys.
Do not include any explanation, markdown fences, or text outside the JSON."""


def _load_seac_prompt() -> str:
    """Load seac_prompt.txt if it exists, else return the default prompt."""
    prompt_path = PROMPT_DIR / "seac_prompt.txt"
    if prompt_path.exists():
        try:
            with open(prompt_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
            if content:
                logger.info("Loaded seac_prompt.txt from prompt directory.")
                return content
        except Exception as e:
            logger.warning(f"Could not read seac_prompt.txt: {e}. Using default.")
    else:
        logger.info("seac_prompt.txt not found — using built-in default prompt.")
    return _default_seac_prompt()


# ─────────────────────────────────────────────────────────────────────────────
# ZIP UTILITY
# ─────────────────────────────────────────────────────────────────────────────

def unzip_seac_pdfs(zip_path: str, extract_dir: str) -> Tuple[List[str], int]:
    """
    Extract all PDF files from a ZIP archive into extract_dir.

    Unlike the main pipeline's file_filter (which matches by known doc-type
    patterns), SEAC files can have arbitrary names — so we extract every PDF.

    Returns:
        pdf_files    : list of absolute paths to extracted PDFs
        ignored_count: number of non-PDF entries skipped
    """
    pdf_files: List[str] = []
    ignored_count = 0

    extract_path = Path(extract_dir)
    extract_path.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            # Skip directories and macOS metadata noise
            if member.is_dir():
                continue
            name_lower = member.filename.lower()
            if "__macosx" in name_lower or name_lower.startswith("."):
                ignored_count += 1
                continue

            if name_lower.endswith(".pdf"):
                # Flatten nested paths — keep only the filename
                flat_name = Path(member.filename).name
                dest = extract_path / flat_name

                # Avoid name collisions by appending an index
                if dest.exists():
                    stem = dest.stem
                    suffix = dest.suffix
                    counter = 1
                    while dest.exists():
                        dest = extract_path / f"{stem}_{counter}{suffix}"
                        counter += 1

                with zf.open(member) as src, open(dest, "wb") as dst:
                    dst.write(src.read())

                pdf_files.append(str(dest))
                logger.info(f"Extracted SEAC PDF: {dest.name}")
            else:
                ignored_count += 1
                logger.debug(f"Skipped non-PDF: {member.filename}")

    logger.info(
        f"SEAC ZIP extraction complete — {len(pdf_files)} PDF(s) extracted, "
        f"{ignored_count} file(s) skipped."
    )
    return pdf_files, ignored_count


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE DOCUMENT EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_seac_document(pdf_path: str, prompt: str) -> Dict[str, Any]:
    """
    Run Azure Document Intelligence + LLM extraction on a single SEAC PDF.

    Args:
        pdf_path : absolute path to the PDF file
        prompt   : LLM extraction prompt (pass once, reuse across all docs)

    Returns:
        Extracted JSON as a Python dict.
        On failure, returns {"error": "<reason>", "file": "<filename>"}.
    """
    # ── Import here to mirror entity_extraction.py's import pattern ──────────
    from functions.doc_intelligence import extract_markdown_from_file
    from functions.entity_extraction import extract_with_size_check, safe_json_parse

    filename = Path(pdf_path).name
    logger.info(f"[SEAC] Extracting: {filename}")

    # Step 1 — Azure Document Intelligence → Markdown
    success, markdown = extract_markdown_from_file(pdf_path)
    if not success or not markdown.strip():
        logger.warning(f"[SEAC] Azure DI returned empty content for {filename}")
        return {"error": "Azure DI extraction failed or returned empty content", "file": filename}

    logger.info(f"[SEAC] {filename}: {len(markdown):,} chars extracted from Azure DI")

    # Step 2 — LLM extraction (with automatic chunking if doc is large)
    try:
        result = extract_with_size_check(prompt, markdown, "seac")
        logger.info(f"[SEAC] {filename}: LLM extraction complete")
        return result
    except Exception as e:
        logger.error(f"[SEAC] LLM extraction failed for {filename}: {e}", exc_info=True)
        return {"error": str(e), "file": filename}


# ─────────────────────────────────────────────────────────────────────────────
# BATCH EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_all_seac_docs(pdf_files: List[str]) -> Dict[str, Any]:
    """
    Extract data from every PDF in the list.

    Loads the prompt once and reuses it across all files to avoid
    repeated disk reads.

    Returns:
        {
          "SEAC_Meeting_12.pdf": { ...extracted fields... },
          "SEAC_Meeting_13.pdf": { ...extracted fields... },
          ...
        }
    """
    if not pdf_files:
        logger.warning("[SEAC] No PDF files provided for extraction.")
        return {}

    prompt = _load_seac_prompt()
    results: Dict[str, Any] = {}

    for idx, pdf_path in enumerate(pdf_files, start=1):
        filename = Path(pdf_path).name
        logger.info(f"[SEAC] Processing file {idx}/{len(pdf_files)}: {filename}")
        try:
            extracted = extract_seac_document(pdf_path, prompt)
            results[filename] = extracted
        except Exception as e:
            logger.error(f"[SEAC] Unexpected error for {filename}: {e}", exc_info=True)
            results[filename] = {"error": str(e), "file": filename}

    logger.info(f"[SEAC] Batch extraction complete — {len(results)} document(s) processed.")
    return results
