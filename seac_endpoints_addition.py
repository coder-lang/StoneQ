# ============================================================================
# SEAC ENDPOINTS — ADD TO stone_quarry_api.py
# ============================================================================
#
# HOW TO INTEGRATE:
#
# 1. Add the import block (Section A) near the top of stone_quarry_api.py,
#    alongside your other imports.
#
# 2. Add the in-memory stores (Section B) near the other global dicts
#    (extraction_results, blob_urls, etc.)
#
# 3. Paste the three endpoints + background task (Section C) anywhere after
#    the existing endpoints — order doesn't matter for FastAPI routing.
#
# ============================================================================


# ── SECTION A — IMPORTS ──────────────────────────────────────────────────────
# Add these lines near the top of stone_quarry_api.py

from functions.seac_extraction import (
    unzip_seac_pdfs,
    extract_all_seac_docs,
)


# ── SECTION B — IN-MEMORY STORES ────────────────────────────────────────────
# Add these lines alongside extraction_results, blob_urls, etc.

seac_results: Dict[str, Dict[str, Any]] = {}        # proposal_id → {filename: extracted_json}
seac_processing_status: Dict[str, str] = {}         # proposal_id → "Processing" | "Completed" | "Failed"


# ── SECTION C — ENDPOINTS + BACKGROUND TASK ─────────────────────────────────

@app.post("/seac-process")
async def seac_process(
    proposal_id: str,
    zip_file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Upload a ZIP containing one or more SEAC PDFs for a given proposal.

    Accepts the same proposal_id format as /validate (slashes allowed —
    pass as a query parameter, e.g. ?proposal_id=SIA/MIN/4353/2024).

    Steps:
      1. Saves the uploaded ZIP to disk.
      2. Kicks off background extraction (Azure DI + LLM per PDF).
      3. Returns immediately — poll /seac-status/{proposal_id} for progress.
    """
    if not zip_file.filename.endswith(".zip"):
        raise HTTPException(status_code=400, detail="File must be a ZIP archive.")

    original_proposal_id = unquote(proposal_id)
    safe_id = safe_filename(original_proposal_id)

    # ── Guard: reject if already processing ──────────────────────────────────
    if seac_processing_status.get(original_proposal_id) == "Processing":
        raise HTTPException(
            status_code=409,
            detail="SEAC processing already in progress for this proposal ID."
        )

    # ── Save ZIP ──────────────────────────────────────────────────────────────
    seac_zip_path = ZIP_UPLOAD_DIR / f"seac_{safe_id}.zip"
    with open(seac_zip_path, "wb") as f:
        f.write(await zip_file.read())
    logger.info(f"[SEAC] ZIP saved: {seac_zip_path}")

    # ── Create proposal output dir ────────────────────────────────────────────
    proposal_dir = OUTPUT_DIR / safe_id
    proposal_dir.mkdir(exist_ok=True)

    # ── Mark as processing and kick off background task ───────────────────────
    seac_processing_status[original_proposal_id] = "Processing"

    background_tasks.add_task(
        _seac_background_task,
        seac_zip_path,
        original_proposal_id,
        proposal_dir,
    )

    return {
        "proposal_id": original_proposal_id,
        "message": "SEAC processing started. Poll /seac-status/{proposal_id} for updates.",
        "status": "Processing",
    }


def _seac_background_task(
    zip_path: Path,
    original_proposal_id: str,
    proposal_dir: Path,
):
    """
    Background task: unzip → extract each SEAC PDF → store results.

    Saves extracted JSON to <proposal_dir>/seac_extracted.json for
    debugging and persistence across server restarts.
    """
    safe_id = safe_filename(original_proposal_id)

    try:
        logger.info(f"[SEAC] Background task started for {original_proposal_id}")

        # ── Step 1: Extract PDFs from ZIP ─────────────────────────────────────
        seac_extract_dir = proposal_dir / "seac_extracted"
        seac_extract_dir.mkdir(exist_ok=True)

        pdf_files, ignored_count = unzip_seac_pdfs(
            str(zip_path),
            str(seac_extract_dir),
        )

        if not pdf_files:
            logger.error(f"[SEAC] No PDFs found in ZIP for {original_proposal_id}")
            seac_processing_status[original_proposal_id] = "Failed"
            seac_results[original_proposal_id] = {
                "error": "No PDF files found inside the uploaded ZIP.",
                "ignored_files_count": ignored_count,
            }
            return

        logger.info(f"[SEAC] {len(pdf_files)} PDF(s) to process, {ignored_count} file(s) skipped")

        # ── Step 2: Extract data from every PDF ───────────────────────────────
        extracted = extract_all_seac_docs(pdf_files)

        # ── Step 3: Store in memory ────────────────────────────────────────────
        seac_results[original_proposal_id] = {
            "documents": extracted,
            "total_files": len(pdf_files),
            "ignored_files_count": ignored_count,
        }
        seac_processing_status[original_proposal_id] = "Completed"
        logger.info(f"[SEAC] Extraction complete for {original_proposal_id}")

        # ── Step 4: Persist to disk (debug / recovery) ────────────────────────
        try:
            seac_json_path = proposal_dir / "seac_extracted.json"
            with open(seac_json_path, "w", encoding="utf-8") as f:
                json.dump(seac_results[original_proposal_id], f, indent=2, ensure_ascii=False)
            logger.info(f"[SEAC] Results saved to disk: {seac_json_path}")
        except Exception as e:
            logger.warning(f"[SEAC] Could not persist results to disk: {e}")

    except Exception as e:
        logger.error(f"[SEAC] Background task failed for {original_proposal_id}: {e}", exc_info=True)
        seac_processing_status[original_proposal_id] = "Failed"
        seac_results[original_proposal_id] = {"error": str(e)}

    finally:
        # ── Cleanup ZIP ────────────────────────────────────────────────────────
        seac_zip = ZIP_UPLOAD_DIR / f"seac_{safe_id}.zip"
        if seac_zip.exists():
            os.remove(seac_zip)
            logger.info(f"[SEAC] ZIP cleaned up: {seac_zip}")


@app.get("/seac-status/{proposal_id:path}")
async def seac_status(proposal_id: str):
    """
    Poll the processing status of a SEAC extraction job.

    Mirrors the /verify-check pattern so the frontend can use
    the same polling loop it uses for the main pipeline.

    Returns:
        status  : "Processing" | "Completed" | "Failed" | "Not Found"
        message : Human-readable description
    """
    decoded_id = proposal_id  # FastAPI already handles path decoding

    status = seac_processing_status.get(decoded_id)

    if status == "Completed":
        return {
            "proposal_id": decoded_id,
            "status": "Completed",
            "message": "SEAC extraction complete. Fetch results from /seac-results/{proposal_id}",
        }
    elif status == "Processing":
        return {
            "proposal_id": decoded_id,
            "status": "Processing",
            "message": "SEAC extraction is in progress. Please check again shortly.",
        }
    elif status == "Failed":
        error_detail = seac_results.get(decoded_id, {}).get("error", "Unknown error")
        return {
            "proposal_id": decoded_id,
            "status": "Failed",
            "message": f"SEAC extraction failed: {error_detail}",
        }
    else:
        # Check if result exists on disk (server restart recovery)
        safe_id = safe_filename(decoded_id)
        seac_json_path = OUTPUT_DIR / safe_id / "seac_extracted.json"
        if seac_json_path.exists():
            return {
                "proposal_id": decoded_id,
                "status": "Completed",
                "message": "Results available (recovered from disk). Fetch from /seac-results/{proposal_id}",
            }

        return {
            "proposal_id": decoded_id,
            "status": "Not Found",
            "message": "No SEAC processing found for this proposal ID. Call /seac-process first.",
        }


@app.get("/seac-results/{proposal_id:path}")
async def seac_get_results(proposal_id: str):
    """
    Return the extracted JSON for all SEAC documents in the uploaded ZIP.

    Response shape:
    {
        "proposal_id": "SIA/MIN/4353/2024",
        "status": "Completed",
        "total_files": 2,
        "ignored_files_count": 1,
        "documents": {
            "SEAC_Meeting_12.pdf": { ...extracted fields... },
            "SEAC_Meeting_13.pdf": { ...extracted fields... }
        }
    }
    """
    decoded_id = proposal_id

    # ── Check in-memory first ─────────────────────────────────────────────────
    if decoded_id in seac_results:
        data = seac_results[decoded_id]
        if "error" in data and "documents" not in data:
            raise HTTPException(
                status_code=500,
                detail=f"SEAC extraction failed: {data['error']}"
            )
        return {
            "proposal_id": decoded_id,
            "status": seac_processing_status.get(decoded_id, "Completed"),
            **data,
        }

    # ── Fall back to disk (server restart recovery) ───────────────────────────
    safe_id = safe_filename(decoded_id)
    seac_json_path = OUTPUT_DIR / safe_id / "seac_extracted.json"
    if seac_json_path.exists():
        try:
            with open(seac_json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "proposal_id": decoded_id,
                "status": "Completed",
                **data,
            }
        except Exception as e:
            logger.error(f"[SEAC] Failed to read persisted results for {decoded_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to read persisted results: {e}")

    # ── Not found ─────────────────────────────────────────────────────────────
    status = seac_processing_status.get(decoded_id, "Not Found")
    if status == "Processing":
        raise HTTPException(
            status_code=202,
            detail="SEAC extraction still in progress. Poll /seac-status/{proposal_id}."
        )

    raise HTTPException(
        status_code=404,
        detail="No SEAC results found for this proposal ID. Call /seac-process first."
    )
