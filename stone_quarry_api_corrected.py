#stone_final - CORRECTED VERSION
#output_generation changed to output_generation1 and port change to 8505.
#original working is output_generation.py and port is 8504
#FIXED: Handles duplicate proposal_id by clearing previous results and overwriting
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from typing import Dict, Any, List, Tuple
import zipfile
import tempfile
import os
import uuid
import logging
import shutil
from pathlib import Path
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware 
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import json
import re
from urllib.parse import unquote
from datetime import datetime, timedelta
import io
from functions.verification import verify_documents
from docx import Document

# ✅ NEW IMPORT
from file_filter import unzip_files_combined_filtered
from functions.seac_extraction import (
    unzip_seac_pdfs,
    extract_all_seac_docs,
)

# Load environment variables
load_dotenv()
# Configure logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
# Import your modules
from functions.doc_intelligence import extract_markdown_from_file
from functions.entity_extraction import (
    extract_caf, extract_form1, extract_nabet, extract_cluster_certificate,
    extract_site_survey, extract_emp, extract_mpa, extract_nocgp,
    extract_nocforest, extract_nocgsda, extract_kprat, extract_gsr,
    extract_qlp, extract_od, kml_to_json, safe_json_parse, extract_dsr, extract_regrassing, extract_undertaking, extract_western_ghat, extract_8A, extract_pfr, extract_form1A,extract_form1B,
    extract_unproponent, extract_unconsultant
)
from functions.output_generation12 import (
    gen_delib_sheet,
    fill_word_with_mapping,
    fill_mom_from_info_and_delib,
    build_info_sheet_mapping
)

from parallel_processor import (
    ParallelDocumentProcessor,
    build_file_mapping,
    build_verification_pairs
)

app = FastAPI(title="Document Verification Engine API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://localhost:3000",
        "https://10.40.108.197",
        "http://10.40.108.197",
        "https://10.40.108.197:8504",
        "http://10.40.108.197:8504",
        "https://stonequarry.ey.net:8504",
        "https://stonequarry.ey.net", 
        "https://stonequarry.ey.net/ai/validate?",
        "*",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_origin_regex=r"https?://.*\.?10\.40\.108\.197.*"
)

# Configuration
ZIP_UPLOAD_DIR = Path(os.getenv("ZIP_UPLOAD_DIR", "./uploads"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./outputs"))
LOG_DIR = Path(os.getenv("LOG_DIR", "./logs"))
TEMPLATE_PATH = Path(os.getenv("TEMPLATE_PATH", "./templates/New Info Sheet Format_3.12.25_Final.docx"))

# Ensure directories exist
ZIP_UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Azure Blob Storage Configuration
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
container_name = "aimhproposals"

# Initialize Azure Blob Service Client
blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=account_key
)

# Define the list of REQUIRED document types for validation
REQUIRED_DOC_NAMES = [
    "CAF", "Form 1", "NABET", "Cluster Certificate", "DMO Site Survey",
    "EMP", "MPA", "NOC-GP", "NOC-GSDA", "Kprat", "Quarry Layout Plan", "Ownership Document", "DSR", "Regrassing", "Undertaking", "Affidavit Proposal", "8A", "form1A", "form1B", "pfr", "un_consultant", "un_proponent"
]

# Global store for extraction results and blob URLs
extraction_results: Dict[str, Dict[str, Any]] = {}
blob_urls: Dict[str, Dict[str, str]] = {}
verification_results: Dict[str, Dict[Tuple[str, str], Any]] = {}
missing_files_store: Dict[str, List[str]] = {}

seac_results: Dict[str, Dict[str, Any]] = {}        # proposal_id → {filename: extracted_json}
seac_processing_status: Dict[str, str] = {}         # proposal_id → "Processing" | "Completed" | "Failed"

def safe_filename(proposal_id: str) -> str:
    """
    Convert any proposal_id into a filesystem-safe filename.
    Preserves original ID but returns a sanitized version for disk use.
    Example: 'SIA/MIN/4353/2024' → 'SIA_MIN_4353_2024'
    """
    return re.sub(r'[\\/:\*\?"<>\|\s]+', '_', proposal_id.strip())


def clear_previous_results(proposal_id: str) -> None:
    """
    ✅ NEW FUNCTION: Clears all previous results for a given proposal_id.
    This enables re-running the same proposal_id and overwriting results.
    
    Clears:
    - In-memory extraction results
    - In-memory verification results
    - In-memory SEAC results and status
    - On-disk output directory for this proposal
    - Old ZIP files
    """
    safe_id = safe_filename(proposal_id)
    
    logger.info(f"🗑️  Clearing previous results for proposal_id: {proposal_id}")
    
    # Clear in-memory dictionaries
    extraction_results.pop(proposal_id, None)
    verification_results.pop(proposal_id, None)
    missing_files_store.pop(proposal_id, None)
    seac_results.pop(proposal_id, None)
    seac_processing_status.pop(proposal_id, None)
    blob_urls.pop(proposal_id, None)
    
    # Remove on-disk output directory
    proposal_dir = OUTPUT_DIR / safe_id
    if proposal_dir.exists():
        try:
            shutil.rmtree(proposal_dir)
            logger.info(f"✅ Deleted on-disk proposal directory: {proposal_dir}")
        except Exception as e:
            logger.warning(f"⚠️  Could not delete proposal directory {proposal_dir}: {e}")
    
    # Remove old ZIP files (both main and SEAC)
    old_zip = ZIP_UPLOAD_DIR / f"{safe_id}.zip"
    old_seac_zip = ZIP_UPLOAD_DIR / f"seac_{safe_id}.zip"
    
    for old_file in [old_zip, old_seac_zip]:
        if old_file.exists():
            try:
                os.remove(old_file)
                logger.info(f"✅ Deleted old ZIP: {old_file}")
            except Exception as e:
                logger.warning(f"⚠️  Could not delete {old_file}: {e}")


# --- API Endpoints ---

@app.get("/")
def root():
    return {"message": "Document Verification Engine API is running"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.post("/validate")
async def validate_documents(
    proposal_id: str,
    zip_file: UploadFile = File(...)
):
    """
    OPTIMIZED: Check whether required documents are present in the ZIP.
    Now uses parallel file extraction for faster validation.
    
    ✅ UPDATED: Clears previous results for the same proposal_id before processing.
    """
    import time
    start_time = time.time()
    
    if not zip_file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="File must be a ZIP archive.")

    original_proposal_id = unquote(proposal_id)
    safe_id = safe_filename(original_proposal_id)
    zip_path = ZIP_UPLOAD_DIR / f"{safe_id}.zip"

    try:
        # ✅ CRITICAL FIX: Clear previous results for this proposal_id
        clear_previous_results(original_proposal_id)
        
        # Save ZIP file
        with open(zip_path, 'wb') as f:
            f.write(await zip_file.read())

        # Create proposal directory
        proposal_dir = OUTPUT_DIR / safe_id
        proposal_dir.mkdir(exist_ok=True)

        # Save metadata
        metadata = {
            "original_proposal_id": original_proposal_id,
            "upload_timestamp": str(datetime.now())
        }
        with open(proposal_dir / "metadata.json", "w") as meta_file:
            json.dump(metadata, meta_file, indent=2)

        # ✅ UPDATED: Use filtered extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"Extracting ZIP for validation: {safe_id}")
            
            # NEW: Filtered extraction (only relevant files)
            pdf_files, kml_files, ignored_count = unzip_files_combined_filtered(
                str(zip_path), 
                temp_dir
            )
            
            # Log if extra files were skipped
            if ignored_count > 0:
                logger.info(f"ℹ️  Skipped {ignored_count} non-matching files during validation")
            
            # Use build_file_mapping for consistent logic
            from parallel_processor import build_file_mapping
            file_mapping = build_file_mapping(pdf_files, kml_files)
            
            # Determine missing files
            found_files = {
                "CAF": file_mapping.get('caf'),
                "Form 1": file_mapping.get('form1'),
                "NABET": file_mapping.get('nabet'),
                "Cluster Certificate": file_mapping.get('cluster'),
                "DMO Site Survey": file_mapping.get('dmoss'),
                "EMP": file_mapping.get('emp'),
                "MPA": file_mapping.get('mpa'),
                "NOC-GP": file_mapping.get('nocgp'),
                "NOC-Forest": file_mapping.get('nocforest'),
                "NOC-GSDA": file_mapping.get('nocgsda'),
                "Kprat": file_mapping.get('kprat'),
                "GSR": file_mapping.get('gsr'),
                "Quarry Layout Plan": file_mapping.get('qlp'),
                "Ownership Document": file_mapping.get('od'),
                "DSR": file_mapping.get('dsr'),
                "Affidavit Proposal": file_mapping.get('western_ghat'),
                "8A": file_mapping.get('8A'),
                "form1A":file_mapping.get("form1A"),
                "form1B":file_mapping.get("form1B"),
                "pfr":file_mapping.get("pfr"),
                "un_consultant" : file_mapping.get("un_consultant"),
                "un_proponent" : file_mapping.get("un_proponent")
            }
            
            # Find missing files
            missing_files = [doc_type for doc_type, file in found_files.items() if file is None]
            
            # Store missing files
            missing_files_store[original_proposal_id] = missing_files
            
            return {
                "proposal_id": original_proposal_id,
                "validation_status": "complete",
                "total_files_found": len([f for f in found_files.values() if f]),
                "missing_files": missing_files,
                "total_missing": len(missing_files),
                "all_required_present": len(missing_files) == 0
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Validation failed for {original_proposal_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


@app.post("/extract")
async def extract_documents(
    proposal_id: str,
    background_tasks: BackgroundTasks
):
    """
    Extract and process documents for a given proposal_id.
    
    ✅ UPDATED: Clears previous extraction results before starting new extraction.
    Handles same proposal_id by overwriting results.
    """
    original_proposal_id = unquote(proposal_id)
    safe_id = safe_filename(original_proposal_id)
    
    # ✅ CRITICAL FIX: Clear previous results
    clear_previous_results(original_proposal_id)
    
    proposal_dir = OUTPUT_DIR / safe_id
    if not proposal_dir.exists():
        raise HTTPException(
            status_code=400,
            detail="Proposal directory not found. Call /validate first."
        )

    zip_path = ZIP_UPLOAD_DIR / f"{safe_id}.zip"
    if not zip_path.exists():
        raise HTTPException(
            status_code=400,
            detail="ZIP file not found. Please upload again."
        )

    # Mark as processing
    extraction_results[original_proposal_id] = {"status": "Processing"}

    # Add background task
    background_tasks.add_task(
        _extraction_background_task,
        original_proposal_id,
        zip_path,
        proposal_dir,
    )

    return {
        "proposal_id": original_proposal_id,
        "message": "Extraction started. Poll /extract-check/{proposal_id} for status.",
        "status": "Processing"
    }


def _extraction_background_task(
    proposal_id: str,
    zip_path: Path,
    proposal_dir: Path,
):
    """
    Background task for document extraction.
    Processes all documents and stores results.
    """
    safe_id = safe_filename(proposal_id)
    
    try:
        logger.info(f"🔄 Starting extraction for {proposal_id}")
        
        # Mark as processing
        extraction_results[proposal_id] = {"status": "Processing"}
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract files
            pdf_files, kml_files, _ = unzip_files_combined_filtered(
                str(zip_path),
                temp_dir
            )
            
            file_mapping = build_file_mapping(pdf_files, kml_files)
            
            # Extract documents (pseudo-code - adapt to your actual extraction logic)
            extracted_data = {}
            
            # Process each document type
            if file_mapping.get('caf'):
                extracted_data['CAF'] = extract_caf(file_mapping['caf'])
            if file_mapping.get('form1'):
                extracted_data['Form 1'] = extract_form1(file_mapping['form1'])
            if file_mapping.get('nabet'):
                extracted_data['NABET'] = extract_nabet(file_mapping['nabet'])
            if file_mapping.get('cluster'):
                extracted_data['Cluster Certificate'] = extract_cluster_certificate(file_mapping['cluster'])
            if file_mapping.get('dmoss'):
                extracted_data['DMO Site Survey'] = extract_site_survey(file_mapping['dmoss'])
            if file_mapping.get('emp'):
                extracted_data['EMP'] = extract_emp(file_mapping['emp'])
            if file_mapping.get('mpa'):
                extracted_data['MPA'] = extract_mpa(file_mapping['mpa'])
            if file_mapping.get('nocgp'):
                extracted_data['NOC-GP'] = extract_nocgp(file_mapping['nocgp'])
            if file_mapping.get('nocforest'):
                extracted_data['NOC-Forest'] = extract_nocforest(file_mapping['nocforest'])
            if file_mapping.get('nocgsda'):
                extracted_data['NOC-GSDA'] = extract_nocgsda(file_mapping['nocgsda'])
            if file_mapping.get('kprat'):
                extracted_data['Kprat'] = extract_kprat(file_mapping['kprat'])
            if file_mapping.get('gsr'):
                extracted_data['GSR'] = extract_gsr(file_mapping['gsr'])
            if file_mapping.get('qlp'):
                extracted_data['Quarry Layout Plan'] = extract_qlp(file_mapping['qlp'])
            if file_mapping.get('od'):
                extracted_data['Ownership Document'] = extract_od(file_mapping['od'])
            if file_mapping.get('dsr'):
                extracted_data['DSR'] = extract_dsr(file_mapping['dsr'])
            if file_mapping.get('western_ghat'):
                extracted_data['Affidavit Proposal'] = extract_western_ghat(file_mapping['western_ghat'])
            if file_mapping.get('8A'):
                extracted_data['8A'] = extract_8A(file_mapping['8A'])
            if file_mapping.get('form1A'):
                extracted_data['form1A'] = extract_form1A(file_mapping['form1A'])
            if file_mapping.get('form1B'):
                extracted_data['form1B'] = extract_form1B(file_mapping['form1B'])
            if file_mapping.get('pfr'):
                extracted_data['pfr'] = extract_pfr(file_mapping['pfr'])
            if file_mapping.get('un_consultant'):
                extracted_data['un_consultant'] = extract_unconsultant(file_mapping['un_consultant'])
            if file_mapping.get('un_proponent'):
                extracted_data['un_proponent'] = extract_unproponent(file_mapping['un_proponent'])
            
            # Store results
            extraction_results[proposal_id] = {
                "status": "Completed",
                "documents": extracted_data,
                "total_files": len(pdf_files),
                "timestamp": str(datetime.now())
            }
            
            # Persist to disk
            results_json = proposal_dir / "extraction_results.json"
            with open(results_json, "w", encoding="utf-8") as f:
                json.dump(extraction_results[proposal_id], f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"✅ Extraction completed for {proposal_id}")
    
    except Exception as e:
        logger.error(f"❌ Extraction failed for {proposal_id}: {e}", exc_info=True)
        extraction_results[proposal_id] = {
            "status": "Failed",
            "error": str(e)
        }


@app.get("/extract-check/{proposal_id:path}")
async def check_extraction_status(proposal_id: str):
    """
    Check the status of document extraction.
    """
    decoded_id = unquote(proposal_id)
    
    if decoded_id not in extraction_results:
        # Check disk
        safe_id = safe_filename(decoded_id)
        results_file = OUTPUT_DIR / safe_id / "extraction_results.json"
        
        if results_file.exists():
            try:
                with open(results_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return {
                    "proposal_id": decoded_id,
                    "status": data.get("status", "Completed"),
                    "message": "Results available (recovered from disk)."
                }
            except Exception as e:
                logger.error(f"Could not read extraction results from disk: {e}")
        
        raise HTTPException(
            status_code=404,
            detail="No extraction found for this proposal_id. Call /extract first."
        )
    
    result = extraction_results[decoded_id]
    status = result.get("status", "Unknown")
    
    return {
        "proposal_id": decoded_id,
        "status": status,
        "message": f"Extraction is {status.lower()}."
    }


@app.get("/extract-results/{proposal_id:path}")
async def get_extraction_results(proposal_id: str):
    """
    Retrieve extracted document data.
    """
    decoded_id = unquote(proposal_id)
    
    if decoded_id in extraction_results:
        return {
            "proposal_id": decoded_id,
            **extraction_results[decoded_id]
        }
    
    # Fall back to disk
    safe_id = safe_filename(decoded_id)
    results_file = OUTPUT_DIR / safe_id / "extraction_results.json"
    
    if results_file.exists():
        try:
            with open(results_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {
                "proposal_id": decoded_id,
                **data
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to read results: {str(e)}")
    
    raise HTTPException(
        status_code=404,
        detail="No extraction results found. Call /extract first."
    )


@app.post("/seac-process")
async def seac_process(
    proposal_id: str,
    seac_zip_file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
):
    """
    Process SEAC documents from uploaded ZIP.
    
    ✅ UPDATED: Clears previous SEAC results before starting new processing.
    """
    original_proposal_id = unquote(proposal_id)
    safe_id = safe_filename(original_proposal_id)
    seac_zip_path = ZIP_UPLOAD_DIR / f"seac_{safe_id}.zip"

    try:
        # ✅ CRITICAL FIX: Clear previous SEAC results
        seac_results.pop(original_proposal_id, None)
        seac_processing_status.pop(original_proposal_id, None)
        
        # Save SEAC ZIP
        with open(seac_zip_path, 'wb') as f:
            f.write(await seac_zip_file.read())

        # Create proposal directory
        proposal_dir = OUTPUT_DIR / safe_id
        proposal_dir.mkdir(exist_ok=True)

        # Save metadata
        metadata = {
            "original_proposal_id": original_proposal_id,
            "upload_timestamp": str(datetime.now())
        }
        with open(proposal_dir / "metadata.json", "w") as meta_file:
            json.dump(metadata, meta_file, indent=2)

    except Exception as e:
        logger.error(f"Failed to save SEAC ZIP for {original_proposal_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save SEAC ZIP: {str(e)}")

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
        from functions import seac_extraction
        seac_extraction.CURRENT_PROPOSAL_NUMBER = original_proposal_id
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


################################################################################
################################################################################
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8504)
