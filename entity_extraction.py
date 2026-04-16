# functions/entity_extraction.py

import json
import logging
from typing import Dict, Any, List, Tuple
from pathlib import Path
from .doc_intelligence import extract_markdown_from_file
from .chunking import recursive_chunk_markdown
from llm_service import extract_with_llm, merge_json_chunks

logger = logging.getLogger(__name__)

# Define the path to the prompt directory
PROMPT_DIR = Path(__file__).parent.parent / "prompt"

# ✅ NEW: Define which documents typically need chunking
LARGE_DOCUMENT_TYPES = ['emp', 'mpa', 'dmoss', 'qlp', 'cc', 'dsr']

# ✅ UPDATED: Much safer token limit accounting for prompt overhead
MAX_TOKENS = 100000  # Conservative limit (128K - 28K buffer for prompt/system)
CHARS_PER_TOKEN = 3.5  # More accurate estimate for technical documents
MAX_CHARS = int(MAX_TOKENS * CHARS_PER_TOKEN)  # ~350,000 characters

# Prompt overhead estimate (your prompts + system messages)
PROMPT_OVERHEAD_TOKENS = 20000  # Average prompt size
EFFECTIVE_MAX_TOKENS = MAX_TOKENS - PROMPT_OVERHEAD_TOKENS  # 80K for content

def load_prompt(filename: str) -> str:
    """Load a prompt from a text file."""
    file_path = PROMPT_DIR / filename
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except FileNotFoundError:
        logger.error(f"Prompt file not found: {file_path}")
        raise FileNotFoundError(f"Prompt file not found: {file_path}")

def safe_json_parse(output):
    """Safely parse JSON output from various formats."""
    if isinstance(output, dict):
        return output
    elif isinstance(output, str):
        try:
            fixed = output.replace("NULL", "null").replace("\n", "").replace("\r", "")
            fixed = fixed.replace("}{", "},{").replace('""', '"')
            return json.loads(fixed)
        except Exception:
            return {"raw_output": output}
    else:
        return {"raw_output": str(output)}

def estimate_tokens(text: str) -> int:
    """
    Estimate number of tokens in text.
    Uses more conservative estimate for technical documents.
    """
    return int(len(text) / CHARS_PER_TOKEN)

def needs_chunking(markdown: str, prompt: str = "") -> bool:
    """
    Check if markdown needs to be chunked.
    Accounts for prompt overhead.
    """
    markdown_tokens = estimate_tokens(markdown)
    prompt_tokens = estimate_tokens(prompt) if prompt else PROMPT_OVERHEAD_TOKENS
    total_tokens = markdown_tokens + prompt_tokens
    
    needs_chunk = total_tokens > MAX_TOKENS
    
    if needs_chunk:
        logger.warning(f"Document too large: ~{markdown_tokens:,} tokens + ~{prompt_tokens:,} prompt = {total_tokens:,} total (limit: {MAX_TOKENS:,}). Will use chunking.")
    else:
        logger.info(f"Document size OK (~{markdown_tokens:,} tokens + ~{prompt_tokens:,} prompt = {total_tokens:,} total).")
    
    return needs_chunk

def extract_with_chunking(prompt: str, markdown: str, doc_type: str) -> Dict[str, Any]:
    """
    Extract entities from large documents using chunking.
    Splits document into chunks and merges results.
    """
    logger.info(f"Using chunking for {doc_type} document")
    
    # Chunk the markdown
    chunks = recursive_chunk_markdown(str(markdown))
    logger.info(f"Split into {len(chunks)} chunks")
    
    # Process each chunk
    outputs = []
    for i, chunk in enumerate(chunks, 1):
        logger.info(f"Processing chunk {i}/{len(chunks)} (~{estimate_tokens(chunk):,} tokens)")
        try:
            output = extract_with_llm(prompt, chunk)
            outputs.append(output)
        except Exception as e:
            logger.error(f"Error processing chunk {i}: {e}")
            outputs.append({"error": str(e), "chunk_index": i})
    
    # Merge all chunk results
    merged = merge_json_chunks(outputs)
    logger.info(f"Successfully merged {len(chunks)} chunks")
    
    return merged

def extract_with_size_check(prompt: str, markdown: str, doc_type: str) -> Dict[str, Any]:
    """
    Intelligently extract based on document size.
    Uses chunking for large documents, normal extraction for small ones.
    Now accounts for prompt overhead in size calculation.
    """
    if needs_chunking(markdown, prompt):
        return extract_with_chunking(prompt, markdown, doc_type)
    else:
        output = extract_with_llm(prompt, markdown)
        return safe_json_parse(output)

# ========================================
# EXTRACTION FUNCTIONS
# ========================================

all_jsons = {}

def extract_caf(caf_file):
    """Extract CAF document data."""
    caf_result, caf_markdown = extract_markdown_from_file(caf_file)
    caf_prompt = load_prompt("caf_prompt.txt")
    
    # CAF is typically small, but check anyway
    result = extract_with_size_check(caf_prompt, caf_markdown, "caf")
    all_jsons["caf"] = result
    return {"caf": result}
    # caf_data = extract_caf(caf_file)
    # all_jsons = {"caf": caf_data["caf"]}

def extract_form1(form1_file):
    """Extract Form 1 document data."""
    form1_result, form1_markdown = extract_markdown_from_file(form1_file)
    form1_prompt = load_prompt("form1_prompt.txt")
    
    result = extract_with_size_check(form1_prompt, form1_markdown, "form1")
    return {"form1": result}

def extract_form1A(form1A_file):
    """Extract Form 1B document data."""
    form1A_result, form1A_markdown = extract_markdown_from_file(form1A_file)
    form1A_prompt = load_prompt("form1A_prompt.txt")
    
    result = extract_with_size_check(form1A_prompt, form1A_markdown, "form1A")
    return {"form1A": result}

def extract_form1B(form1B_file):
    """Extract Form 1B document data."""
    form1B_result, form1B_markdown = extract_markdown_from_file(form1B_file)
    form1B_prompt = load_prompt("form1B_prompt.txt")
    
    result = extract_with_size_check(form1B_prompt, form1B_markdown, "form1B")
    return {"form1B": result}

def extract_pfr(pfr_file):
    """Extract pfr document data."""
    pfr_result, pfr_markdown = extract_markdown_from_file(pfr_file)
    pfr_prompt = load_prompt("pfr_prompt.txt")
    
    result = extract_with_size_check(pfr_prompt, pfr_markdown, "pfr")
    return {"pfr": result}

def extract_nabet(nabet_file):
    """Extract NABET certificate data."""
    nabet_result, nabet_markdown = extract_markdown_from_file(nabet_file)
    nabet_prompt = load_prompt("nabet_prompt.txt")
    
    result = extract_with_size_check(nabet_prompt, nabet_markdown, "nabet")
    return {"nabet": result}

def extract_cluster_certificate(cc_file):
    """Extract Cluster Certificate data."""
    cc_result, cc_markdown = extract_markdown_from_file(cc_file)
    cc_prompt = load_prompt("cluster_certificate_prompt.txt")
    
    # Cluster Certificate often needs chunking
    if needs_chunking(cc_markdown):
        merged = extract_with_chunking(cc_prompt, cc_markdown, "cc")
        chunks_count = len(recursive_chunk_markdown(str(cc_markdown)))
        return {"cc": merged}, chunks_count
    else:
        chunks = recursive_chunk_markdown(str(cc_markdown))
        outputs = [extract_with_llm(cc_prompt, chunk) for chunk in chunks]
        merged = merge_json_chunks(outputs)
        return {"cc": merged}, len(chunks)

def extract_site_survey(dmoss_file):
    """Extract DMO Site Survey data."""
    ss_result, ss_markdown = extract_markdown_from_file(dmoss_file)
    ss_prompt = load_prompt("site_survey_prompt.txt")
    
    # Site Survey can be large
    result = extract_with_size_check(ss_prompt, ss_markdown, "dmoss")
    return {"ss": result}

def extract_emp(emp_file):
    """
    Extract EMP document data.
    EMP documents are typically very large (100+ pages).
    """
    logger.info("=== STARTING EMP EXTRACTION ===")
    
    emp_result, emp_markdown = extract_markdown_from_file(emp_file)
    logger.info(f"Markdown extracted: {len(emp_markdown):,} chars (~{estimate_tokens(emp_markdown):,} tokens)")
    
    emp_prompt = load_prompt("emp_prompt.txt")
    
    # EMP almost always needs chunking
    result = extract_with_size_check(emp_prompt, emp_markdown, "emp")
    
    logger.info("=== EMP EXTRACTION COMPLETE ===")
    return {"emp": result}

def extract_mpa(mpa_file):
    """
    Extract MPA document data.
    MPA documents are often large.
    """
    mpa_result, mpa_markdown = extract_markdown_from_file(mpa_file)
    logger.info(f"MPA markdown: {len(mpa_markdown):,} chars (~{estimate_tokens(mpa_markdown):,} tokens)")
    
    mpa_prompt = load_prompt("mpa_prompt.txt")
    
    # MPA often needs chunking
    result = extract_with_size_check(mpa_prompt, mpa_markdown, "mpa")
    return {"mpa": result}

def extract_nocgp(nocgp_file):
    """Extract NOC-GP (Gram Panchayat) document data."""
    nocgp_result, nocgp_markdown = extract_markdown_from_file(nocgp_file)
    nocgp_prompt = load_prompt("nocgp_prompt.txt")
    
    result = extract_with_size_check(nocgp_prompt, nocgp_markdown, "nocgp")
    return {"nocgp": result}

def extract_nocforest(nocforest_file):
    """Extract NOC from forests document data."""
    nocforest_result, nocforest_markdown = extract_markdown_from_file(nocforest_file)
    nocforest_prompt = load_prompt("nocforest_prompt.txt")
    
    result = extract_with_size_check(nocforest_prompt, nocforest_markdown, "nocforest")
    return {"nocforest": result}

def extract_nocgsda(nocgsda_file):
    """Extract NOC-GSDA document data."""
    nocgsda_result, nocgsda_markdown = extract_markdown_from_file(nocgsda_file)
    nocgsda_prompt = load_prompt("nocgsda_prompt.txt")
    
    result = extract_with_size_check(nocgsda_prompt, nocgsda_markdown, "nocgsda")
    return {"nocgsda": result}

def extract_kprat(kprat_file):
    """Extract KPRAT document data."""
    kprat_result, kprat_markdown = extract_markdown_from_file(kprat_file)
    kprat_prompt = load_prompt("kprat_prompt.txt")
    
    result = extract_with_size_check(kprat_prompt, kprat_markdown, "kprat")
    return {"kprat": result}

def extract_gsr(gsr_file):
    """Extract Geological Study Report (GSR) document data."""
    gsr_result, gsr_markdown = extract_markdown_from_file(gsr_file)
    gsr_prompt = load_prompt("gsr_prompt.txt")
    
    result = extract_with_size_check(gsr_prompt, gsr_markdown, "gsr")
    return {"gsr": result}

def extract_qlp(qlp_file):
    """
    Extract Quarry Layout Plan (qlp) document data.
    QLP can be large with many diagrams.
    """
    qlp_result, qlp_markdown = extract_markdown_from_file(qlp_file)
    logger.info(f"QLP markdown: {len(qlp_markdown):,} chars (~{estimate_tokens(qlp_markdown):,} tokens)")
    
    qlp_prompt = load_prompt("qlp_prompt.txt")
    
    # QLP often needs chunking
    result = extract_with_size_check(qlp_prompt, qlp_markdown, "qlp")
    return {"qlp": result}


def extract_od(od_file):
    """Extract Ownership Document (OD) data."""
    global all_jsons
    od_result, od_markdown = extract_markdown_from_file(od_file)
    # Get CAF proponent name
    caf_proponent = "Not Available"
    if "caf" in all_jsons and isinstance(all_jsons["caf"], dict):
        caf_proponent = (
            all_jsons["caf"].get("Project Proponent Details") or 
            all_jsons["caf"].get("Project Proponent Name") or 
            all_jsons["caf"].get("Project_Proponent_Name") or 
            "Not Available"
        )
    # Load prompt and inject CAF name
    od_prompt_template = load_prompt("od_prompt.txt")
    od_prompt = od_prompt_template.replace("{CAF_PROPONENT_NAME}", caf_proponent)
    # Extract
    od_output = extract_with_llm(od_prompt, od_markdown)
    return {"od": safe_json_parse(od_output)}


def extract_dsr(dsr_file):
    """
    Extract DSR Document (DSR) data.
    DSR documents can be large — uses size-aware chunking like emp/mpa.
    """
    logger.info("=== STARTING DSR EXTRACTION ===")

    dsr_result, dsr_markdown = extract_markdown_from_file(dsr_file)
    logger.info(f"DSR markdown: {len(dsr_markdown):,} chars (~{estimate_tokens(dsr_markdown):,} tokens)")

    # Get CAF proponent name — same as before
    caf_proponent = "Not Available"
    if "caf" in all_jsons and isinstance(all_jsons["caf"], dict):
        caf_proponent = (
            all_jsons["caf"].get("Project Proponent Details") or
            all_jsons["caf"].get("Project Proponent Name") or
            "Not Available"
        )

    # Inject CAF name into prompt — same as before
    dsr_prompt_template = load_prompt("dsr_prompt.txt")
    dsr_prompt = dsr_prompt_template.replace("{CAF_PROPONENT_NAME}", caf_proponent)

    # Use size-aware extraction instead of direct extract_with_llm
    result = extract_with_size_check(dsr_prompt, dsr_markdown, "dsr")

    logger.info("=== DSR EXTRACTION COMPLETE ===")
    return {"dsr": result}




def extract_8A(EightA_file):
    """Extract 8A Document (dsr) data."""
    EightA_result, EightA_markdown = extract_markdown_from_file(EightA_file)
    EightA_prompt = load_prompt("8A_prompt.txt")
    
    result = extract_with_size_check(EightA_prompt, EightA_markdown, "8A")
    return {"8A": result}

def extract_regrassing(regrassing_file):
    """Extract Regrassing document data."""
    regrassing_result, regrassing_markdown = extract_markdown_from_file(regrassing_file)
    regrassing_prompt = load_prompt("regrassing_prompt.txt")
    
    result = extract_with_size_check(regrassing_prompt, regrassing_markdown, "regrassing")
    return {"regrassing": result}

def extract_undertaking(undertaking_file):
    """Extract Undertaking document data."""
    undertaking_result, undertaking_markdown = extract_markdown_from_file(undertaking_file)
    undertaking_prompt = load_prompt("undertaking_prompt.txt")
    
    result = extract_with_size_check(undertaking_prompt, undertaking_markdown, "undertaking")
    return {"undertaking": result}

def extract_western_ghat(western_ghat_file):
    """Extract Western Ghats document data."""
    western_ghat_result, western_ghat_markdown = extract_markdown_from_file(western_ghat_file)
    western_ghat_prompt = load_prompt("western_ghat_prompt.txt")
    
    result = extract_with_size_check(western_ghat_prompt, western_ghat_markdown, "western_ghat")
    return {"western_ghat": result}

def extract_unproponent(unproponent_file):
    """Extract Western Ghats document data."""
    unproponent_result, unproponent_markdown = extract_markdown_from_file(unproponent_file)
    unproponent_prompt = load_prompt("un_proponent_prompt.txt")
    
    result = extract_with_size_check(unproponent_prompt, unproponent_markdown, "unproponent")
    return {"unproponent": result}

def extract_unconsultant(unconsultant_file):
    """Extract Western Ghats document data."""
    unconsultant_result, unconsultant_markdown = extract_markdown_from_file(unconsultant_file)
    unconsultant_prompt = load_prompt("un_consulatant_prompt.txt")
    
    result = extract_with_size_check(unconsultant_prompt, unconsultant_markdown, "unconsultant")
    return {"unconsultant": result}

def kml_to_json(kml_path):
    """Read KML and return JSON with coordinates in DMS format."""
    from functions.output_generation import extract_latlon_dicts
    import geopandas as gpd
    gdf = gpd.read_file(kml_path, driver="KML")
    data = []
    for _, row in gdf.iterrows():
        geom = row.geometry
        coords = extract_latlon_dicts(geom)
        name = row.get("Name") if "Name" in row else None
        data.append({
            "name": name,
            "type": geom.geom_type if geom is not None else None,
            "coordinates": coords
        })
    return data

def verify_documents(doc_a, doc_b, doc_a_name, doc_b_name):
    """Verify documents against each other."""
    from llm_service import verify_with_llm
    verify_prompt = """
    You are a document verification engine. Compare the details of Document A JSON to Document B JSON.
    Return the comparison results only for fields present in Document A, as a JSON array.
    Instructions:
    - Compare values **contextually**, not only by literal text.
    - Treat abbreviations, minor spelling differences, punctuation, case changes, and word order as **Match** if they convey the same meaning.
    (Example: "Pvt. Ltd." ≈ "Private Limited")
    - If two values clearly differ in meaning or refer to different entities, mark as **Mismatch**.
    - If a field exists in one JSON but not in the other, mark as **Missing**.
    - Include a short reason or note for each comparison.
    - Return only valid JSON array with this structure:
    [
        {
            "Field": "Consultant Name",
            "Status": "Match",
            "Doc_A_Value": "ABC Consultants",
            "Doc_B_Value": "ABC Consultants",
            "Details": "Values match exactly"
        }
    ]
    """
    verification_result = verify_with_llm(verify_prompt, doc_a, doc_b)
    parsed_result = safe_json_parse(verification_result)
    # Ensure result is a list
    if isinstance(parsed_result, dict):
        return [parsed_result]
    elif isinstance(parsed_result, list):
        return parsed_result
    else:
        return []
