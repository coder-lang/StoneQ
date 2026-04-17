#llm_service.py
import openai
from typing import Any, Dict
from langchain_text_splitters import RecursiveCharacterTextSplitter
import os
from dotenv import load_dotenv
import json
import copy
import re
import time  # ✅ NEW: Add time import
import logging

load_dotenv() 

# Configure OpenAI
openai.api_type = "azure"
openai.api_version = "2023-07-01-preview"
openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
openai.api_key = os.getenv("AZURE_OPENAI_API_KEY")

# Replace with your deployment name in Azure
AZURE_OPENAI_DEPLOYMENT = "gpt-4o"

logger = logging.getLogger(__name__)

def safe_json_parse(llm_output: str) -> dict:
    """
    Safely parse LLM output into JSON, handling extra text, markdown, or invisible characters.
    """
    if not llm_output or llm_output.strip() == "":
        return {}

    # Remove markdown code block markers (```json ... ``` or ```)
    llm_output = re.sub(r"```.*?\n", "", llm_output)
    llm_output = llm_output.replace("```", "")

    # Strip leading/trailing whitespace
    llm_output = llm_output.strip()

    # Try direct JSON parse
    try:
        return json.loads(llm_output)
    except json.JSONDecodeError:
        # Extract {...} block from string
        match = re.search(r"\{.*\}", llm_output, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                return {}
        return {}

# ✅ NEW: Helper function to detect rate limit errors
def is_rate_limit_error(error: Exception) -> bool:
    """Check if the error is a rate limit error."""
    error_msg = str(error).lower()
    return 'rate' in error_msg and 'limit' in error_msg

# ✅ NEW: Extract wait time from error message
def extract_wait_time(error: Exception) -> int:
    """Extract the suggested wait time from Azure error message."""
    error_msg = str(error)
    match = re.search(r'retry after (\d+)', error_msg, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 5  # Default wait time

def extract_with_llm(user_prompt: str, context: str) -> Dict[str, Any]:
    """
    ✅ UPDATED: Ask the LLM to extract parameter values with retry logic.
    """
    system_prompt = """
    You are an intelligent document analysis assistant. 
    Extract only the requested parameters and return strict JSON only, starting with '{' and ending with '}'. 
    Do not include any explanations or text outside JSON.
    If a value is missing, return null. Give whole total after calculation, dont do 23+36474+26464+376464 anywhere always add and give total whenver asked.
    """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Context:\n{context}\n\nInstruction:\n{user_prompt}"}
    ]

    max_retries = 3
    base_delay = 5

    for attempt in range(max_retries):
        try:
            response = openai.ChatCompletion.create(
                engine=AZURE_OPENAI_DEPLOYMENT,
                messages=messages,
                temperature=0
            )
            content = response.choices[0].message["content"]
            return safe_json_parse(content)

        except Exception as e:
            if is_rate_limit_error(e):
                if attempt < max_retries - 1:
                    wait_time = extract_wait_time(e) + 1  # Add 1s buffer
                    logger.warning(f"⏳ Rate limit in extract_with_llm. Retry {attempt + 1}/{max_retries} after {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Max retries reached in extract_with_llm")
                    print(f"Error: {e}")
                    return {}
            else:
                # Not a rate limit error, don't retry
                print(f"Error: {e}")
                return {}
    
    return {}

def verify_with_llm(user_prompt: str, evidence_json: dict, base_truth_json: dict) -> Dict[str, Any]:
    """
    ✅ UPDATED: Compare two JSONs with retry logic for rate limits.
    """
    system_prompt = """
    You are an intelligent JSON verification assistant.
    You strictly compare JSON fields as instructed.
    Always return valid JSON only (no explanations, no extra text).
    """

    formatted_prompt = (
        f"{user_prompt}\n\n"
        f"Base Truth JSON:\n{json.dumps(base_truth_json, indent=2)}\n\n"
        f"Evidence JSON:\n{json.dumps(evidence_json, indent=2)}"
    )

    messages = [
        {"role": "system", "content": system_prompt.strip()},
        {"role": "user", "content": formatted_prompt.strip()}
    ]

    max_retries = 3
    base_delay = 5

    for attempt in range(max_retries):
        try:
            response = openai.ChatCompletion.create(
                engine="gpt-4o",
                messages=messages,
                temperature=0
            )
            content = response.choices[0].message["content"]
            return safe_json_parse(content)

        except Exception as e:
            if is_rate_limit_error(e):
                if attempt < max_retries - 1:
                    wait_time = extract_wait_time(e) + 1  # Add 1s buffer
                    logger.warning(f"⏳ Rate limit in verify_with_llm. Retry {attempt + 1}/{max_retries} after {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Max retries reached in verify_with_llm")
                    print(f"Error in verify_with_llm: {e}")
                    return {"error": str(e)}
            else:
                # Not a rate limit error, don't retry
                print(f"Error in verify_with_llm: {e}")
                return {"error": str(e)}
    
    return {"error": "Max retries exceeded"}

def recursive_chunk_markdown(markdown_text):        
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=150000,
        chunk_overlap=200,
        separators=["\n## ", "\n### ", "\n\n", "\n", " ", ""]
    )
    chunks = splitter.split_text(markdown_text)
    return chunks

def merge_json_chunks(chunks):
    """
    Dynamically merges multiple JSON-like dict chunks based on rules:
      - Lists are merged with deduplication (safe for dicts).
      - Dicts are merged recursively.
      - Non-null values override None values.
    """

    def deep_merge(a, b):
        merged = copy.deepcopy(a)

        for key, value in b.items():
            if key not in merged:
                merged[key] = value
            else:
                # Merge nested dictionaries
                if isinstance(merged[key], dict) and isinstance(value, dict):
                    merged[key] = deep_merge(merged[key], value)

                # Merge lists (handle dicts safely)
                elif isinstance(merged[key], list) and isinstance(value, list):
                    merged_list = merged[key] + value

                    # Deduplicate intelligently
                    unique = []
                    seen = set()
                    for item in merged_list:
                        # Convert hashable types to tuple for comparison
                        if isinstance(item, dict):
                            item_key = json.dumps(item, sort_keys=True)
                        else:
                            item_key = str(item)

                        if item_key not in seen:
                            seen.add(item_key)
                            unique.append(item)
                    merged[key] = unique

                # Prefer non-null values
                elif merged[key] is None and value is not None:
                    merged[key] = value

                # Keep latest non-null value
                elif value is not None:
                    merged[key] = value

        return merged

    # Start merging sequentially
    result = {}
    for chunk in chunks:
        result = deep_merge(result, chunk)

    return result
