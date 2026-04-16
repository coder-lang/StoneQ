# functions/doc_intelligence.py

import logging
from typing import Tuple, Any
from doc_int import extract_markdown_from_file as azure_extract # Import the provided function

logger = logging.getLogger(__name__)

def extract_markdown_from_file(file_path: str) -> Tuple[bool, str]:
    """
    Wrapper function for the provided doc_int.py extract_markdown_from_file.
    Calls the original function and returns (success, content).
    """
    try:
        logger.info(f"Attempting to extract text from PDF: {file_path} using Azure DI")
        # The provided function returns (result_object, content_string)
        result, content = azure_extract(file_path)
        if content and len(content.strip()) > 0: # Check if content is returned
            logger.info("Azure DI extraction successful.")
            return True, content
        else:
            logger.warning("Azure DI returned empty content.")
            return False, ""
    except Exception as e:
        logger.error(f"Azure DI failed for {file_path}: {e}")
        return False, ""
