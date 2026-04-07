import hashlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def calculate_file_hash(filepath: str) -> str:
    """
    Calculate SHA256 hash of a file

    Args:
        filepath: Path to the file

    Returns:
        SHA256 hash as hexadecimal string
    """
    sha256_hash = hashlib.sha256()

    try:
        with open(filepath, "rb") as f:
            # Read file in chunks to handle large files efficiently
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

        file_hash = sha256_hash.hexdigest()
        logger.debug(f"Calculated hash for {filepath}: {file_hash}")
        return file_hash

    except Exception as e:
        logger.error(f"Error calculating hash for {filepath}: {e}")
        raise
