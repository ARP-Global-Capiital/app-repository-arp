import pandas as pd
import os
import re
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from io import StringIO

from config import Config
from file_hasher import calculate_file_hash
from db_handler import get_db_connection
from schema_generator import clean_column_name
from schema_manager import add_missing_columns

logger = logging.getLogger(__name__)


def extract_date_from_filename(filename: str) -> str:
    """
    Extract date from filename: PositionDetailTimeSeries_04022026.csv → 2026-02-04

    Args:
        filename: Name of the file

    Returns:
        Date string in YYYY-MM-DD format
    """
    match = re.search(r'(\d{2})(\d{2})(\d{4})', filename)
    if match:
        day, month, year = match.groups()
        return f'{year}-{month}-{day}'
    return None


def check_file_status(filename: str, file_hash: str, conn) -> Dict:
    """
    Check if file has been processed before

    Args:
        filename: Name of the file
        file_hash: SHA256 hash of the file
        conn: Database connection

    Returns:
        Dictionary with status information
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT id, file_hash, status, rows_processed
            FROM processed_files
            WHERE filename = %s
            ORDER BY id DESC
            LIMIT 1
        """, (filename,))

        result = cursor.fetchone()
        cursor.close()

        if not result:
            return {'status': 'new', 'id': None}

        existing_id, existing_hash, existing_status, rows_processed = result

        if existing_hash == file_hash and existing_status == 'completed':
            return {
                'status': 'duplicate',
                'id': existing_id,
                'rows': rows_processed
            }
        elif existing_hash != file_hash:
            return {
                'status': 'changed',
                'id': existing_id,
                'old_hash': existing_hash
            }
        else:
            return {
                'status': 'reprocess',
                'id': existing_id
            }

    except Exception as e:
        logger.error(f"Error checking file status: {e}")
        cursor.close()
        raise


def mark_file_processing(filename: str, file_hash: str, file_size: int, file_modified_date: datetime, conn) -> int:
    """
    Mark file as being processed

    Args:
        filename: Name of the file
        file_hash: SHA256 hash
        file_size: File size in bytes
        file_modified_date: File modification timestamp
        conn: Database connection

    Returns:
        ID of the processing record
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO processed_files
            (filename, file_size, file_hash, file_modified_date, processing_started_at, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (filename, file_size, file_hash, file_modified_date, datetime.now(), 'processing'))

        record_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        return record_id

    except Exception as e:
        logger.error(f"Error marking file as processing: {e}")
        conn.rollback()
        cursor.close()
        raise


def mark_file_completed(record_id: int, rows_processed: int, columns_processed: int, new_columns: List[str], conn):
    """
    Mark file processing as completed

    Args:
        record_id: ID of the processing record
        rows_processed: Number of rows inserted
        columns_processed: Number of columns with data
        new_columns: List of newly added column names
        conn: Database connection
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE processed_files
            SET processing_completed_at = %s,
                rows_processed = %s,
                columns_processed = %s,
                new_columns_added = %s,
                status = %s,
                updated_at = %s
            WHERE id = %s
        """, (datetime.now(), rows_processed, columns_processed, json.dumps(new_columns), 'completed', datetime.now(), record_id))

        conn.commit()
        cursor.close()
        logger.info(f"Marked file as completed: {rows_processed} rows, {columns_processed} columns")

    except Exception as e:
        logger.error(f"Error marking file as completed: {e}")
        conn.rollback()
        cursor.close()
        raise


def mark_file_failed(record_id: int, error_message: str, conn):
    """
    Mark file processing as failed

    Args:
        record_id: ID of the processing record
        error_message: Error description
        conn: Database connection
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE processed_files
            SET processing_completed_at = %s,
                status = %s,
                error_message = %s,
                updated_at = %s
            WHERE id = %s
        """, (datetime.now(), 'failed', error_message, datetime.now(), record_id))

        conn.commit()
        cursor.close()

    except Exception as e:
        logger.error(f"Error marking file as failed: {e}")
        conn.rollback()
        cursor.close()


def mark_old_file_replaced(old_record_id: int, conn):
    """
    Mark old file record as replaced

    Args:
        old_record_id: ID of the old processing record
        conn: Database connection
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE processed_files
            SET status = %s,
                updated_at = %s
            WHERE id = %s
        """, ('replaced', datetime.now(), old_record_id))

        conn.commit()
        cursor.close()
        logger.info(f"Marked old record {old_record_id} as replaced")

    except Exception as e:
        logger.error(f"Error marking old file as replaced: {e}")
        conn.rollback()
        cursor.close()
        raise


def delete_old_data(filename: str, conn):
    """
    Delete old data for a file

    Args:
        filename: Source filename
        conn: Database connection
    """
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM position_detail WHERE source_file = %s", (filename,))
        deleted_count = cursor.rowcount
        conn.commit()
        cursor.close()
        logger.info(f"Deleted {deleted_count} old rows for file {filename}")

    except Exception as e:
        logger.error(f"Error deleting old data: {e}")
        conn.rollback()
        cursor.close()
        raise


def process_position_file(filepath: str):
    """
    Process a Position CSV file

    Args:
        filepath: Path to the CSV file
    """
    filename = os.path.basename(filepath)
    logger.info(f"Processing file: {filename}")

    # Calculate file hash
    try:
        file_hash = calculate_file_hash(filepath)
        file_size = os.path.getsize(filepath)
        file_modified_date = datetime.fromtimestamp(os.path.getmtime(filepath))
    except Exception as e:
        logger.error(f"Error reading file metadata: {e}")
        raise

    # Connect to database
    conn = get_db_connection()

    try:
        # Check file status
        status_info = check_file_status(filename, file_hash, conn)

        if status_info['status'] == 'duplicate':
            logger.info(f"File {filename} already processed (hash match), skipping")
            return

        # Handle file with changed content
        if status_info['status'] == 'changed':
            logger.info(f"File {filename} has changed content, deleting old data")
            delete_old_data(filename, conn)
            mark_old_file_replaced(status_info['id'], conn)

        # Mark as processing
        record_id = mark_file_processing(filename, file_hash, file_size, file_modified_date, conn)

        # Extract date from filename
        file_date = extract_date_from_filename(filename)
        if not file_date:
            raise ValueError(f"Could not extract date from filename: {filename}")

        # Read CSV and detect new columns
        logger.info(f"Reading CSV file: {filename}")
        df_sample = pd.read_csv(filepath, nrows=100)

        # Add missing columns to table
        new_columns = add_missing_columns(filepath, df_sample)

        # Process file in chunks
        total_rows = 0
        chunk_num = 0

        for chunk in pd.read_csv(filepath, chunksize=Config.CHUNK_SIZE):
            chunk_num += 1
            logger.info(f"Processing chunk {chunk_num} ({len(chunk)} rows)")

            # Clean column names
            original_columns = chunk.columns.tolist()
            cleaned_columns = [clean_column_name(col) for col in original_columns]
            chunk.columns = cleaned_columns

            # Filter out completely empty columns
            non_empty_cols = []
            for col in chunk.columns:
                if chunk[col].notna().any() and (chunk[col].astype(str).str.strip() != '').any():
                    non_empty_cols.append(col)

            chunk = chunk[non_empty_cols]

            # Add metadata columns
            chunk['source_file'] = filename
            chunk['file_date'] = file_date
            chunk['loaded_at'] = datetime.now()

            # Convert data types for known patterns
            for col in chunk.columns:
                if col in ['source_file', 'loaded_at', 'file_date']:
                    continue

                # Date columns
                if 'date' in col.lower() and col not in ['file_date']:
                    try:
                        chunk[col] = pd.to_datetime(chunk[col], errors='coerce')
                    except:
                        pass

                # Numeric columns
                elif any(keyword in col.lower() for keyword in [
                    'price', 'value', 'cost', 'exposure', 'nav', 'position',
                    'accrued', 'notional', 'beta', 'delta', 'gamma', 'rho',
                    'theta', 'vega', 'factor', 'ratio'
                ]):
                    try:
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                    except:
                        pass

            # Check for missing columns before inserting this chunk
            chunk_columns = set(chunk.columns) - {'source_file', 'file_date', 'loaded_at'}
            ensure_chunk_columns_exist(chunk_columns, chunk, conn)

            # Insert chunk
            insert_chunk(chunk, conn)
            total_rows += len(chunk)

        # Mark as completed
        mark_file_completed(record_id, total_rows, len(non_empty_cols), new_columns, conn)
        logger.info(f"Successfully processed {filename}: {total_rows} rows, {len(non_empty_cols)} columns")

    except Exception as e:
        logger.error(f"Error processing file {filename}: {e}", exc_info=True)
        if 'record_id' in locals():
            mark_file_failed(record_id, str(e), conn)
        raise
    finally:
        conn.close()


def ensure_chunk_columns_exist(chunk_columns: set, df_sample: pd.DataFrame, conn):
    """
    Ensure all columns in the chunk exist in the table

    Args:
        chunk_columns: Set of column names in the chunk
        df_sample: Sample DataFrame with data for type inference
        conn: Database connection
    """
    from schema_manager import get_existing_columns
    from schema_generator import infer_column_type

    try:
        existing_columns = get_existing_columns()
    except:
        logger.warning("Could not fetch existing columns")
        return

    missing_columns = chunk_columns - existing_columns

    if missing_columns:
        logger.info(f"Adding {len(missing_columns)} missing columns to table")
        cursor = conn.cursor()

        for col in missing_columns:
            # Infer type from the sample data
            col_type = infer_column_type(df_sample[col], col)

            try:
                alter_sql = f"ALTER TABLE position_detail ADD COLUMN IF NOT EXISTS {col} {col_type};"
                cursor.execute(alter_sql)
                logger.info(f"Added column: {col} {col_type}")
            except Exception as e:
                logger.error(f"Failed to add column {col}: {e}")
                raise

        conn.commit()
        cursor.close()


def insert_chunk(df: pd.DataFrame, conn):
    """
    Insert a chunk of data using COPY

    Args:
        df: DataFrame to insert
        conn: Database connection
    """
    cursor = conn.cursor()
    try:
        # Convert DataFrame to CSV string
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, na_rep='\\N')
        buffer.seek(0)

        # Get column names
        columns = df.columns.tolist()

        # Use COPY for fast insertion
        cursor.copy_from(buffer, 'position_detail', sep=',', columns=columns, null='\\N')
        conn.commit()
        cursor.close()

    except Exception as e:
        logger.error(f"Error inserting chunk: {e}")
        conn.rollback()
        cursor.close()
        raise


def process_existing_files():
    """
    Process all existing Position files in the data directory
    """
    import glob

    data_dir = Config.WATCH_DIR
    file_pattern = os.path.join(data_dir, Config.FILE_PATTERN)

    files = glob.glob(file_pattern)
    logger.info(f"Found {len(files)} existing Position files to process")

    for filepath in sorted(files):
        try:
            process_position_file(filepath)
        except Exception as e:
            logger.error(f"Failed to process {filepath}: {e}")
            # Continue with next file
            continue
