import pandas as pd
import os
import re
import json
import glob
import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from config import Config
from file_hasher import calculate_file_hash
from schema_generator import clean_column_name

logger = logging.getLogger(__name__)


def get_sqlalchemy_engine():
    """Create SQLAlchemy engine"""
    connection_string = f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
    return create_engine(connection_string, poolclass=NullPool)


def extract_date_from_filename(filename: str) -> str:
    """Extract date from filename: PositionDetailTimeSeries_04022026.csv → 2026-02-04"""
    match = re.search(r'(\d{2})(\d{2})(\d{4})', filename)
    if match:
        day, month, year = match.groups()
        return f'{year}-{month}-{day}'
    return None


def check_file_status(filename: str, file_hash: str, engine):
    """Check if file has been processed before"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, file_hash, status, rows_processed
            FROM processed_files
            WHERE filename = :filename
            ORDER BY id DESC
            LIMIT 1
        """), {"filename": filename}).fetchone()

        if not result:
            return {'status': 'new', 'id': None}

        existing_id, existing_hash, existing_status, rows_processed = result

        if existing_hash == file_hash and existing_status == 'completed':
            return {'status': 'duplicate', 'id': existing_id, 'rows': rows_processed}
        elif existing_hash != file_hash:
            return {'status': 'changed', 'id': existing_id, 'old_hash': existing_hash}
        else:
            return {'status': 'reprocess', 'id': existing_id}


def mark_file_processing(filename: str, file_hash: str, file_size: int, file_modified_date: datetime, engine) -> int:
    """Mark file as being processed"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            INSERT INTO processed_files
            (filename, file_size, file_hash, file_modified_date, processing_started_at, status)
            VALUES (:filename, :file_size, :file_hash, :file_modified_date, :processing_started_at, :status)
            RETURNING id
        """), {
            "filename": filename,
            "file_size": file_size,
            "file_hash": file_hash,
            "file_modified_date": file_modified_date,
            "processing_started_at": datetime.now(),
            "status": 'processing'
        })
        conn.commit()
        return result.scalar()


def mark_file_completed(record_id: int, rows_processed: int, columns_processed: int, engine):
    """Mark file processing as completed"""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE processed_files
            SET processing_completed_at = :completed_at,
                rows_processed = :rows,
                columns_processed = :cols,
                status = 'completed',
                updated_at = :updated_at
            WHERE id = :id
        """), {
            "completed_at": datetime.now(),
            "rows": rows_processed,
            "cols": columns_processed,
            "updated_at": datetime.now(),
            "id": record_id
        })
        conn.commit()


def mark_file_failed(record_id: int, error_message: str, engine):
    """Mark file processing as failed"""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE processed_files
            SET processing_completed_at = :completed_at,
                status = 'failed',
                error_message = :error,
                updated_at = :updated_at
            WHERE id = :id
        """), {
            "completed_at": datetime.now(),
            "error": error_message,
            "updated_at": datetime.now(),
            "id": record_id
        })
        conn.commit()


def mark_old_file_replaced(old_record_id: int, engine):
    """Mark old file record as replaced"""
    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE processed_files
            SET status = 'replaced', updated_at = :updated_at
            WHERE id = :id
        """), {"updated_at": datetime.now(), "id": old_record_id})
        conn.commit()


def delete_old_data(filename: str, table_name: str, engine):
    """Delete old data for a file from the specified table"""
    with engine.connect() as conn:
        result = conn.execute(text(f"DELETE FROM {table_name} WHERE source_file = :filename"), {"filename": filename})
        conn.commit()
        logger.info(f"Deleted {result.rowcount} old rows for file {filename} from {table_name}")


def get_completed_files(engine) -> dict:
    """Batch-load all completed file hashes from processed_files table.
    Returns {filename: file_hash} for completed files."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT ON (filename) filename, file_hash
            FROM processed_files
            WHERE status = 'completed'
            ORDER BY filename, id DESC
        """)).fetchall()
        return {row[0]: row[1] for row in result}


def ensure_columns_exist(df, table_name: str, engine):
    """Add any columns from the DataFrame that don't exist in the table yet"""
    with engine.connect() as conn:
        # Check if table exists
        table_exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table_name
            )
        """), {"table_name": table_name}).scalar()

        if not table_exists:
            return  # to_sql will create the table

        # Get existing columns
        result = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table_name
        """), {"table_name": table_name}).fetchall()
        existing_cols = {row[0] for row in result}

        # Find missing columns
        missing_cols = [col for col in df.columns if col not in existing_cols]
        if not missing_cols:
            return

        logger.info(f"Adding {len(missing_cols)} new columns to {table_name}")
        for col in missing_cols:
            conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{col}" TEXT'))
            logger.debug(f"Added column: {col}")
        conn.commit()


def process_csv_file(filepath: str, table_name: str):
    """Process a CSV file and insert into the specified table using pandas to_sql"""
    filename = os.path.basename(filepath)
    logger.info(f"Processing file: {filename} → table: {table_name}")

    # Calculate file metadata
    try:
        file_hash = calculate_file_hash(filepath)
        file_size = os.path.getsize(filepath)
        file_modified_date = datetime.fromtimestamp(os.path.getmtime(filepath))
    except Exception as e:
        logger.error(f"Error reading file metadata: {e}")
        raise

    # Create engine
    engine = get_sqlalchemy_engine()

    try:
        # Check file status
        status_info = check_file_status(filename, file_hash, engine)

        if status_info['status'] == 'duplicate':
            logger.info(f"File {filename} already processed (hash match), skipping")
            return

        # Handle file with changed content
        if status_info['status'] == 'changed':
            logger.info(f"File {filename} has changed content, deleting old data")
            delete_old_data(filename, table_name, engine)
            mark_old_file_replaced(status_info['id'], engine)

        # Mark as processing
        record_id = mark_file_processing(filename, file_hash, file_size, file_modified_date, engine)

        # Extract date from filename
        file_date = extract_date_from_filename(filename)
        if not file_date:
            raise ValueError(f"Could not extract date from filename: {filename}")

        # Read CSV with pandas - let pandas infer types
        logger.info(f"Reading CSV file: {filename}")
        df = pd.read_csv(
            filepath,
            dtype=str,  # Read everything as string initially to avoid type issues
            na_values=['', 'NULL', 'null', 'N/A'],
            keep_default_na=True
        )

        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]

        # Add metadata columns
        df['source_file'] = filename
        df['file_date'] = file_date
        df['loaded_at'] = datetime.now()

        # Remove completely empty columns
        df = df.dropna(axis=1, how='all')

        logger.info(f"Processing {len(df)} rows with {len(df.columns)} columns")

        # Ensure all columns exist in the table before inserting
        ensure_columns_exist(df, table_name, engine)

        # Use pandas to_sql to insert data
        # if_exists='append' will automatically handle schema
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=Config.CHUNK_SIZE,
            method='multi'  # Use multi-row INSERT for better performance
        )

        # Mark as completed
        mark_file_completed(record_id, len(df), len(df.columns), engine)
        logger.info(f"Successfully processed {filename}: {len(df)} rows, {len(df.columns)} columns → {table_name}")

    except Exception as e:
        logger.error(f"Error processing file {filename}: {e}", exc_info=True)
        if 'record_id' in locals():
            mark_file_failed(record_id, str(e), engine)
        raise
    finally:
        engine.dispose()


def process_position_file(filepath: str):
    """Process a Position CSV file (backward-compatible wrapper)"""
    process_csv_file(filepath, 'position_detail')


def process_existing_files():
    """Process all existing files for all configured patterns.
    Batch-loads completed file hashes to skip already-processed files quickly."""
    engine = get_sqlalchemy_engine()
    try:
        completed = get_completed_files(engine)
        logger.info(f"Found {len(completed)} previously completed files in database")
    finally:
        engine.dispose()

    file_patterns = Config.get_file_patterns()
    for pattern, table_name in file_patterns.items():
        data_dir = Config.WATCH_DIR
        full_pattern = os.path.join(data_dir, pattern)
        files = glob.glob(full_pattern)
        logger.info(f"Found {len(files)} existing files matching '{pattern}' for table '{table_name}'")

        for filepath in sorted(files):
            filename = os.path.basename(filepath)

            # Quick skip: check hash without hitting the DB per-file
            if filename in completed:
                try:
                    file_hash = calculate_file_hash(filepath)
                    if file_hash == completed[filename]:
                        logger.debug(f"Skipping {filename} (already completed, hash match)")
                        continue
                except Exception:
                    pass  # fall through to normal processing

            try:
                process_csv_file(filepath, table_name)
            except Exception as e:
                logger.error(f"Failed to process {filepath}: {e}")
                continue
