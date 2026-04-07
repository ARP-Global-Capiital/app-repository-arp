import logging
from typing import List, Dict, Set
from db_handler import get_db_connection
from schema_generator import clean_column_name, infer_column_type
import pandas as pd

logger = logging.getLogger(__name__)


def get_existing_columns(table_name: str = 'position_detail') -> Set[str]:
    """
    Get list of existing columns in the table

    Args:
        table_name: Name of the table

    Returns:
        Set of column names
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            AND table_schema = 'public'
        """, (table_name,))

        columns = {row[0] for row in cursor.fetchall()}
        cursor.close()
        logger.debug(f"Found {len(columns)} existing columns in {table_name}")
        return columns

    except Exception as e:
        logger.error(f"Failed to get existing columns: {e}")
        raise
    finally:
        conn.close()


def add_missing_columns(csv_path: str, df_sample: pd.DataFrame) -> List[str]:
    """
    Detect new columns in CSV and add them to the table

    Args:
        csv_path: Path to CSV file (for logging)
        df_sample: Sample DataFrame with current CSV data

    Returns:
        List of newly added column names
    """
    logger.info(f"Checking for new columns in {csv_path}")

    # Get existing columns
    try:
        existing_columns = get_existing_columns()
    except:
        # Table might not exist yet
        logger.warning("Could not fetch existing columns, table may not exist yet")
        return []

    # Analyze CSV columns
    new_columns = []
    column_types = {}

    for col in df_sample.columns:
        # Check if column has any non-null, non-empty values
        non_empty = df_sample[col].dropna()
        non_empty = non_empty[non_empty.astype(str).str.strip() != '']

        if len(non_empty) > 0:
            cleaned_name = clean_column_name(col)

            # Check if column is new
            if cleaned_name not in existing_columns:
                col_type = infer_column_type(df_sample[col], cleaned_name)
                new_columns.append(cleaned_name)
                column_types[cleaned_name] = col_type
                logger.info(f"New column detected: {cleaned_name} ({col_type})")

    # Add new columns to table
    if new_columns:
        logger.info(f"Adding {len(new_columns)} new columns to position_detail table")
        conn = get_db_connection()
        try:
            cursor = conn.cursor()

            for col in new_columns:
                col_type = column_types[col]
                alter_sql = f"ALTER TABLE position_detail ADD COLUMN IF NOT EXISTS {col} {col_type};"

                try:
                    cursor.execute(alter_sql)
                    logger.info(f"Added column: {col} {col_type}")
                except Exception as e:
                    logger.error(f"Failed to add column {col}: {e}")
                    raise

            conn.commit()
            cursor.close()
            logger.info(f"Successfully added {len(new_columns)} new columns")

        except Exception as e:
            logger.error(f"Failed to add columns: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    else:
        logger.debug("No new columns detected")

    return new_columns
