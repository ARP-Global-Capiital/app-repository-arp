import pandas as pd
import re
import logging
from typing import Dict, List, Tuple
from db_handler import get_db_connection

logger = logging.getLogger(__name__)

# PostgreSQL reserved words that need special handling
POSTGRES_RESERVED_WORDS = {
    'all', 'analyse', 'analyze', 'and', 'any', 'array', 'as', 'asc', 'asymmetric',
    'both', 'case', 'cast', 'check', 'collate', 'column', 'constraint', 'create',
    'current_catalog', 'current_date', 'current_role', 'current_time', 'current_timestamp',
    'current_user', 'default', 'deferrable', 'desc', 'distinct', 'do', 'else', 'end',
    'except', 'false', 'fetch', 'for', 'foreign', 'from', 'grant', 'group', 'having',
    'in', 'initially', 'intersect', 'into', 'lateral', 'leading', 'limit', 'localtime',
    'localtimestamp', 'not', 'null', 'offset', 'on', 'only', 'or', 'order', 'placing',
    'primary', 'references', 'returning', 'select', 'session_user', 'some', 'symmetric',
    'table', 'then', 'to', 'trailing', 'true', 'union', 'unique', 'user', 'using',
    'variadic', 'when', 'where', 'window', 'with'
}


def clean_column_name(col: str) -> str:
    """
    Convert CSV column name to PostgreSQL-friendly format

    Examples:
        'P:P:Position ID' → 'position_id'
        'P:P:L/S' → 'long_short'
        'P:S:Delta Changed Date' → 'ps_delta_changed_date'

    Args:
        col: Original column name from CSV

    Returns:
        Cleaned column name
    """
    # Store original for logging
    original = col

    # Remove prefixes but keep track of them
    if col.startswith('P:P:'):
        col = col[4:]
    elif col.startswith('P:S:'):
        # Keep P:S: distinction by adding ps_ prefix
        col = 'ps_' + col[4:]
    elif col.startswith('S:'):
        # Keep S: distinction by adding s_ prefix
        col = 's_' + col[2:]

    # Convert to snake_case
    col = col.strip()
    col = col.replace(' ', '_')
    col = col.replace('/', '_')
    col = col.replace('%', '_percent')
    col = col.replace('&', '_and_')
    col = col.replace('#', 'num_')
    col = col.replace('$', 'dollar_')
    col = re.sub(r'[^\w_]', '', col)  # Remove special chars
    col = re.sub(r'_+', '_', col)      # Collapse multiple underscores
    col = col.strip('_')               # Remove leading/trailing underscores
    col = col.lower()

    # Handle PostgreSQL reserved words
    if col in POSTGRES_RESERVED_WORDS:
        col = f'{col}_value'

    # Truncate to 63 characters (PostgreSQL limit)
    if len(col) > 63:
        col = col[:60] + '_' + str(abs(hash(original)) % 100).zfill(2)

    return col


def infer_column_type(series: pd.Series, col_name: str) -> str:
    """
    Infer PostgreSQL column type from pandas Series
    ULTRA CONSERVATIVE: Only DATE for date columns, TEXT for everything else
    This avoids all type inference issues with mixed data

    Args:
        series: Pandas series with sample data
        col_name: Column name for context

    Returns:
        PostgreSQL column type
    """
    # Drop null values for type inference
    non_null = series.dropna()

    if len(non_null) == 0:
        return 'TEXT'  # Default for empty columns

    col_lower = col_name.lower()

    # Check for date columns (only thing we try to detect)
    if 'date' in col_lower:
        try:
            pd.to_datetime(non_null.iloc[:100], errors='raise')
            return 'DATE'
        except:
            return 'TEXT'

    # Everything else is TEXT (safest approach - avoids all type mismatches)
    # PostgreSQL can handle text data and users can CAST in queries if needed
    return 'TEXT'


def analyze_csv_columns(csv_path: str, sample_size: int = 100) -> Tuple[List[str], Dict[str, str]]:
    """
    Analyze CSV file to determine columns with data and their types

    Args:
        csv_path: Path to CSV file
        sample_size: Number of rows to sample for type inference

    Returns:
        Tuple of (column names list, column types dict)
    """
    logger.info(f"Analyzing CSV structure: {csv_path}")

    # Read first chunk to analyze structure
    df_sample = pd.read_csv(csv_path, nrows=sample_size)

    logger.info(f"CSV has {len(df_sample.columns)} total columns")

    # Identify columns with data (not completely empty)
    columns_with_data = []
    column_types = {}

    for col in df_sample.columns:
        # Check if column has any non-null, non-empty values
        non_empty = df_sample[col].dropna()
        non_empty = non_empty[non_empty.astype(str).str.strip() != '']

        if len(non_empty) > 0:
            cleaned_name = clean_column_name(col)
            columns_with_data.append(cleaned_name)

            # Infer column type
            col_type = infer_column_type(df_sample[col], cleaned_name)
            column_types[cleaned_name] = col_type

            logger.debug(f"Column '{col}' -> '{cleaned_name}' ({col_type})")
        else:
            logger.debug(f"Column '{col}' is empty, skipping")

    logger.info(f"Found {len(columns_with_data)} columns with data")

    return columns_with_data, column_types


def create_position_table(csv_path: str):
    """
    Create position_detail table based on CSV structure

    Args:
        csv_path: Path to first CSV file to analyze
    """
    logger.info("Creating position_detail table...")

    # Analyze CSV structure
    columns, column_types = analyze_csv_columns(csv_path)

    # Build CREATE TABLE statement
    create_table_sql = "CREATE TABLE IF NOT EXISTS position_detail (\n"
    create_table_sql += "    id BIGSERIAL PRIMARY KEY,\n"
    create_table_sql += "    source_file VARCHAR(255) NOT NULL,\n"
    create_table_sql += "    file_date DATE NOT NULL,\n"
    create_table_sql += "    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"

    # Add data columns
    for col in columns:
        col_type = column_types[col]
        create_table_sql += f"    {col} {col_type},\n"

    # Remove trailing comma and close
    create_table_sql = create_table_sql.rstrip(',\n') + "\n);\n"

    # Add indexes
    create_table_sql += """
    CREATE INDEX IF NOT EXISTS idx_position_file_date ON position_detail(file_date);
    CREATE INDEX IF NOT EXISTS idx_position_source_file ON position_detail(source_file);
    """

    # Try to create indexes on common columns if they exist
    if 'position_date' in columns:
        create_table_sql += "CREATE INDEX IF NOT EXISTS idx_position_date ON position_detail(position_date);\n"
    if 'fund_code' in columns:
        create_table_sql += "CREATE INDEX IF NOT EXISTS idx_fund_code ON position_detail(fund_code);\n"
    if 'security_symbol' in columns:
        create_table_sql += "CREATE INDEX IF NOT EXISTS idx_security_symbol ON position_detail(security_symbol);\n"

    # Execute table creation
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        logger.info(f"position_detail table created with {len(columns)} columns")
        cursor.close()
    except Exception as e:
        logger.error(f"Failed to create position_detail table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
