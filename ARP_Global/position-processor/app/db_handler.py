import psycopg2
import psycopg2.extras
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import Config

logger = logging.getLogger(__name__)

class DatabaseHandler:
    """Handle database connections and operations"""

    def __init__(self):
        self.connection = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError))
    )
    def connect(self, database=None):
        """
        Connect to PostgreSQL database

        Args:
            database: Database name (defaults to Config.DB_NAME)
        """
        db_name = database or Config.DB_NAME

        try:
            self.connection = psycopg2.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                database=db_name,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD
            )
            self.connection.autocommit = False
            logger.info(f"Connected to database: {db_name}")
            return self.connection

        except Exception as e:
            logger.error(f"Failed to connect to database {db_name}: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

    def execute(self, query, params=None, fetch=False):
        """
        Execute a SQL query

        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results

        Returns:
            Query results if fetch=True, else None
        """
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            return None
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            cursor.close()

    def commit(self):
        """Commit current transaction"""
        self.connection.commit()

    def rollback(self):
        """Rollback current transaction"""
        self.connection.rollback()


def ensure_database_exists():
    """
    Create 'global' database if it doesn't exist
    """
    logger.info("Checking if 'global' database exists...")

    # Connect to default 'postgres' database
    handler = DatabaseHandler()
    handler.connect(database='postgres')

    try:
        # Enable autocommit for CREATE DATABASE
        handler.connection.autocommit = True

        # Check if database exists
        result = handler.execute(
            "SELECT 1 FROM pg_database WHERE datname='global'",
            fetch=True
        )

        if not result:
            logger.info("Creating 'global' database...")
            handler.execute("CREATE DATABASE global")
            logger.info("Database 'global' created successfully")
        else:
            logger.info("Database 'global' already exists")

    finally:
        handler.close()


def ensure_tables_exist():
    """
    Create processed_files tracking table if it doesn't exist
    """
    logger.info("Ensuring processed_files table exists...")

    handler = DatabaseHandler()
    handler.connect()

    try:
        # Create processed_files tracking table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS processed_files (
            id SERIAL PRIMARY KEY,
            filename VARCHAR(255) NOT NULL,
            file_size BIGINT NOT NULL,
            file_hash VARCHAR(64) NOT NULL,
            file_modified_date TIMESTAMP NOT NULL,
            processing_started_at TIMESTAMP NOT NULL,
            processing_completed_at TIMESTAMP,
            rows_processed INTEGER,
            columns_processed INTEGER,
            new_columns_added JSONB,
            status VARCHAR(50) NOT NULL,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_processed_files_status
            ON processed_files(status);
        CREATE INDEX IF NOT EXISTS idx_processed_files_filename
            ON processed_files(filename);
        CREATE INDEX IF NOT EXISTS idx_processed_files_hash
            ON processed_files(file_hash);
        """

        handler.execute(create_table_sql)
        handler.commit()
        logger.info("processed_files table created successfully")

    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        handler.rollback()
        raise
    finally:
        handler.close()


def get_db_connection():
    """
    Get a new database connection

    Returns:
        psycopg2 connection object
    """
    handler = DatabaseHandler()
    return handler.connect()
