import logging
import os
import glob
from config import Config
from db_handler import ensure_database_exists, ensure_tables_exist
from csv_processor_v2 import process_existing_files
from file_watcher import CsvFileWatcher

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def table_exists(table_name='position_detail'):
    """Check if a table exists"""
    try:
        from db_handler import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            )
        """, (table_name,))
        exists = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return exists
    except:
        return False


def main():
    """Main entry point"""
    logger.info("="*60)
    logger.info("Starting ARP CSV File Processor")
    logger.info("="*60)

    # Validate configuration
    try:
        Config.validate()
        logger.info("Configuration validated successfully")
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return

    # Load file patterns
    file_patterns = Config.get_file_patterns()
    logger.info(f"Configured file patterns: {file_patterns}")

    # Initialize database
    logger.info("Initializing database...")
    try:
        ensure_database_exists()
        ensure_tables_exist()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return

    # No need to manually create data tables
    # pandas to_sql() will handle table creation automatically
    logger.info("Using pandas to_sql() for automatic schema management")

    # Process existing files
    logger.info("Processing existing files...")
    try:
        process_existing_files()
        logger.info("Finished processing existing files")
    except Exception as e:
        logger.error(f"Error processing existing files: {e}")

    # Start file watcher
    logger.info("Starting file watcher...")
    try:
        watcher = CsvFileWatcher(
            watch_dir=Config.WATCH_DIR,
            file_patterns=file_patterns
        )
        watcher.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        watcher.stop()
    except Exception as e:
        logger.error(f"File watcher error: {e}")


if __name__ == "__main__":
    main()
