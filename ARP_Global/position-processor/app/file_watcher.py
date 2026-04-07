import time
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent

from config import Config
from csv_processor_v2 import process_csv_file

logger = logging.getLogger(__name__)


class CsvFileHandler(FileSystemEventHandler):
    """Handle file system events for CSV files matching configured patterns"""

    def __init__(self, file_patterns: dict, debounce_seconds=5):
        self.file_patterns = file_patterns  # {glob_pattern: table_name}
        self.debounce_seconds = debounce_seconds
        self.pending_files = {}  # {filepath: (last_modified_time, table_name)}

    def on_created(self, event):
        """Handle file creation events"""
        if not event.is_directory:
            table_name = self._match_pattern(event.src_path)
            if table_name:
                logger.info(f"Detected new file: {event.src_path}")
                self._schedule_processing(event.src_path, table_name)

    def on_modified(self, event):
        """Handle file modification events"""
        if not event.is_directory:
            table_name = self._match_pattern(event.src_path)
            if table_name:
                logger.debug(f"File modified: {event.src_path}")
                self._schedule_processing(event.src_path, table_name)

    def _match_pattern(self, filepath):
        """Check if file matches any configured pattern, return table name or None"""
        for pattern, table_name in self.file_patterns.items():
            if Path(filepath).match(pattern):
                return table_name
        return None

    def _schedule_processing(self, filepath, table_name):
        """Schedule file for processing after debounce period"""
        self.pending_files[filepath] = (time.time(), table_name)

    def process_pending_files(self):
        """Process files that have been stable for debounce period"""
        current_time = time.time()
        files_to_process = []

        for filepath, (scheduled_time, table_name) in list(self.pending_files.items()):
            if current_time - scheduled_time >= self.debounce_seconds:
                if self._is_file_stable(filepath):
                    files_to_process.append((filepath, table_name))
                    del self.pending_files[filepath]

        for filepath, table_name in files_to_process:
            try:
                logger.info(f"Processing file: {filepath} → {table_name}")
                process_csv_file(filepath, table_name)
            except Exception as e:
                logger.error(f"Error processing {filepath}: {e}", exc_info=True)

    def _is_file_stable(self, filepath):
        """Check if file size hasn't changed (file write complete)"""
        try:
            size1 = Path(filepath).stat().st_size
            time.sleep(1)
            size2 = Path(filepath).stat().st_size
            return size1 == size2
        except Exception:
            return False


class CsvFileWatcher:
    """File system watcher for CSV files matching configured patterns"""

    def __init__(self, watch_dir, file_patterns: dict):
        self.watch_dir = watch_dir
        self.event_handler = CsvFileHandler(file_patterns, Config.DEBOUNCE_SECONDS)
        self.observer = Observer()
        self.observer.schedule(self.event_handler, watch_dir, recursive=False)
        self.running = False

    def start(self):
        """Start watching for file changes"""
        self.observer.start()
        self.running = True
        logger.info(f"Watching directory: {self.watch_dir}")
        logger.info(f"Watching patterns: {self.event_handler.file_patterns}")

        try:
            # Background thread to process pending files
            while self.running:
                self.event_handler.process_pending_files()
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop watching"""
        self.running = False
        self.observer.stop()
        self.observer.join()
        logger.info("File watcher stopped")


# Backward-compatible aliases
PositionFileHandler = CsvFileHandler
PositionFileWatcher = CsvFileWatcher
