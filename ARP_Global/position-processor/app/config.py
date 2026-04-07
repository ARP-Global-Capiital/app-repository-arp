import os
import json
from dataclasses import dataclass

DEFAULT_FILE_PATTERNS = {
    "Position*.csv": "position_detail",
    "FundRiskMetrics*.csv": "fund_risk_metrics"
}

@dataclass
class Config:
    """Application configuration"""

    # Database configuration
    DB_HOST: str = os.getenv('DB_HOST', 'arp-tooling-ir-postgres.chyeiegwedeb.eu-west-1.rds.amazonaws.com')
    DB_PORT: int = int(os.getenv('DB_PORT', '5432'))
    DB_NAME: str = os.getenv('DB_NAME', 'global')
    DB_USER: str = os.getenv('DB_USER', 'tooling_admin')
    DB_PASSWORD: str = os.getenv('DB_PASSWORD')

    # File watching configuration
    WATCH_DIR: str = os.getenv('WATCH_DIR', '/data')
    FILE_PATTERN: str = os.getenv('FILE_PATTERN', 'Position*.csv')

    # Processing configuration
    CHUNK_SIZE: int = int(os.getenv('CHUNK_SIZE', '1000'))
    DEBOUNCE_SECONDS: int = int(os.getenv('DEBOUNCE_SECONDS', '5'))

    # Logging
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

    @classmethod
    def get_file_patterns(cls) -> dict:
        """Get file pattern to table name mapping"""
        raw = os.getenv('FILE_PATTERNS')
        if raw:
            return json.loads(raw)
        return DEFAULT_FILE_PATTERNS

    @classmethod
    def validate(cls):
        """Validate required configuration"""
        if not cls.DB_PASSWORD:
            raise ValueError("DB_PASSWORD environment variable is required")
