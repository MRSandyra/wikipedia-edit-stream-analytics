#!/usr/bin/env python3
"""
Aggregator - Wikipedia Edit Stream Analytics
Job terjadwal untuk menghitung tabel gold dan cleanup bronze files.
"""

import logging
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

import duckdb

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("aggregator")

# Configuration
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/wikipedia.duckdb")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/app/data/bronze")
MAX_BRONZE_RETENTION_DAYS = int(os.getenv("MAX_BRONZE_RETENTION_DAYS", "3"))


def connect_duckdb_with_retry(path, read_only=False, retries=5, base_delay=0.2):
    """Open a DuckDB connection, retrying with jittered backoff on lock
    conflicts. DuckDB's embedded file allows only one process to hold it at
    a time, and consumer/aggregator/dashboard all touch the same file
    periodically — brief collisions are expected, not exceptional (Bug #33).
    """
    for attempt in range(retries):
        try:
            return duckdb.connect(path, read_only=read_only)
        except Exception as e:
            if "lock" not in str(e).lower() or attempt == retries - 1:
                raise
            time.sleep(base_delay * (attempt + 1) + random.uniform(0, base_delay))


class Aggregator:
    """Aggregator untuk menghitung gold tables dan cleanup bronze files"""
    
    def __init__(self, duckdb_path: str, bronze_path: str, bronze_retention_days: int):
        self.duckdb_path = duckdb_path
        self.bronze_path = bronze_path
        self.bronze_retention_days = bronze_retention_days
        self.duckdb = None
        self.scheduler = BlockingScheduler()
        
    def connect(self):
        """Connect ke DuckDB"""
        if self.duckdb is None:
            self.duckdb = connect_duckdb_with_retry(self.duckdb_path)
            logger.info(f"Connected to DuckDB at {self.duckdb_path}")
    
    def close(self):
        """Close DuckDB connection"""
        if self.duckdb:
            self.duckdb.close()
            self.duckdb = None
            logger.info("DuckDB connection closed")
    
    def _check_bronze_path(self):
        """Check if bronze path exists"""
        path = Path(self.bronze_path)
        return path.exists()
    
    def cleanup_bronze(self):
        """Cleanup old bronze Parquet files"""
        if not self._check_bronze_path():
            logger.info("Bronze path does not exist, skipping cleanup")
            return
        
        cutoff = datetime.now() - timedelta(days=self.bronze_retention_days)
        deleted_count = 0
        
        try:
            for file_path in Path(self.bronze_path).glob("**/*.parquet"):
                try:
                    file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_time < cutoff:
                        file_path.unlink()
                        deleted_count += 1
                        logger.debug(f"Deleted old bronze file: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete {file_path}: {e}")
            
            if deleted_count > 0:
                logger.info(f"Cleanup completed: deleted {deleted_count} files")
                
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
    
    def compute_edit_volume_1min(self):
        """Compute gold_edit_volume_1min: jumlah edit per menit per wiki"""
        try:
            self.duckdb.execute("""
                CREATE OR REPLACE TABLE gold_edit_volume_1min AS
                SELECT
                    date_trunc('minute', processed_at) AS minute,
                    wiki,
                    COUNT(*) AS edit_count
                FROM enriched_edits
                WHERE processed_at >= NOW() - INTERVAL '30 minutes'
                GROUP BY date_trunc('minute', processed_at), wiki
                ORDER BY minute DESC, edit_count DESC
            """)
            logger.debug("Computed gold_edit_volume_1min")
        except Exception as e:
            logger.error(f"Failed to compute gold_edit_volume_1min: {e}")
    
    def compute_top_pages_15min(self):
        """Compute gold_top_pages_15min: top halaman paling banyak diedit (15 menit)"""
        try:
            self.duckdb.execute("""
                CREATE OR REPLACE TABLE gold_top_pages_15min AS
                SELECT
                    title,
                    COUNT(*) AS edit_count,
                    COUNT(DISTINCT user) AS unique_editors
                FROM enriched_edits
                WHERE processed_at >= NOW() - INTERVAL '15 minutes'
                GROUP BY title
                ORDER BY edit_count DESC
                LIMIT 100
            """)
            logger.debug("Computed gold_top_pages_15min")
        except Exception as e:
            logger.error(f"Failed to compute gold_top_pages_15min: {e}")
    
    def compute_suspicious_feed(self):
        """Compute gold_suspicious_feed: 200 edit suspicious terbaru"""
        try:
            self.duckdb.execute("""
                CREATE OR REPLACE TABLE gold_suspicious_feed AS
                SELECT
                    id,
                    processed_at,
                    wiki,
                    title,
                    user,
                    bot,
                    suspicion_score,
                    comment
                FROM enriched_edits
                WHERE is_suspicious = true
                ORDER BY processed_at DESC
                LIMIT 200
            """)
            logger.debug("Computed gold_suspicious_feed")
        except Exception as e:
            logger.error(f"Failed to compute gold_suspicious_feed: {e}")
    
    def compute_bot_human_ratio(self):
        """Compute gold_bot_human_ratio_1min: jumlah edit bot vs human per menit"""
        try:
            self.duckdb.execute("""
                CREATE OR REPLACE TABLE gold_bot_human_ratio_1min AS
                SELECT
                    date_trunc('minute', processed_at) AS minute,
                    wiki,
                    SUM(CASE WHEN bot = true THEN 1 ELSE 0 END) AS bot_count,
                    SUM(CASE WHEN bot = false THEN 1 ELSE 0 END) AS human_count,
                    COUNT(*) AS total_count
                FROM enriched_edits
                WHERE processed_at >= NOW() - INTERVAL '30 minutes'
                GROUP BY date_trunc('minute', processed_at), wiki
                ORDER BY minute DESC
            """)
            logger.debug("Computed gold_bot_human_ratio_1min")
        except Exception as e:
            logger.error(f"Failed to compute gold_bot_human_ratio_1min: {e}")
    
    def aggregate(self):
        """Run all aggregation jobs"""
        try:
            self.connect()
            
            # Run all computations
            self.compute_edit_volume_1min()
            self.compute_top_pages_15min()
            self.compute_suspicious_feed()
            self.compute_bot_human_ratio()
            
            # Run cleanup
            self.cleanup_bronze()
            
            logger.info("Aggregation completed successfully")
            
        except Exception as e:
            logger.error(f"Aggregation error: {e}")
        finally:
            self.close()
    
    def start(self):
        """Start the scheduler"""
        # Run immediately on start
        self.aggregate()
        
        # Schedule to run every 60 seconds
        self.scheduler.add_job(
            self.aggregate,
            trigger=IntervalTrigger(seconds=60),
            id="aggregate",
            replace_existing=True
        )
        
        logger.info("Starting aggregator scheduler (interval: 60 seconds)")
        
        try:
            self.scheduler.start()
        except KeyboardInterrupt:
            logger.info("Aggregator stopped by user")
        finally:
            self.scheduler.shutdown()


if __name__ == "__main__":
    aggregator = Aggregator(DUCKDB_PATH, BRONZE_PATH, MAX_BRONZE_RETENTION_DAYS)
    aggregator.start()