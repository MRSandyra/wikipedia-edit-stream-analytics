#!/usr/bin/env python3
"""
Script inisialisasi DuckDB.
Menjalankan sekali di awal untuk memastikan schema database sudah siap.
"""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/wikipedia.duckdb")


def init_duckdb():
    """Initialize DuckDB with required tables"""
    print(f"Initializing DuckDB at {DUCKDB_PATH}")
    
    conn = duckdb.connect(DUCKDB_PATH)
    
    # Create silver table (enriched edits)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS enriched_edits (
            id VARCHAR,
            type VARCHAR,
            namespace INTEGER,
            title VARCHAR,
            comment VARCHAR,
            user VARCHAR,
            bot BOOLEAN,
            length_old INTEGER,
            length_new INTEGER,
            revision_old BIGINT,
            revision_new BIGINT,
            wiki VARCHAR,
            server_name VARCHAR,
            suspicion_score INTEGER,
            is_suspicious BOOLEAN,
            processed_at TIMESTAMP,
            raw_event JSON,
            PRIMARY KEY (id)
        )
    """)
    print("Created/verified enriched_edits table")
    
    # Create index for common queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_enriched_edits_processed_at 
        ON enriched_edits(processed_at)
    """)
    print("Created index on processed_at")
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_enriched_edits_wiki 
        ON enriched_edits(wiki)
    """)
    print("Created index on wiki")
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_enriched_edits_suspicious 
        ON enriched_edits(is_suspicious)
    """)
    print("Created index on is_suspicious")
    
    # Verify tables
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"Tables in database: {[t[0] for t in tables]}")
    
    conn.close()
    print("DuckDB initialization complete!")


if __name__ == "__main__":
    init_duckdb()