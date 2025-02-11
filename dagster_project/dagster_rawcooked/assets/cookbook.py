'''
Create/populate the rawcook.db
sqlite3 database to store data about
the DPX encoding workflows
'''
# Imports
import os
from dagster import asset
from dagster._utils.backoff import backoff
import sqlite3


@asset
def encoding_database():
    '''
    Initialize and maintain connection to SQLite database tracking encoding progress.
    This is a foundational asset that other assets depend on for state tracking.
    The database serves as the source of truth for encoding progress.
    '''
    conn = backoff(fn=sqlite3.connect,
                   retry_on=(RuntimeError, sqlite3.SQLITE_IOERR),
                   kwargs={
                       'database': os.getenv('RAWCOOK_DB') 
                   },
                   max_retries=10
    )
    cursor = conn.cursor()

    # Create tables if they don't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS encoding_status (
            dpx_id TEXT PRIMARY KEY,
            folder_path TEXT,
            first_dpx TEXT,
            last_dpx TEXT,
            gaps_in_sequence TEXT,
            assessment_pass BOOL,
            assessment_complete TIMESTAMP,
            colorspace TEXT DEFAULT 0,
            dir_size INTEGER DEFAULT 0,
            bitdepth INTEGER DEFAULT 0,
            resolution TEXT,
            process_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            encoding_choice TEXT,
            rawcook_complete TIMESTAMP 0,
            mkv_path TEXT,
            mkv_size INTEGER DEFAULT 0,
            validation_complete TIMESTAMP 0,
            validation_success TEXT,
            wipeup_complete INTEGER DEFAULT 0,
            status TEXT,
            error_message TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # For status queries, idx status addition
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_status 
        ON encoding_status(status, assessment_complete)
    """)

    return conn