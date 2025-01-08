'''
Create/populate the dagster_rawcook.db
sqlite3 database to store data about
the DPX encoding workflows
'''
# Imports
import os
from dagster import asset
from dagster._utils.backoff import backoff
import duckdb
import sqlite3


@asset
def encoding_database():
    '''
    Initialize and maintain connection to SQLite database tracking encoding progress.
    This is a foundational asset that other assets depend on for state tracking.
    The database serves as the source of truth for encoding progress.
    DUCKDB method better with backoff() but won't work as below (see exaple in Dagster Uni)
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
            colorspace TEXT DEFAULT 0,
            dpx_size INTEGER DEFAULT 0,
            bitdepth INTEGER DEFAULT 0,
            process_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            mkv_size INTEGER DEFAULT 0,
            assessment_complete TIMESTAMP,
            rawcook_complete TIMESTAMP 0,
            validation_complete TIMESTAMP 0,
            validation_success TEXT,
            wipeup_complete INTEGER DEFAULT 0,
            status TEXT,
            current_stage TEXT,
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