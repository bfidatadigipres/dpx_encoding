'''
Suggested fields for dB:
name TEXT, colourspace TEXT, size_dpx TEXT, bitdepth TEXT, start TEXT,
splitting TEXT, size_mkv TEXT, complete TEXT, status TEXT, rawcook_version TEXT
'''

import sqlite3
import datetime
from .config import DATABASE

CONNECT = sqlite3.connect(DATABASE)
CONNECT.execute(
    'CREATE TABLE IF NOT EXISTS PROCESSING (name TEXT, colourspace TEXT, size_dpx TEXT, bitdepth TEXT, start TEXT, splitting TEXT, size_mkv TEXT, complete TEXT, status TEXT, rawcook_version TEXT)'
)

def format_date_time():
    '''
    Return string date time for now
    '''
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%d %H:%M:%S')


def create_frist_entry(fname):
    '''
    Apply name and start time etc
    to dB to ensure it's operational
    '''
    connect = sqlite3.connect(DATABASE)
    connect.execute(
        'CREATE TABLE IF NOT EXISTS PROCESSING (name TEXT, colourspace TEXT, size_dpx TEXT, bitdepth TEXT, start TEXT, splitting TEXT, size_mkv TEXT, complete TEXT, status TEXT, rawcook_version TEXT)'
    )
    # Do first entry here
    pass


def update_table(fname, new_status, database):
    '''
    Update specific row with new
    data, for fname match
    '''
    try:
        sqlite_connection = sqlite3.connect(database)
        cursor = sqlite_connection.cursor()
        # Update row with new status
        sql_query = '''UPDATE PROCESSING SET status = ? WHERE name = ?'''
        data = (new_status, fname)
        cursor.execute(sql_query, data)
        sqlite_connection.commit()
        print(f"Record updated with new status {new_status}")
        cursor.close()
    except sqlite3.Error as err:
        print(f"Failed to update database: {err}")
        raise
    finally:
        if sqlite_connection:
            sqlite_connection.close()