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


def create_first_entry(fname, cspace, fsize, bdepth, status, encoding_type, fpath):
    '''
    Apply name and start time etc to dB
    '''

    start_time = format_date_time()
    processing_values = (fname,cspace,fsize,bdepth,start_time,start_time,status,encoding_type,fpath)
    sql = f''' INSERT INTO PROCESSING(name,colourspace,size_dpx,bitdepth,start,status,encoding_type,fpath) VALUES (?,?,?,?,?,?,?,?) '''
    try:
        connect = sqlite3.connect(DATABASE)
        connect.execute(
            'CREATE TABLE IF NOT EXISTS PROCESSING (name TEXT, colourspace TEXT, size_dpx TEXT, bitdepth TEXT, start TEXT, splitting TEXT, size_mkv TEXT, complete TEXT, status TEXT, encoding_type TEXT, fullpath TEXT)'
        )
        cur = connect.cursor()
        cur.execute(sql, processing_values)
        connect.commit()
        print(f"Record updated with new values {cur.lastrowid}")
        return cur.lastrowid
    except sqlite3.Error as err:
        print(f"Failed to update database: {err}")
        raise
    finally:
        if connect:
            connect.close()


def update_table(arg, fname, new_status):
    '''
    Update specific row with new
    data, for fname match
    '''
    data = (new_status, fname)
    if arg == 'status':
        sql_query = '''UPDATE PROCESSING SET status = ? WHERE name = ?'''
    elif arg == 'size_mkv':
        sql_query = '''UPDATE PROCESSING SET size_mkv = ? WHERE name = ?'''
    elif arg == 'splitting':
        sql_query = '''UPDATE PROCESSING SET splitting = ? WHERE name = ?'''
    elif arg == 'complete':
        sql_query = '''UPDATE PROCESSING SET complete = ? WHERE name = ?'''
    try:
        connect = sqlite3.connect(DATABASE)
        cur = connect.cursor()
        cur.execute(sql_query, data)
        connect.commit()
        cur.close()
        print(f"Record updated with new status {new_status}")
        return cur.lastrowid
    except sqlite3.Error as err:
        print(f"Failed to update database: {err}")
        raise
    finally:
        if connect:
            connect.close()
