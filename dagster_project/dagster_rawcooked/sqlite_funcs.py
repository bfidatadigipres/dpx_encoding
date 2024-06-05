'''
Suggested fields for dB:
name TEXT, colourspace TEXT, size_dpx TEXT, bitdepth TEXT, start TEXT,
splitting TEXT, size_mkv TEXT, complete TEXT, status TEXT, rawcook_version TEXT
'''

from dagster import asset
import sqlite3
import datetime


def format_date_time():
    '''
    Return string date time for now
    '''
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%d %H:%M:%S')

@asset
def make_filename_entry(fname, fpath, database):
    '''
    Find list of sequences in dpx_to_assess
    and move into dB
    '''

    processing_values = (fname,fpath)
    sql = f''' INSERT INTO PROCESSING(name,fpath) VALUES (?,?) '''
    try:
        connect = sqlite3.connect(database)
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


@asset
def retrieve_fnames(DATABASE):
    '''
    Read dB and retrieve new entries made today
    '''
    rdata = []
    try:
        sqlite_conn = sqlite3.connect(DATABASE)
        cursor = sqlite_conn.cursor()
        cursor.execute(''' SELECT * FROM PROCESSING WHERE fname = "N_*" AND WHERE start = ISNULL ''')
        data = cursor.fetchall()
        for row in data:
            rdata.append(row[10])
    except sqlite3.Error as err:
        print(err)
    finally:
        if sqlite_conn:
            sqlite_conn.close()
    return rdata


def create_first_entry(fname, cspace, fsize, bdepth, status, encoding_type, fpath, database):
    '''
    Apply name and start time etc to dB
    '''

    start_time = format_date_time()
    processing_values = (fname,cspace,fsize,bdepth,start_time,start_time,status,encoding_type,fpath)
    sql = f''' INSERT INTO PROCESSING(name,colourspace,size_dpx,bitdepth,start,status,encoding_type,fpath) VALUES (?,?,?,?,?,?,?,?) '''
    try:
        connect = sqlite3.connect(database)
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


def update_table(arg, fname, new_status, database):
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
        connect = sqlite3.connect(database)
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
