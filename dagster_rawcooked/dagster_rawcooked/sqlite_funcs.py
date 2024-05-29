import sqlite3

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