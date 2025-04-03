'''
Flask App for DPI downloader tool using SQLite database supply
Retrieve requests from HTML web input, update SQLite db with
new requests, using POST. Accessed by download Python script
using GET requests, and statuses updated as download progresses.

2023
'''

import os
import re
import sqlite3
import datetime
from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
@app.route('/home')
def index():
    return render_template('index_reset.html')

DBASE = os.environ.get('DATABASE')
CONNECT = sqlite3.connect(DBASE)
CONNECT.execute("""
                CREATE TABLE IF NOT EXISTS encoding_status (
                    process_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    seq_id TEXT NOT NULL,
                    status TEXT DEFAULT 'Started',
                    folder_path TEXT NOT NULL,
                    first_image TEXT,
                    last_image TEXT,
                    gaps_in_sequence TEXT,
                    assessment_pass TEXT,
                    assessment_complete TIMESTAMP,
                    colourspace TEXT,
                    seq_size INTEGER,
                    bitdepth INTEGER,
                    image_width TEXT,
                    image_height TEXT,
                    process_start TIMESTAMP,
                    encoding_choice TEXT,
                    encoding_log TEXT,
                    encoding_retry TEXT,
                    encoding_complete TIMESTAMP,
                    derivative_path TEXT,
                    derivative_size INTEGER,
                    derivative_md5 TEXT,
                    validation_complete TIMESTAMP,
                    validation_success TEXT,
                    error_message TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sequence_deleted TEXT,
                    moved_to_autoingest TEXT
                )
            """)


@app.route('/reset_request', methods=['GET', 'POST'])
def reset_request():
    '''
    Handle requests to clear all fields
    that relate to a specific seq_id if
    error_message present
    '''

    if request.method == 'GET':
        fname = request.args.get("file")
        transcode = request.args.get("option")
        if fname and transcode:
            return render_template('initiate_reset.html', file=fname, trans_option=transcode)

    if request.method == 'POST':
        seq_id = request.form['sequence'].strip()
        email = request.form['email'].strip()
        request = request.form['request'].strip()

        # Check for non-BFI email and reject
        if 'bfi.org.uk' not in email:
            return render_template('email_error_reset.html')
        if request == 'Full reset':
            status = 'Triggered assessment'
            date_stamp = str(datetime.datetime.today())[:19]
            with sqlite3.connect(DBASE) as users:
                cursor = users.cursor()
                cursor.execute(f"""
                    INSERT INTO encoding_status (
                        seq_id,
                        status,
                        folder_path,
                        first_image,
                        last_image,
                        gaps_in_sequence,
                        assessment_pass,
                        assessment_complete,
                        colourspace,
                        seq_size,
                        bitdepth,
                        image_width,
                        image_height,
                        process_start,
                        encoding_choice,
                        encoding_log,
                        encoding_retry,
                        encoding_complete,
                        derivative_path,
                        derivative_size,
                        derivative_md5,
                        validation_complete,
                        validation_success,
                        error_message,
                        last_updated,
                        sequence_deleted,
                        moved_to_autoingest
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?), (
                        {seq_id},
                        {status},
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        '',
                        {date_stamp},
                        '',
                        ''
                    )"""
                )
                users.commit()
        return render_template('index_reset.html')
    else:
        return render_template('initiate_reset.html')


@app.route('/encodings')
def encodings():
    '''
    Return the View all requested page
    '''
    connect = sqlite3.connect(DBASE)
    cursor = connect.cursor()
    cursor.execute(f"SELECT * FROM encoding_status where last_updated >= datetime('now','-100 days')")
    data = cursor.fetchall()
    return render_template("encodings.html", data=data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5500)