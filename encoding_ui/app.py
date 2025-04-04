'''
Flask App for DPX/TIF encoding tool using SQLite database supply
Retrieve refresh requests from HTML web input, update SQLite db with
new requests, using POST. Viewed by teams, 100 day since last update.

2025
'''

import os
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
                    moved_to_autoingest TEXT,
                    project TEXT
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
        seq_id = request.args.get("seq_id")
        email = request.args.get("email")
        if seq_id and email:
            return render_template('initiate_reset.html', seq_id=seq_id, email=email)

    if request.method == 'POST':
        seq_id = request.form['seq_id'].strip()
        email = request.form['email'].strip()
        req = request.form['request'].strip()
        capture_log(email, seq_id)

        # Check for non-BFI email and reject
        if 'bfi.org.uk' not in email:
            return render_template('error_email_reset.html')
        if req == 'Full reset':
            status = 'Triggered assessment'
            date_stamp = str(datetime.datetime.today())[:19]
            with sqlite3.connect(DBASE) as users:
                cursor = users.cursor()
                cursor.execute(f"""
                    UPDATE encoding_status SET
                        status = ?,
                        folder_path = '',
                        first_image = NULL,
                        last_image = NULL,
                        gaps_in_sequence = NULL,
                        assessment_pass = NULL,
                        assessment_complete = NULL,
                        colourspace = NULL,
                        seq_size = NULL,
                        bitdepth = NULL,
                        image_width = NULL,
                        image_height = NULL,
                        process_start = ?,
                        encoding_choice = NULL,
                        encoding_log = NULL,
                        encoding_retry = NULL,
                        encoding_complete = NULL,
                        derivative_path = NULL,
                        derivative_size = NULL,
                        derivative_md5 = NULL,
                        validation_complete = NULL,
                        validation_success = NULL,
                        error_message = NULL,
                        last_updated = ?,
                        sequence_deleted = NULL,
                        moved_to_autoingest = NULL
                    WHERE seq_id = ?
                """, (status, date_stamp, date_stamp, seq_id))
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
    connect.execute("PRAGMA wal_checkpoint(FULL)")
    cursor = connect.cursor()
    cursor.execute(f"SELECT * FROM encoding_status where last_updated >= datetime('now','-100 days')")
    data = cursor.fetchall()
    return render_template("encodings.html", data=data)


def capture_log(email: str, sequence: str) -> None:
    '''
    Capture email / sequence requests
    with datestamp
    '''
    date_stamp = str(datetime.datetime.today())[:19]
    with open("refresh_requests.log", "a") as log:
        log.write(f"{date_stamp} {email} {sequence}\n")


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False, port=5500)