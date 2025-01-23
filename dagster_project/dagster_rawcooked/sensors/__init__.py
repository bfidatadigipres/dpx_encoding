'''
From Dagster training, read JSON config. Not required here, read SQLite
required for status 'Transcoded' which triggers next process preferably.
The idea being this sensor launches post-rawcooked checks
'''
from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor
)
import os
import json
import sqlite3
from datetime import datetime
from ..jobs import adhoc_request_job

@sensor(
    job=adhoc_request_job
)
def adhoc_request_sensor(context: SensorEvaluationContext):
    PATH_TO_SEQUENCE = os.getenv("ENCODE_PATH")
    RAWCOOKED_DB = os.getenv("RAWCOOK_DB")

    # Set up cursor context for run
    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    runs_to_request = []

    for fname in os.listdir(PATH_TO_SEQUENCE):
        file_path = os.path.join(PATH_TO_SEQUENCE, fname)
        if os.path.isdir(file_path):
            # Check here for entry in SQLITE DB, set new status
            pass
            




            if status is None:
                current_state[fname] = 'new_sequence'
            elif status is 'retry':
                current_state[fname] = 'retry_sequence'

            # if the file is new or has been modified since the last run, add it to the request queue
            if fname not in previous_state or previous_state[fname] != 'new_sequence' or previous_state[fname] != 'retry_sequence':
                with open(file_path, "r") as f:
                    request_config = json.load(f)

                    runs_to_request.append(RunRequest(
                        run_key=f"adhoc_request_{fname}_{str(datetime.now())[:19]}",
                        run_config={
                            "ops": {
                                "adhoc_request": {
                                    "config": {
                                        "filename": fname,
                                        **request_config
                                    }
                                }
                            }
                        }
                    ))

    return SensorResult(
        run_requests=runs_to_request,
        cursor=json.dumps(current_state)
    )