import os
import dagster as dg
from dotenv import load_dotenv
from .assets import get_sequences, assessment, archiving, transcoding, validation, transcode_retry
from .sensors import failed_encoding_retry_sensor
from . import resources

# Global paths
PROJECT = dg.EnvVar("DAG_PROJECT")
DATABASE = dg.EnvVar("DATABASE")
CID_MEDIAINFO = dg.EnvVar("CID_MEDIAINFO")


def validate_env_vars():
    '''
    Check environmental variables are live before launch
    '''
    required_vars = ["PROJECT", "DATABASE", "CID_MEDIAINFO"]
    missing = [var for var in required_vars if not dg.EnvVar(var) and not os.path.exists(dg.EnvVar(var))]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

validate_env_vars()

process_assets1 = dg.load_assets_from_modules([
    get_sequences,
    assessment,
    archiving,
    transcoding,
    validation,
    transcode_retry
])

process_assets2 = dg.load_assets_from_modules([
    get_sequences,
    assessment,
    archiving,
    transcoding,
    validation,
    transcode_retry
])

process_assets3 = dg.load_assets_from_modules([
    get_sequences,
    assessment,
    archiving,
    transcoding,
    validation,
    transcode_retry
])
# Select just the encoding asset(s) that need retrying
encoding_assets = dg.AssetSelection.assets("reencode_failed_asset")

# Create a dedicated job for retrying failed encodings
backfill_failed_encodings_job = dg.define_asset_job(
    name="backfill_failed_encodings_job",
    selection=encoding_assets
)

# Define all assets job
all_assets_job = dg.define_asset_job(name="launch_process", selection="*")

# Schedule definitions
hourly_schedule1 = dg.ScheduleDefinition(
    name="hourly_schedule1",
    job=all_assets_job,
    cron_schedule="0 */2 * * *",
)

hourly_schedule2 = dg.ScheduleDefinition(
    name="hourly_schedule2",
    job=all_assets_job,
    cron_schedule="0 1-23/2 * * *",
)

hourly_schedule3 = dg.ScheduleDefinition(
    name="hourly_schedule3",
    job=all_assets_job,
    cron_schedule="30 */2 * * *",
)


# Project definitions, default project1
project1_defs = dg.Definitions(
    assets=process_assets1,
    resources={
        "source_path": dg.EnvVar('TARGET1'),
        "database": resources.SQLiteResource(filepath=dg.EnvVar('DATABASE')),
        "process_pool": resources.process_pool.configured({"num_processes": 2})
    },
    sensors=[failed_encoding_retry_sensor],
    jobs=[all_assets_job, backfill_failed_encodings_job],
    schedules=[hourly_schedule1]
)

project2_defs = dg.Definitions(
    assets=process_assets2,
    resources={
        "source_path": dg.EnvVar('TARGET2'),
        "database": resources.SQLiteResource(filepath=dg.EnvVar('DATABASE')),
        "process_pool": resources.process_pool.configured({"num_processes": 2})
    },
    sensors=[failed_encoding_retry_sensor],
    jobs=[all_assets_job, backfill_failed_encodings_job],
    schedules=[hourly_schedule2]
)

project3_defs = dg.Definitions(
    assets=process_assets3,
    resources={
        "source_path": dg.EnvVar('TARGET3'),
        "database": resources.SQLiteResource(filepath=dg.EnvVar('DATABASE')),
        "process_pool": resources.process_pool.configured({"num_processes": 2})
    },
    sensors=[failed_encoding_retry_sensor],
    jobs=[all_assets_job, backfill_failed_encodings_job],
    schedules=[hourly_schedule3]
)
