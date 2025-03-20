import os
import dagster as dg
from dotenv import load_dotenv
from .assets import get_sequences, assessment, archiving, transcoding, validation, transcode_retry
from .sensors import failed_encoding_retry_sensor
from . import resources


# Get environment variables from .env
load_dotenv()
PROJECT = os.environ.get("PROJECT")
IMG_PROC = os.environ.get("IMG_PROC")
AUTOINGEST = os.environ.get("AUTOINGEST")
DATABASE = os.environ.get("DATABASE")
LOGS = os.environ.get("LOGS")
FAILS = os.environ.get("FAILS")
CID_MEDIAINFO = os.environ.get("CID_MEDIAINFO")

# Example validation at startup
def validate_env_vars():
    required_vars = ["PROJECT", "IMG_PROC", "AUTOINGEST", "DATABASE", "LOGS", "FAILS", "CID_MEDIAINFO"]
    missing = [var for var in required_vars if not os.environ.get(var) and not os.path.exists(os.environ.get(var))]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
validate_env_vars()

process_assets = dg.load_assets_from_modules([
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

# Schedule definition
hourly_schedule = dg.ScheduleDefinition(
    name="hourly_schedule",
    job=all_assets_job,
    cron_schedule="0 * * * *",
)

# Def for resources
resources = {
    "database": resources.SQLiteResource(filepath=f"{DATABASE}"),
    "process_pool": resources.process_pool.configured({"num_processes": 4})
}

defs = dg.Definitions(
    assets=process_assets,
    resources=resources,
    sensors=[failed_encoding_retry_sensor],
    jobs=[all_assets_job, backfill_failed_encodings_job],
    schedules=[hourly_schedule]
)
