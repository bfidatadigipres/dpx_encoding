import os
import dagster as dg
from typing import List, Dict, Optional

# Asset imports
from .assets.get_sequences import target_sequences, build_target_sequences_asset
from .assets.assessment import assess_sequence, build_assess_sequence_asset
from .assets.archiving import create_tar, build_archiving_asset
from .assets.transcoding import transcode_ffv1, build_transcode_ffv1_asset
from .assets.validation import validate_output, build_validation_asset
from .assets.transcode_retry import reencode_failed_asset, build_transcode_retry_asset

# Sensor imports
from .sensors import failed_encoding_retry_sensor, build_failed_encoding_retry_sensor

# Resource imports
from . import resources

# Global environment variables
DATABASE = dg.EnvVar("DATABASE")
CID_MEDIAINFO = dg.EnvVar("CID_MEDIAINFO")


def validate_env_vars():
    '''
    Check that required environment variables are defined
    '''
    required_vars = ["DATABASE", "CID_MEDIAINFO", "TARGET1", "TARGET2", "TARGET3"]
    missing = [var for var in required_vars if not dg.EnvVar(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


def create_project_definitions(project_id: str):
    '''
    Create a set of assets and sensors with project-specific prefixes
    '''
    assets = [
        build_target_sequences_asset(project_id),
        build_assess_sequence_asset(project_id),
        build_archiving_asset(project_id),
        build_transcode_ffv1_asset(project_id),
        build_validation_asset(project_id),
        build_transcode_retry_asset(project_id)
    ]

    sensors = [
        build_failed_encoding_retry_sensor(project_id)
    ]
    
    return assets, sensors


def create_project_schedule(project_id: str, cron_schedule: str, project_assets):
    '''
    Create a schedule for a specific project
    '''
    # Create job for all assets with this project's prefix
    job_name = f"{project_id}_process_job"

    # Select all assets with this project's prefix using a list instead of keys_by_prefix
    selection = dg.AssetSelection.assets(*[asset.key for asset in project_assets])
    job = dg.define_asset_job(
        name=job_name,
        selection=selection
    )
    
    # Create schedule with the job
    return dg.ScheduleDefinition(
        name=f"{project_id}_schedule",
        job=job,
        cron_schedule=cron_schedule,
    )


def create_project_retry_job(project_id: str, retry_asset=None):
    '''
    Create a job for retrying failed encodings for a specific project
    '''
    # Instead of selecting by key, use the passed asset
    job_name = f"{project_id}_retry_job" if project_id else "backfill_failed_encodings_job"
    
    if retry_asset:
        # Use AssetSelection.assets() with the asset key
        return dg.define_asset_job(
            name=job_name,
            selection=dg.AssetSelection.assets(retry_asset.key)
        )
    else:
        # For the default case with no prefix
        return dg.define_asset_job(
            name=job_name, 
            selection=dg.AssetSelection.assets("reencode_failed_asset")
        )


@dg.repository
def bfi_repository():
    '''
    Repository definition with all assets, sensors, jobs, and schedules
    '''
    # Validate environment variables
    #validate_env_vars()
    
    # Project configuration
    projects = [
        {"id": "TARGET1", "cron": "0 */2 * * *"},
        {"id": "TARGET2", "cron": "0 1-23/2 * * *"},
        {"id": "TARGET3", "cron": "30 */2 * * *"}
    ]
    
    # Default assets (without prefixes)
    default_assets = [
        target_sequences,
        assess_sequence,
        create_tar,
        transcode_ffv1,
        validate_output,
        reencode_failed_asset
    ]
    
    # Default sensor
    default_sensors = [
        failed_encoding_retry_sensor
    ]
    
    # Default jobs
    default_jobs = [
        dg.define_asset_job(name="default_process_job", selection=default_assets),
        create_project_retry_job("")  # Empty string for no prefix
    ]
    
    # Project-specific components
    project_assets = []
    project_sensors = []
    project_jobs = []
    project_schedules = []
    
    for project in projects:
        project_id = project["id"]
        cron = project["cron"]
        
        # Create assets and sensors for this project
        assets, sensors = create_project_definitions(project_id)
        project_assets.extend(assets)
        project_sensors.extend(sensors)
        
        # Get the retry asset for this project (it's the last one in our assets list)
        retry_asset = assets[-1]  # This should be the build_transcode_retry_asset result
        
        # Create jobs and schedules for this project
        project_jobs.append(create_project_retry_job(project_id, retry_asset))
        project_schedules.append(create_project_schedule(project_id, cron, assets))
    
    # Resource definitions
    resource_defs = {
        "database": resources.SQLiteResource(filepath=DATABASE),
        "process_pool": resources.process_pool.configured({"num_processes": 2}),
        # Dynamic source path will be set per job run
    }
    
    # Create and return all definitions
    return [
        *default_assets,
        *project_assets,
        *default_sensors,
        *project_sensors,
        *default_jobs,
        *project_jobs,
        *project_schedules,
        resource_defs
    ]


# For backwards compatibility and direct imports
# These are maintained separately from the repository definition
default_retry_job = dg.define_asset_job(
    name="backfill_failed_encodings_job",
    selection=dg.AssetSelection.assets("reencode_failed_asset")
)

default_all_job = dg.define_asset_job(
    name="launch_process", 
    selection="*"
)

# Individual project definitions can be used directly if needed
def build_project_definitions(project_id: str, cron_schedule: str):
    '''
    Build complete Definitions object for a specific project
    '''
    project_assets, project_sensors = create_project_definitions(project_id)
    
    # Get the retry asset specifically
    retry_asset = project_assets[-1]  # This should be the retry asset
    
    process_job = dg.define_asset_job(
        name=f"{project_id}_process_job",
        selection=project_assets
    )
    
    retry_job = dg.define_asset_job(
        name=f"{project_id}_retry_job",
        selection=[retry_asset]
    )
    
    return dg.Definitions(
        assets=project_assets,
        resources={
            "source_path": dg.EnvVar(project_id),
            "database": resources.SQLiteResource(filepath=DATABASE),
            "process_pool": resources.process_pool.configured({"num_processes": 2})
        },
        sensors=project_sensors,
        jobs=[retry_job, process_job],
        schedules=[
            dg.ScheduleDefinition(
                name=f"{project_id}_schedule",
                job=process_job,
                cron_schedule=cron_schedule
            )
        ]
    )

# Pre-built project definitions for direct use
project1_defs = build_project_definitions("TARGET1", "0 */2 * * *")
project2_defs = build_project_definitions("TARGET2", "0 1-23/2 * * *")
project3_defs = build_project_definitions("TARGET3", "30 */2 * * *")
