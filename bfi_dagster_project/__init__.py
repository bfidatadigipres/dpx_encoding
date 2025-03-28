import os
import dagster as dg
from typing import List, Dict, Optional

# Asset imports
from .assets.get_sequences import build_target_sequences_asset
from .assets.assessment import build_assess_sequence_asset
from .assets.archiving import build_archiving_asset
from .assets.transcoding import build_transcode_ffv1_asset
from .assets.validation import build_validation_asset
from .assets.transcode_retry import build_transcode_retry_asset

# Sensor imports
from .sensors import build_failed_encoding_retry_sensor

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
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


@dg.repository
def bfi_repository():
    '''
    Repository definition with direct creation of all assets, jobs, and schedules
    '''
    # Optional: Validate environment variables
    # validate_env_vars()
    
    all_assets = []
    all_jobs = []
    all_schedules = []
    all_sensors = []
    
    # Define project configurations
    project_configs = [
        {"id": "TARGET1", "cron": "0 */2 * * *"},
        {"id": "TARGET2", "cron": "0 1-23/2 * * *"},
        {"id": "TARGET3", "cron": "30 */2 * * *"}
    ]
    
    # Create assets, jobs, and schedules for each project
    for config in project_configs:
        project_id = config["id"]
        cron_schedule = config["cron"]
        
        # Directly create all assets for this project
        target_seq_asset = build_target_sequences_asset(project_id)
        assess_seq_asset = build_assess_sequence_asset(project_id)
        archive_asset = build_archiving_asset(project_id)
        transcode_asset = build_transcode_ffv1_asset(project_id)
        validate_asset = build_validation_asset(project_id)
        retry_asset = build_transcode_retry_asset(project_id)
        
        # Add all valid assets to our collection
        project_assets = []
        if target_seq_asset is not None:
            project_assets.append(target_seq_asset)
            all_assets.append(target_seq_asset)
        
        if assess_seq_asset is not None:
            project_assets.append(assess_seq_asset)
            all_assets.append(assess_seq_asset)
        
        if archive_asset is not None:
            project_assets.append(archive_asset)
            all_assets.append(archive_asset)
        
        if transcode_asset is not None:
            project_assets.append(transcode_asset)
            all_assets.append(transcode_asset)
        
        if validate_asset is not None:
            project_assets.append(validate_asset)
            all_assets.append(validate_asset)
        
        if retry_asset is not None:
            project_assets.append(retry_asset)
            all_assets.append(retry_asset)
            
            # Create retry job (only if retry asset exists)
            retry_job = dg.define_asset_job(
                name=f"{project_id}_retry_job",
                selection=dg.AssetSelection.assets(retry_asset.key)
            )
            all_jobs.append(retry_job)
        
        # Create sensor for this project
        retry_sensor = build_failed_encoding_retry_sensor(project_id)
        if retry_sensor is not None:
            all_sensors.append(retry_sensor)
        
        # Create process job (runs all assets for this project)
        if project_assets:  # Only create job if we have assets
            asset_keys = [asset.key for asset in project_assets]
            process_job = dg.define_asset_job(
                name=f"{project_id}_process_job",
                selection=dg.AssetSelection.assets(*asset_keys)
            )
            all_jobs.append(process_job)
            
            # Create schedule for the process job
            schedule = dg.ScheduleDefinition(
                name=f"{project_id}_schedule",
                job=process_job,
                cron_schedule=cron_schedule
            )
            all_schedules.append(schedule)
    
    # Create a utility job that can run any asset
    all_job = dg.define_asset_job(
        name="run_all_assets",
        selection="*"
    )
    all_jobs.append(all_job)
    
    # Define resources
    resource_defs = {
        "database": resources.SQLiteResource(filepath=DATABASE),
        "process_pool": resources.process_pool.configured({"num_processes": 2})
    }
    
    # Return a single Definitions object with everything
    return dg.Definitions(
        assets=all_assets,
        sensors=all_sensors,
        jobs=all_jobs,
        schedules=all_schedules,
        resources=resource_defs
    )


# For individual project deployment (used when deploying a single project)
def build_project_definitions(project_id: str, cron_schedule: str):
    '''
    Build complete Definitions object for a specific project
    For use when deploying a single project rather than the full repository
    '''
    # Directly create assets for this project
    target_seq_asset = build_target_sequences_asset(project_id)
    assess_seq_asset = build_assess_sequence_asset(project_id)
    archive_asset = build_archiving_asset(project_id)
    transcode_asset = build_transcode_ffv1_asset(project_id)
    validate_asset = build_validation_asset(project_id)
    retry_asset = build_transcode_retry_asset(project_id)
    
    # Collect valid assets
    project_assets = []
    if target_seq_asset is not None:
        project_assets.append(target_seq_asset)
    if assess_seq_asset is not None:
        project_assets.append(assess_seq_asset)
    if archive_asset is not None:
        project_assets.append(archive_asset)
    if transcode_asset is not None:
        project_assets.append(transcode_asset)
    if validate_asset is not None:
        project_assets.append(validate_asset)
    if retry_asset is not None:
        project_assets.append(retry_asset)
    
    # Create process job for all assets
    process_job = dg.define_asset_job(
        name=f"{project_id}_process_job",
        selection=dg.AssetSelection.assets(*[asset.key for asset in project_assets])
    )
    
    # Create retry job if retry asset exists
    jobs = [process_job]
    if retry_asset is not None:
        retry_job = dg.define_asset_job(
            name=f"{project_id}_retry_job",
            selection=dg.AssetSelection.assets(retry_asset.key)
        )
        jobs.append(retry_job)
    
    # Create sensor
    sensors = []
    retry_sensor = build_failed_encoding_retry_sensor(project_id)
    if retry_sensor is not None:
        sensors.append(retry_sensor)
    
    return dg.Definitions(
        assets=project_assets,
        resources={
            "source_path": dg.EnvVar(project_id),
            "database": resources.SQLiteResource(filepath=DATABASE),
            "process_pool": resources.process_pool.configured({"num_processes": 2})
        },
        sensors=sensors,
        jobs=jobs,
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
