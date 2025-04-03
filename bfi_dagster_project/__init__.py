import os
import dagster as dg
from typing import List, Dict, Optional

# Asset imports
from .assets.get_sequences import build_target_sequences_asset
from .assets.assessment import build_assess_sequence_asset
from .assets.archiving import build_archiving_asset
from .assets.transcoding import build_transcode_ffv1_asset
from .assets.transcode_retry import build_transcode_retry_asset

# Sensor imports
from .sensors import build_failed_encoding_retry_sensor

# Resource imports
from . import resources

# Global environment variables
DATABASE = dg.EnvVar("DATABASE").get_value()
CID_MEDIAINFO = dg.EnvVar("CID_MEDIAINFO").get_value()


def validate_env_vars():
    '''
    Check that required environment variables are defined
    '''
    required_vars = ["DATABASE", "CID_MEDIAINFO", "TARGET1", "TARGET2", "TARGET3"]
    missing = [var for var in required_vars if not dg.EnvVar(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    

# For individual project deployment (used when deploying a single project)
def build_project_definitions(project_id: str, cron_schedule: str):
    '''
    Build complete Definitions object for a specific project
    For use when deploying a single project rather than the full repository
    '''
    validate_env_vars()

    # Directly create assets for this project
    target_seq_asset = build_target_sequences_asset(project_id)
    assess_seq_asset = build_assess_sequence_asset(project_id)
    archive_asset = build_archiving_asset(project_id)
    transcode_asset = build_transcode_ffv1_asset(project_id)
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
    if retry_asset is not None:
        project_assets.append(retry_asset)

    if not project_assets:
        raise ValueError(f"No valid assets found for project {project_id}")

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
            "source_path": dg.EnvVar(project_id).get_value(),
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
