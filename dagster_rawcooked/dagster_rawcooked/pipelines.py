from dagster import Definitions, JobDefinition, multiprocess_executor
from assets_assess import get_dpx_folders, dynamic_process_subfolders, assessment, move_for_split_or_encoding
from assets_rawcook import get_dpx_folders, dynamic_process_subfolders,encoding
from .config import QNAP_FILM
from .resources import dpx_path_resource
from .loggers import log_status

dpx_assessment_job = JobDefinition(
    name="dpx_assessment_job",
    resource_defs={"storage_path": dpx_path_resource.configured({"dpx_path": QNAP_FILM})},
    asset_defs=[get_dpx_folders, dynamic_process_subfolders, assessment, move_for_split_or_encoding],
    executor_def=multiprocess_executor.configured({'max_concurrent': 1})
)

dpx_encoding_job = JobDefinition(
    name="dpx_encoding_job",
    resource_defs={"storage_path": dpx_path_resource.configured({"dpx_path": QNAP_FILM})},
    asset_defs=[get_dpx_folders, dynamic_process_subfolders, encoding],
    executor_def=multiprocess_executor.configured({'max_concurrent': 10})
)

defs = Definitions(
    jobs=[dpx_assessment_job, dpx_encoding_job],
    resources={"storage_path": dpx_path_resource.configured({"dpx_path": QNAP_FILM})},
    loggers={"log_status": log_status.configured({"name": "logger_path1", "log_level": "INFO"})}
)