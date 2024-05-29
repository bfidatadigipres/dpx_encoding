from dagster import Definitions
from assess import get_dpx_folders, dynamic_process_subfolders, dpx_assessment
from .config import QNAP_FILM, QNAP_FILMOPS, QNAP_FILMOPS2, FILM_OPS
from .resources import dpx_path_resource
from .loggers import log_status

defs_assess1 = Definitions(
    assets=[get_dpx_folders, dynamic_process_subfolders, dpx_assessment],
    resources={"storage_path": dpx_path_resource.configured({"dpx_path": QNAP_FILM})},
    loggers={"log_status": log_status.configured({"name": "logger_path1", "log_level": "INFO"})}
)

defs_assess2 = Definitions(
    assets=[get_dpx_folders, dynamic_process_subfolders, dpx_assessment],
    resources={"storage_path": dpx_path_resource.configured({"dpx_path": QNAP_FILMOPS})},
    loggers={"log_status": log_status.configured({"name": "logger_path2", "log_level": "INFO"})}
)

defs_assess3 = Definitions(
    assets=[get_dpx_folders, dynamic_process_subfolders, dpx_assessment],
    resources={"storage_path": dpx_path_resource.configured({"dpx_path": QNAP_FILMOPS2})},
    loggers={"log_status": log_status.configured({"name": "logger_path3", "log_level": "INFO"})}
)

defs_assess4 = Definitions(
    assets=[get_dpx_folders, dynamic_process_subfolders, dpx_assessment],
    resources={"storage_path": dpx_path_resource.configured({"dpx_path": FILM_OPS})},
    loggers={"log_status": log_status.configured({"name": "logger_path1", "log_level": "INFO"})}
)

defs = Definitions(
    assets=defs_assess1.assets + defs_assess2.assets + defs_assess3.assets + defs_assess4.assets,
    resources=defs_assess1.resources.update(defs_assess2.resources, defs_assess3.resources, defs_assess4.resources),
    loggers=defs_assess1.loggers(defs_assess2.loggers, defs_assess3.loggers, defs_assess4.loggers)
)