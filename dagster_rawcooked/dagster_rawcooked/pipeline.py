from dagster import Definitions
from assess import get_dpx_folders, dynamic_process_subfolders, dpx_assessment
from .config import QNAP_FILM, QNAP_FILMOPS, QNAP_FILMOPS2, FILM_OPS
from .resources import dpx_path_resource

defs_assess1 = Definitions(
    assets=[get_dpx_folders, dynamic_process_subfolders, dpx_assessment],
    resources={
        "storage_path": dpx_path_resource.configured({"dpx_path": QNAP_FILM})
    }
)

defs_assess2 = Definitions(
    assets=[get_dpx_folders, dynamic_process_subfolders, dpx_assessment],
    resources={
        "storage_path": dpx_path_resource.configured({"dpx_path": QNAP_FILMOPS})
    }
)

defs_assess3 = Definitions(
    assets=[get_dpx_folders, dynamic_process_subfolders, dpx_assessment],
    resources={
        "storage_path": dpx_path_resource.configured({"dpx_path": QNAP_FILMOPS2})
    }
)

defs_assess4 = Definitions(
    assets=[get_dpx_folders, dynamic_process_subfolders, dpx_assessment],
    resources={
        "storage_path": dpx_path_resource.configured({"dpx_path": FILM_OPS})
    }
)

defs = Definitions(
    assets=defs_assess1.assets + defs_assess2.assets + defs_assess3.assets + defs_assess4.assets,
    resources=defs_assess1.resources.update(defs_assess2.resources, defs_assess3.resources, defs_assess4.resources)
)