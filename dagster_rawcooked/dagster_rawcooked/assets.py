'''
dagster_rawcooked/assets.py
Links together all python code
for RAWcooked processing calling
in external modules where needed

Joanna White
2024
'''

import os
import json
from dagster import asset, DynamicOut, DynamicOutput
import dpx_assess
import dpx_splitting
import dpx_rawcook
import mkv_assess
import mkv_check


@asset
def get_dpx_folders():
    '''
    Retrieve list of DPX subfolders
    extract items partially processed
    '''
    folder = os.environ['QNAP_FILM']
    active = os.environ['ACTIVE_JOBS']
    dpx_folders = [x for x in os.listdir(folder) if os.path.isdir(os.path.join(folder, x))]
    with open(active, 'r') as file:
        active_jobs = json.loads(file)
    for key, value in active_jobs.items():
        if value == 'processing':
            if key in dpx_folders:
                dpx_folder.remove(key)

    return dpx_folder


@asset(
    config_schema={"dpx_folder": str},
    required_resource_keys={"dpx_path"}
)


@asset
def dpx_assessment(context):
    dpx_path = context.resources.dpx_path
    context.log.info("Processing DPX sequence {dpx_path}")
    pass


