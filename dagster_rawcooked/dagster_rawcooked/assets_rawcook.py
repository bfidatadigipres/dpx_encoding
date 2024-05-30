'''
Assets that manage RAWcooked encoding
Read sqlite db and look for 'rawcook'
items. Where found initiate encoding
and with use of sqlite update on prgress
'''

# Imports
import os
import sys
import json
import shutil
from dagster import asset, DynamicOut, DynamicOutput
from .sqlite_funcs import create_first_entry, update_table
from .dpx_rawcook import encoder
from .config import DOWNTIME, QNAP_FILM, DPX_COOK, MKV_ENCODED, LOGS


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(DOWNTIME) as control:
        j = json.load(control)
        if not j['rawcooked']:
            sys.exit("Downtime control close")


@asset
def get_dpx_folders():
    '''
    Retrieve list of DPX subfolders
    extract items partially processed
    '''
    dpx_folder = os.path.join(QNAP_FILM, DPX_COOK)
    mkv_folder = os.path.join(QNAP_FILM, MKV_ENCODED, 'mkv_cooked')

    dpx_folders = [x for x in os.listdir(dpx_folder) if os.path.isdir(os.path.join(dpx_folder, x))]
    mkv_processing = [x for x in os.listdir(mkv_folder) if x.endswith('.mkv.txt')]

    for file in mkv_processing:
        mkv = file.split('.')[0]
        if mkv in dpx_folders:
            dpx_folders.remove(mkv)

    return dpx_folders


@asset
def dynamic_process_subfolders(get_dpx_folders):
    ''' Push get_dpx_folder list to multiple assets'''
    for dpx_path in get_dpx_folders:
        dpath = os.path.join(QNAP_FILM, DPX_COOK, dpx_path)
        yield DynamicOutput(dpath, mapping_key=dpx_path)


@asset
def encoding(context, dynamic_process_subfolders):
    ''' Calling subprocess modules to run encode '''
    dpx_path = dynamic_process_subfolders
    dpx_seq = os.path.basename(dpx_path)
    log_path = os.path.join(QNAP_FILM, MKV_ENCODED, 'logs/')
    mkv_cooked = os.path.join(QNAP_FILM, MKV_ENCODED, 'mkv_cooked/')
    mkv_path = encoder(dpx_path, mkv_cooked, log_path)
    if os.path.isfile(mkv_path):
        row_id = update_table('status', dpx_seq, f'RAWcooked encoding complete.')
        if not row_id:
            context.log.warning(f"Failed to update status with 'RAWcooked encoding complete'")
    else:
        row_id = update_table('status', dpx_seq, f'Fail! DPX encoding to MKV failure.')
        if not row_id:
            context.log.warning(f"Failed to update status with 'DPX encoding to MKV failure'")
            return {"status": "encoding failure", "dpx_seq": dpx_path}
    