#!/usr/bin/env python3

'''
dpx_sequence_gap_check.py

Script functions:
1. Iterate dpx_gap_check/ folders for DPX sequences
   counting depth of DPX sequence folder from top folder
2. Extract all numerical values from DPX filenames to
   DPX file list
3. Build two sets, one from the list and one with
   a complete number sequence
4. Compare the two and check for difference
5. If different, DPX sequence is to be moved to one
   folder and the missing DPX numbers are to be written
   to a log file
   If not different, the DPX sequence is moved to the
   correct dpx_to_assess folder path / 3 or 4 depth

Joanna White 2024
'''

import os
import re
import sys
import shutil
import logging
import datetime

DPX_PATH = os.environ['QNAP_FILMOPS']
DPX_GAP_CHECK = os.path.join(DPX_PATH, 'automation_dpx/encoding/dpx_gap_check/')
DPX_REVIEW = os.path.join(DPX_PATH, os.environ['DPX_REVIEW'])
DPX_ASSESS = os.path.join(DPX_PATH, os.environ['DPX_ASSESS'])
DPX_ASSESS_FOUR = os.path.join(DPX_PATH, os.environ['DPX_ASSESS_FOUR'])
LOG = os.path.join(DPX_PATH, os.environ['DPX_SCRIPT_LOG'], 'dpx_sequence_gap_check.log')
LOCAL_LOG = os.path.join(DPX_REVIEW, 'dpx_gaps_found.log')

# Logging config
LOGGER = logging.getLogger('dpx_sequence_gap_check')
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


def count_folder_depth(fpath):
    '''
    Work out the depth of folders to the DPX sequence
    '''
    folder_contents = [ x for x in os.listdir(fpath) if os.path.isdir(os.path.join(fpath, x)) ]
    for contents in folder_contents:
        if 'scan' in str(contents).lower():
            return '3'
    return '4'


def retrieve_dpx(fpath):
    '''
    Get lists of DPX files
    '''
    file_nums = []
    filenames = []
    for root, _,files in os.walk(fpath):
        for file in files:
            if file.endswith(('.dpx', '.DPX')):
                file_nums.append(int(re.search(r'\d+', file).group()))
                filenames.append(os.path.join(root, file))

    return file_nums, filenames


def main():
    '''
    Iterate all folders in dpx_gap_check/
    Check in each folder if DPX list is shorter than min() max() range list
    If yes, report different to log and move folder to dpx_for_review
    If identical, move folder to dpx_to_assess/ folder for folder type
    old folder formatting or new folder formatting
    '''
    log_list = []
    paths = [ x for x in os.listdir(DPX_GAP_CHECK) if os.path.isdir(os.path.join(DPX_GAP_CHECK, x)) ]
    if not paths:
        sys.exit()

    LOGGER.info("==== DPX SEQUENCE GAP CHECK START ===================")
    for pth in paths:
        fpath = os.path.join(DPX_GAP_CHECK, pth)
        log_list.append(f"----- {fpath} -----")
        LOGGER.info("%s", fpath)
        depth = count_folder_depth(fpath)
        log_list.append(f"Folder assessed as {depth} depth folder.")

        # Fetch lists
        file_nums, filenames = retrieve_dpx(fpath)
        log_list.append(f" - {len(file_nums)} DPX files found in folder {fpath}")

        # Calculate range from first/last
        file_range = [ x for x in range(min(file_nums), max(file_nums) + 1) ]

        # Retrieve equivalent DPX names for logs
        first_dpx = filenames[file_nums.index(min(file_nums))]
        last_dpx = filenames[file_nums.index(max(file_nums))]
        log_list.append(f"Total range of DPX is {len(file_range)}\nFirst: {first_dpx}\nLast: {last_dpx}")
        LOGGER.info("First DPX: %s", first_dpx)
        LOGGER.info("Last DPX: %s", last_dpx)

        # Check for absent numbers in sequence
        missing = list(set(file_nums) ^ set(file_range))
        if len(missing) == 0:
            LOGGER.info("No missing items found in DPX range: %s", missing)
            success = move_folder(fpath, depth)
        else:
            LOGGER.warning("Missing DPX in sequence: %s", missing)
            log_list.append(f"Quantity of missing DPX files: {len(missing)}")
            log_list.append(f"Missing DPX numbers in sequence: {', '.join(missing)}")
            for missed in missing:
                idx = missed - 1
                try:
                    prev_dpx = filenames[file_nums.index(idx)]
                    log_list.append(f"DPX number {missed} missing after: {prev_dpx}")
                except IndexError:
                    log_list.append(f"DPX number missing: {missed}")

            # Move to dpx_for_review
            success = move_folder(fpath, 'review')
            if not success:
                log_list.append(f"FAILED TO MOVE: {fpath}")
                log_list.append(f"Please move this file manually.\n")
            else:
                log_list.append(f"File moved to dpx_for_review/ path successfully.\n")

            # Write just failed examples to log
            for log in log_list:
                local_logs(log)

    LOGGER.info("==== DPX SEQUENCE GAP CHECK END =====================")


def move_folder(fpath, arg):
    '''
    Move DPX sequence to assessment paths
    '''
    folder = os.path.split(fpath)[-1]
    if arg == '3':
        move_path = os.path.join(DPX_ASSESS, folder)
    elif arg == '4':
        move_path = os.path.join(DPX_ASSESS_FOUR, folder)
    elif arg == 'review':
        move_path = os.path.join(DPX_REVIEW, folder)

    LOGGER.info("Moving %s to path: %s", folder, move_path)
    try:
        shutil.move(fpath, move_path)
        return True
    except Exception as err:
        LOGGER.warning("Unable to move folder %s to %s", fpath, move_path)
        print(err)
        return False


def local_logs(data):
    '''
    Output local log data for team
    to monitor TAR wrap process
    '''
    timestamp = str(datetime.datetime.now())

    with open(LOCAL_LOG, 'a') as log:
        log.write(f"{timestamp[0:19]} - {data}\n")
        log.close()


if __name__ == '__main__':
    main()
