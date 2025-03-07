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
import collections

DPX_PATH = os.environ['QNAP_10_DIGIOPS']
DPX_GAP_CHECK = os.path.join(DPX_PATH, 'automation_dpx/encoding/dpx_gap_check/')
DPX_REVIEW = os.path.join(DPX_PATH, os.environ['DPX_REVIEW'])
DPX_ASSESS = os.path.join(DPX_PATH, os.environ['DPX_ASSESS'])
DPX_ASSESS_FOUR = os.path.join(DPX_PATH, os.environ['DPX_ASSESS_FOUR'])
LOG = os.path.join(DPX_PATH, os.environ['DPX_SCRIPT_LOG'], 'dpx_sequence_gap_check.log')
LOCAL_LOG = os.path.join(DPX_REVIEW, 'dpx_gaps_found.log')

# Logging config
LOGGER = logging.getLogger('dpx_sequence_gap_check_qnap_10')
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


def count_folder_depth(fpath):
    '''
    Work out the depth of folders to the DPX sequence
    and ensure folders follow file naming conventions
    - This should only fail if more than one R01o01 present,
      other folders present that shouldn't be or order wrong.
    '''
    folder_contents = []
    for root, dirs, _ in os.walk(fpath):
        for directory in dirs:
            folder_contents.append(os.path.join(root, directory))

    # Check for nested folders (eg N_123_01of01/N_123_01of01/scan01)
    repeat_folders = [ x for x, count in collections.counter(folder_contents).items() if count > 1 ]
    if len(repeat_folders) > 0:
        LOGGER.info("Folder has repeating folder names, skipping: %s", repeat_folders)
        return None, None

    if len(folder_contents) < 2:
        return None, None
    if len(folder_contents) == 2:
        if 'scan' in folder_contents[0].split('/')[-1].lower() and 'x' in folder_contents[1].split('/')[-1].lower():
            sorted(folder_contents, key=len)
            return '3', [folder_contents[-1]]
    if len(folder_contents) == 3:
        if 'x' in folder_contents[0].split('/')[-1].lower() and 'scan' in folder_contents[1].split('/')[-1].lower() and 'R' in folder_contents[2].split('/')[-1].upper():
            sorted(folder_contents, key=len)
            return '4', [folder_contents[-1]]
    if len(folder_contents) > 3:
        total_scans = []
        for num in range(0, len(folder_contents)):
            if 'scan' in folder_contents[num].split('/')[-1].lower():
                total_scans.append(folder_contents[num].split('/')[-1])

        scan_num = len(total_scans)
        # Temp log monitoring of unusually high folder numbers
        LOGGER.info("DPX sequence found with excess folders:")
        for fold in folder_contents:
            LOGGER.info(fold)
        if len(folder_contents) / scan_num == 2:
            # Ensure folder naming order is correct
            if 'scan' not in folder_contents[0].split('/')[-1].lower():
                return None, None
            sorted(folder_contents, key=len)
            return '3', folder_contents[-scan_num:]
        if (len(folder_contents) - 1) / scan_num == 2:
            # Ensure folder naming order is correct
            if 'scan' in folder_contents[0].split('/')[-1].lower() and 'R' not in folder_contents[len(folder_contents) - 1].split('/')[-1].upper():
                return None, None
            sorted(folder_contents, key=len)
            return '4', folder_contents[-scan_num:]

    return None, None


def iterate_folders(fpath):
    '''
    Iterate suppied path and return list
    of filenames
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
        LOGGER.info("** --- %s ---", fpath)
        depth, dpx_paths = count_folder_depth(fpath)
        if depth is None:
            LOGGER.warning("Folder depths do not match either three or fourdepth specs")
            success = move_folder(fpath, 'review')
            if not success:
                LOGGER.warning("Failed to move DPX sequence to dpx_for_review: %s", fpath)
            continue
        log_list.append(f"----- {fpath} -----")
        log_list.append(f"Folder assessed as {depth} depth folder.")

        # Fetch lists
        gaps = False
        for dpath in dpx_paths:
            file_nums, filenames = iterate_folders(dpath)
            log_list.append(f"{len(file_nums)} DPX files found in folder {dpath}")

            # Calculate range from first/last
            file_range = [ x for x in range(min(file_nums), max(file_nums) + 1) ]

            # Retrieve equivalent DPX names for logs
            first_dpx = filenames[file_nums.index(min(file_nums))]
            last_dpx = filenames[file_nums.index(max(file_nums))]
            LOGGER.info("Total DPX files in sequence: %s", len(file_range))
            LOGGER.info("First DPX: %s", first_dpx)
            LOGGER.info("Last DPX: %s", last_dpx)

            # Check for absent numbers in sequence
            missing = list(set(file_nums) ^ set(file_range))
            print(missing)
            if len(missing) > 0:
                gaps = True
                LOGGER.warning("Missing DPX in sequence: %s", missing)
                log_list.append(f"Quantity of missing DPX files: {len(missing)}")
                for missed in missing:
                    idx = missed - 1
                    try:
                        prev_dpx = filenames[file_nums.index(idx)]
                        log_list.append(f"DPX number {missed} missing after: {prev_dpx}")
                    except ValueError:
                        log_list.append(f"DPX number missing: {missed}")

        # Move to correct path
        if gaps is True:
            LOGGER.warning("Gaps in sequence. Moving file to dpx_for_review/ folder")
            success = move_folder(fpath, 'review')
            if not success:
                log_list.append(f"FAILED TO MOVE: {fpath} to dpx_for_review path")
                log_list.append(f"Please move this file manually.\n")
        else:
            LOGGER.info("No gaps in sequence. Moving to DPX assessment folder")
            success = move_folder(fpath, depth)
            if not success:
                log_list.append(f"FAILED TO MOVE: {fpath} to DPX to assess path")
                log_list.append(f"Please move this file manually.\n")

        # Write just failed examples to log
        for log in log_list:
            local_logs(log)

    LOGGER.info("==== DPX SEQUENCE GAP CHECK END =====================")


def move_folder(fpath, arg):
    '''
    Move DPX sequence to assessment paths
    '''
    move_path = ''
    folder = os.path.split(fpath)[-1]
    if arg == '3':
        move_path = os.path.join(DPX_ASSESS, folder)
    if arg == '4':
        move_path = os.path.join(DPX_ASSESS_FOUR, folder)
    if arg == 'review':
        move_path = os.path.join(DPX_REVIEW, folder)

    if not move_path:
        LOGGER.warning("Cannot move %s", folder)
        return False

    try:
        shutil.move(fpath, move_path)
        LOGGER.info("Moved: %s to %s", fpath, move_path)
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
