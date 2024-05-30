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
import logging


DPX_PATH = os.environ['QNAP_FILM']
DPX_GAP_CHECK = os.path.join(DPX_PATH, 'automation_dpx/encoding/dpx_gap_check/')
DPX_REVIEW = os.path.join(DPX_PATH, os.environ['DPX_REVIEW'])
DPX_ASSESS = os.path.join(DPX_PATH, os.environ['DPX_ASSESS'])
DPX_ASSESS_FOUR = os.path.join(DPX_PATH, os.environ['DPX_ASSESS_FOUR'])
LOG = os.path.join(DPX_PATH, os.environ['DPX_SCRIPT_LOG'], 'dpx_sequence_gap_check.log')
LOCAL_LOG = os.path.join(DPX_REVIEW, 'dpx_gaps_found.log')

# Logging config
LOGGER = logging.getLogger('dpx_sequence_gap_check_qnap_film')
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


def iterate_folders(fpath):
    '''
    Iterate suppied path and return list
    of filenames
    '''
    file_nums = []
    filenames = []
    for root, _,files in os.walk(fpath):
        for file in files:
            dpx_path = root
            if file.endswith(('.dpx', '.DPX')):

                file_nums.append(int(re.search(r'\d+', file).group()))
                filenames.append(os.path.join(root, file))
    return file_nums, filenames, dpx_path


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

    if len(folder_contents) < 2:
        return None
    if len(folder_contents) == 2:
        if 'scan' in folder_contents[0].split('/')[-1].lower() and 'x' in folder_contents[1].split('/')[-1].lower():
            sorted(folder_contents, key=len)
            return [folder_contents[-1]]
    if len(folder_contents) == 3:
        if 'x' in folder_contents[0].split('/')[-1].lower() and 'scan' in folder_contents[1].split('/')[-1].lower() and 'R' in folder_contents[2].split('/')[-1].upper():
            sorted(folder_contents, key=len)
            return [folder_contents[-1]]
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
                return None
            sorted(folder_contents, key=len)
            return folder_contents[-scan_num:]
        if (len(folder_contents) - 1) / scan_num == 2:
            # Ensure folder naming order is correct
            if 'scan' in folder_contents[0].split('/')[-1].lower() and 'R' not in folder_contents[len(folder_contents) - 1].split('/')[-1].upper():
                return None
            sorted(folder_contents, key=len)
            return folder_contents[-scan_num:]

    return None


def gaps(fpath):
    '''
    Iterate all folders in dpx_gap_check/
    Check in each folder if DPX list is shorter than min() max() range list
    If yes, report different and return missing list
    '''

    dpx_paths = count_folder_depth(fpath)
    
    # Fetch lists
    gaps = False
    for dpath in dpx_paths:
        file_nums, filenames, dpx_path = iterate_folders(dpath)

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
            for missed in missing:
                print(f"DPX number missing: {missed}")

    # Return findings
    if gaps is True:
        return True, missing, os.path.join(dpx_path, first_dpx)
    else:
        return False, None, os.path.join(dpx_path, first_dpx)
