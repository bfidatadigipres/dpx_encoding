#!/usr/bin/env python3

'''
*** THIS SCRIPT MUST RUN FROM SHELL LAUNCH SCRIPT RUNNING PARALLEL 1 JOB AT A TIME ***
Receives three sys.argv items from shell script:
KB size, path to folder, and encoding type (tar or luma_4k/rawcooked)

Script functions:
0. Checks in CID if dpx_sequence record has 'DPX' in file_type (numbered correctly)
1. Checks if dpx sequence name is 01of01
   If yes, skips on to stage 2 and 3
   If no, checks if name is in CSV and renames, then skips to stages 2 and 3
2. Division requirements of DPX sequence calculated:
   a. None, folder is under splitting size. 01of01 DPX sequences moved to encoding paths,
      all others moved to part_whole_split rawcooked or tar folders
   b. Oversize, folder is too large for splitting scripts. Moved to error path
   c. Division calculated at 2, 3, 4 or 5 splits - continue to stage 3
3. Splitting functions begin:
   a. splitting_log updates with details of DPX sequence to be split
   b. New folder names are generated for split additions, and older folder names
      have new folder part wholes calculated for them. Current folder number updated
   c. All new folder names and old folder names (where present) are updated to CSV_PATH file
   d. DPX path iterated over finding folder containing '.dpx' or '.DPX' files
      This will find all instances of scan01, scan02 etc folders within DPX path and
      ensure each is split equally and moved to new folder path
   e. New folders have new paths created, incorporating full path for located .dpx files
   f. DPX files in DPX path are counted, divided as per division, allocated to blocks
      and each block of sorted DPX stored in a list of dictionaries
   g. The new folder list and first DPX of each block data is written to splitting_log
   h. Each block is moved to its corresponding new folder, one DPX at a time using shutil.move
   i. Each block is checked against folder list to ensure all moved correctly to new folder
      If any are missing and found in original folder, a repeated move happens for
      each (in case of network drops etc).
   j. Original dpx_sequence folder is also checked against block list for first sequence
      to ensure all original files still in place, plus length checked to ensure it matches
      with total sequence split length to see if some moves failed.
4. Where a DPX sequence was 01of01 and split is successful all files are left in place
   to be moved next script pass.
5. Where a DPX sequence is any other part whole, after successful splitting they
   are also left in place to be moved to part_whole_split tar/rawcooked folders to
   next pass to assess and relocate in time.

Joanna White 2021
'''

# Global packages
import os
import sys
import csv
import json
import shutil
import logging
import datetime
import requests

# Global variables
DPX_PATH = os.environ['QNAP_FILMOPS']
ERRORS = os.path.join(DPX_PATH, os.environ['CURRENT_ERRORS'])
OVERSIZED_SEQ = os.path.join(ERRORS, 'oversized_sequences/')
SCRIPT_LOG = os.path.join(DPX_PATH, os.environ['DPX_SCRIPT_LOG'])
CSV_PATH = os.path.join(SCRIPT_LOG, 'splitting_document.csv')
PART_WHOLE_LOG = os.path.join(ERRORS, 'part_whole_search.log')
SPLITTING_LOG = os.path.join(SCRIPT_LOG, 'DPX_splitting.log')
DPX_REVIEW = os.path.join(DPX_PATH, os.environ['DPX_REVIEW'])
TAR_PATH = os.path.join(DPX_PATH, os.environ['DPX_WRAP'])
RAWCOOKED_PATH = os.path.join(DPX_PATH, os.environ['DPX_COOK'])
PART_RAWCOOK = os.path.join(DPX_PATH, os.environ['PART_RAWCOOK'])
PART_TAR = os.path.join(DPX_PATH, os.environ['PART_TAR'])
CONTROL_JSON = os.path.join(os.environ['LOG_PATH'], 'downtime_control.json')
TODAY = str(datetime.datetime.now())[:10]
CID_API = os.environ['CID_API3']

# Setup logging
LOGGER = logging.getLogger('dpx_splitting_script.log')
HDLR = logging.FileHandler(os.path.join(SCRIPT_LOG, 'dpx_splitting_script.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def check_control():
    '''
    Check control json for downtime requests
    '''
    with open(CONTROL_JSON) as control:
        j = json.load(control)
        if not j['rawcooked']:
            LOGGER.info('Script run prevented by downtime_control.json. Script exiting.')
            sys.exit('Script run prevented by downtime_control.json. Script exiting.')


def get_cid_data(dpx_sequence):
    '''
    Use requests to retrieve priref for associated item object number
    '''
    ob_num_split = dpx_sequence.split('_')
    ob_num = '-'.join(ob_num_split[0:-1])
    search = f"object_number='{ob_num}'"
    query = {'database': 'items',
             'search': search,
             'output': 'json'}
    results = requests.get(CID_API, params=query)
    results = results.json()
    try:
        priref = results['adlibJSON']['recordList']['record'][0]['@attributes']['priref']
    except (IndexError, KeyError):
        priref = ''
    try:
        file_type = results['adlibJSON']['recordList']['record'][0]['file_type'][0]
    except (IndexError, KeyError):
        file_type = ''

    return (priref, file_type)


def get_utb(priref):
    '''
    Use requests to retrieve UTB data for associated priref
    '''
    search = f"priref='{priref}'"
    query = {'database': 'items',
             'search': search,
             'output': 'json'}
    results = requests.get(CID_API, params=query)
    results = results.json()
    try:
        utb_content = results['adlibJSON']['recordList']['record'][0]['utb'][0]['utb.content']
    except (IndexError, KeyError):
        utb_content = ['']

    return utb_content


def read_csv(dpx_sequence):
    '''
    Does fname entry exist in CSV, if yes retrieve latest sequence
    number for that entry and return
    '''
    number_present = True
    new_sequence = dpx_sequence
    with open(CSV_PATH, newline='') as fname:
        readme = csv.DictReader(fname)

        while number_present is True:
            for row in readme:
                orig_num = row['original']
                if str(orig_num) == str(new_sequence):
                    new_sequence = row['new_number']
                else:
                    number_present = False
    if new_sequence == dpx_sequence:
        return ''
    else:
        return new_sequence


def folder_depth(fullpath):
    '''
    Check if folder is three depth of four depth
    across total scan folder contents
    '''
    scan_folders = [x for x in os.listdir(fullpath) if os.path.isdir(os.path.join(fullpath, x))]
    scan_num = len(scan_folders)
    folders = 0

    for _, dirnames, _ in os.walk(fullpath):
        folders += len(dirnames)

    if (folders / scan_num) == 2 or (folders / scan_num) == 3:
        return True
    else:
        return False


def count_files(dirpath, division):
    '''
    Counts total DPX files in supplied sorted folder and returns the division totals
    Plus returns list of dictionary blocks of dpx numbers for move/checks
    '''
    block_list = []
    dpx_list = []
    dpx_sequence = [name for name in os.listdir(dirpath) if name.endswith(('.dpx', '.DPX'))]
    file_count = len(dpx_sequence)
    dpx_sequence.sort()

    if division == '2':
        cuts = int(file_count) // 2
        dpx_block1 = dpx_sequence[0]
        start_num = cuts
        dpx_block2 = dpx_sequence[start_num]
        block_list = [file_count, cuts, dpx_block1, dpx_block2]
        dpx_list.append({'block1': dpx_sequence[:start_num]})
        dpx_list.append({'block2': dpx_sequence[start_num:]})

    elif division == '3':
        cuts = int(file_count) // 3
        dpx_block1 = dpx_sequence[0]
        start_block3 = (cuts * 2)
        dpx_block2 = dpx_sequence[cuts]
        dpx_block3 = dpx_sequence[start_block3]
        block_list = [file_count, cuts, dpx_block1, dpx_block2, dpx_block3]
        dpx_list.append({'block1': dpx_sequence[:cuts]})
        dpx_list.append({'block2': dpx_sequence[cuts:start_block3]})
        dpx_list.append({'block3': dpx_sequence[start_block3:]})

    elif division == '4':
        cuts = int(file_count) // 4
        dpx_block1 = dpx_sequence[0]
        start_block2 = cuts
        start_block3 = (cuts * 2)
        start_block4 = (cuts * 3)
        dpx_block2 = dpx_sequence[start_block2]
        dpx_block3 = dpx_sequence[start_block3]
        dpx_block4 = dpx_sequence[start_block4]
        block_list = [file_count, cuts, dpx_block1, dpx_block2, dpx_block3, dpx_block4]
        dpx_list.append({'block1': dpx_sequence[:start_block2]})
        dpx_list.append({'block2': dpx_sequence[start_block2:start_block3]})
        dpx_list.append({'block3': dpx_sequence[start_block3:start_block4]})
        dpx_list.append({'block4': dpx_sequence[start_block4:]})

    elif division == '5':
        cuts = int(file_count) // 5
        dpx_block1 = dpx_sequence[0]
        start_block2 = cuts
        start_block3 = (cuts * 2)
        start_block4 = (cuts * 3)
        start_block5 = (cuts * 4)
        dpx_block2 = dpx_sequence[start_block2]
        dpx_block3 = dpx_sequence[start_block3]
        dpx_block4 = dpx_sequence[start_block4]
        dpx_block5 = dpx_sequence[start_block5]
        block_list = [file_count, cuts, dpx_block1, dpx_block2, dpx_block3, dpx_block4, dpx_block5]
        dpx_list.append({'block1': dpx_sequence[:start_block2]})
        dpx_list.append({'block2': dpx_sequence[start_block2:start_block3]})
        dpx_list.append({'block3': dpx_sequence[start_block3:start_block4]})
        dpx_list.append({'block4': dpx_sequence[start_block4:start_block5]})
        dpx_list.append({'block5': dpx_sequence[start_block5:]})

    elif division == '6':
        cuts = int(file_count) // 6
        dpx_block1 = dpx_sequence[0]
        start_block2 = cuts
        start_block3 = (cuts * 2)
        start_block4 = (cuts * 3)
        start_block5 = (cuts * 4)
        start_block6 = (cuts * 5)
        dpx_block2 = dpx_sequence[start_block2]
        dpx_block3 = dpx_sequence[start_block3]
        dpx_block4 = dpx_sequence[start_block4]
        dpx_block5 = dpx_sequence[start_block5]
        dpx_block6 = dpx_sequence[start_block6]
        block_list = [file_count, cuts, dpx_block1, dpx_block2, dpx_block3, dpx_block4, dpx_block5, dpx_block6]
        dpx_list.append({'block1': dpx_sequence[:start_block2]})
        dpx_list.append({'block2': dpx_sequence[start_block2:start_block3]})
        dpx_list.append({'block3': dpx_sequence[start_block3:start_block4]})
        dpx_list.append({'block4': dpx_sequence[start_block4:start_block5]})
        dpx_list.append({'block5': dpx_sequence[start_block5:start_block6]})
        dpx_list.append({'block6': dpx_sequence[start_block6:]})

    return (block_list, dpx_list)


def fname_split(fname):
    '''
    Receive a filename extract part whole from end
    Return items split up
    '''
    fname = fname.rstrip('/')
    name_split = fname.split('_')
    part_whole = name_split[-1:][0]
    if 'of' in str(part_whole):
        part, whole = part_whole.split('of')
    else:
        return None
    if len(name_split) == 3:
        return (f"{name_split[0]}_{name_split[1]}_", part, whole)
    if len(name_split) == 4:
        return (f"{name_split[0]}_{name_split[1]}_{name_split[2]}_", part, whole)


def workout_division(arg, kb_size):
    '''
    Pass encoding argument and which is passed from shell launcher script
    Kilobyte calculated as byte = 1024
    '''
    division = ''
    kb_size = int(kb_size)

    # Size calculation for rawcooked RGB encodings (now 1.3TB increments, upto 6.5TB)
    if 'rawcooked' in arg:
        if kb_size <= 1395864370:
            division = None
        elif 1395864370 <= kb_size <= 2791728740:
            division = '2'
        elif 2791728740 <= kb_size <= 4187593110:
            division = '3'
        elif 4187593110 <= kb_size <= 5583457480:
            division = '4'
        elif 5583457480 <= kb_size <= 6979321850:
            division = '5'
        elif kb_size > 6979321850:
            LOGGER.warning("workout_division(): RAWcooked file is too large for DPX splitting: %s KB", kb_size)
            division = 'oversize'

    # Size calculation for luma_4k or tar encoding sizes
    elif 'tar' in arg or 'luma_4k' in arg:
        if kb_size <= 1073741823:
            division = None
        elif 1073741824 <= kb_size <= 2147483648:
            division = '2'
        elif 2147483649 <= kb_size <= 3221225472:
            division = '3'
        elif 3221225473 <= kb_size <= 4294967296:
            division = '4'
        elif 4294967297 <= kb_size <= 5368709120:
            division = '5'
        elif 5368709120 <= kb_size <= 6442450944:
            division = '6'
        elif kb_size > 6442450944:
            LOGGER.warning("workout_division(): TAR, Luma Y or 4K file is too large for DPX splitting: %s KB", kb_size)
            division = 'oversize'

    return division


def return_range_prior(dpx_sequence, division):
    '''
    Receive file being processed, extract part whole data
    create all fnames that precede in that same range for update to CSV
    '''
    fname, part, whole = fname_split(dpx_sequence)
    part = int(part)
    whole = int(whole)
    division = int(division) - 1
    whole_count = whole + division
    change_list = []

    # Create new numbered files
    for count in range(1, whole_count + 1):
        new_name = fname + str(count).zfill(2) + 'of' + str(whole_count).zfill(2)
        old_name = fname + str(count).zfill(2) + 'of' + str(whole).zfill(2)
        # output old_name / new_name to CSV
        change_list.append({old_name: new_name})

    return change_list[:part - 1]


def folder_update_creation(dpx_sequence, division, root_path):
    '''
    Take DPX path and rename/create new folders based on division
    Needs refactor for WET issues
    '''
    fname, part, whole = fname_split(dpx_sequence)
    part = int(part)
    whole = int(whole)
    change_list = []
    folder1 = folder2 = folder3 = folder4 = folder5 = ''

    if division == '2':
        whole += 1
        dpx_seq_renumber = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        change_list.append({dpx_sequence: dpx_seq_renumber})
        change_list.append({'New folder': dpx_seq_new_folder})
        folder1 = os.path.join(root_path, dpx_seq_new_folder)

    if division == '3':
        whole += 2
        dpx_seq_renumber = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder1 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder2 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        change_list.append({dpx_sequence: dpx_seq_renumber})
        change_list.append({'New folder': dpx_seq_new_folder1})
        change_list.append({'New folder': dpx_seq_new_folder2})
        folder1 = os.path.join(root_path, dpx_seq_new_folder1)
        folder2 = os.path.join(root_path, dpx_seq_new_folder2)

    if division == '4':
        whole += 3
        dpx_seq_renumber = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder1 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder2 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder3 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        change_list.append({dpx_sequence: dpx_seq_renumber})
        change_list.append({'New folder': dpx_seq_new_folder1})
        change_list.append({'New folder': dpx_seq_new_folder2})
        change_list.append({'New folder': dpx_seq_new_folder3})
        folder1 = os.path.join(root_path, dpx_seq_new_folder1)
        folder2 = os.path.join(root_path, dpx_seq_new_folder2)
        folder3 = os.path.join(root_path, dpx_seq_new_folder3)

    if division == '5':
        whole += 4
        dpx_seq_renumber = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder1 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder2 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder3 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder4 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        change_list.append({dpx_sequence: dpx_seq_renumber})
        change_list.append({'New folder': dpx_seq_new_folder1})
        change_list.append({'New folder': dpx_seq_new_folder2})
        change_list.append({'New folder': dpx_seq_new_folder3})
        change_list.append({'New folder': dpx_seq_new_folder4})
        folder1 = os.path.join(root_path, dpx_seq_new_folder1)
        folder2 = os.path.join(root_path, dpx_seq_new_folder2)
        folder3 = os.path.join(root_path, dpx_seq_new_folder3)
        folder4 = os.path.join(root_path, dpx_seq_new_folder4)

    if division == '6':
        whole += 5
        dpx_seq_renumber = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder1 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder2 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder3 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder4 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        part += 1
        dpx_seq_new_folder5 = fname + str(part).zfill(2) + 'of' + str(whole).zfill(2)
        change_list.append({dpx_sequence: dpx_seq_renumber})
        change_list.append({'New folder': dpx_seq_new_folder1})
        change_list.append({'New folder': dpx_seq_new_folder2})
        change_list.append({'New folder': dpx_seq_new_folder3})
        change_list.append({'New folder': dpx_seq_new_folder4})
        change_list.append({'New folder': dpx_seq_new_folder5})
        folder1 = os.path.join(root_path, dpx_seq_new_folder1)
        folder2 = os.path.join(root_path, dpx_seq_new_folder2)
        folder3 = os.path.join(root_path, dpx_seq_new_folder3)
        folder4 = os.path.join(root_path, dpx_seq_new_folder4)
        folder5 = os.path.join(root_path, dpx_seq_new_folder5)

    return (change_list, folder1, folder2, folder3, folder4, folder5)


def return_range_following(dpx_sequence, division):
    '''
    Receive file being processed, extract part whole data
    create all fnames that follow in that same range for update to CSV
    '''
    fname, part, whole = fname_split(dpx_sequence)
    part = int(part)
    whole = int(whole)
    division = int(division) - 1
    part_count = part + division
    whole_count = whole + division
    change_list = []

    # Create new numbered files
    for count in range(part_count + 1, whole_count + 1):
        new_name = fname + str(count).zfill(2) + 'of' + str(whole_count).zfill(2)
        count -= division
        old_name = fname + str(count).zfill(2) + 'of' + str(whole).zfill(2)
        # output old_name / new_name to list dict
        change_list.append({old_name: new_name})

    return change_list


def main():
    '''
    Receives sys.argv items from launching script
    Checks folder against CSV_PATH file. Updates if found in original column
    Checks for divisions necessary. None, moves to encoding path. Oversize moves to errors.
    If divisions necessary takes steps necessary to subdivide large sequence into sub-folders
    so each folder total file size is beneath 1TB/1.4TB.
    '''
    check_control()

    if len(sys.argv) < 2:
        LOGGER.warning("SCRIPT NOT STARTING: Error with shell script input:\n %s", sys.argv)
        sys.exit()
    else:
        LOGGER.info("================== START Python3 DPX splitting script START ==================")
        data = sys.argv[1]
        LOGGER.info("Received data: %s", data)
        data = data.split(',\ ')
        if len(data) == 1:
            data = data.split(', ')
        kb_size = int(data[0])
        dpx_path = str(data[1])
        encoding = str(data[2])
        dpx_path = dpx_path.rstrip('/')
        dpx_sequence = os.path.basename(dpx_path)
        priref, file_type = get_cid_data(dpx_sequence)

        # Sequence CID Item record check
        if 'dpx' in file_type.lower():
            LOGGER.info("Processing DPX sequence: %s", dpx_sequence)
        else:
            LOGGER.warning("CID record does not have File Type DPX - priref: %s", priref)
            LOGGER.warning("DPX sequence being moved to dpx_for_review/ folder for further inspection")
            splitting_log(f"DPX not found in file_type of CID Item record for this sequence: {priref}")
            splitting_log(f"Moving DPX sequence {dpx_sequence} to 'dpx_for_review/' folder")
            shutil.move(dpx_path, os.path.join(DPX_REVIEW, dpx_sequence))
            error_mssg1 = "DPX not matched to file_type of CID item record. Moving DPX sequence to dpx_for_review/ folder."
            error_mssg2 = "if you are unable to update the file_type to 'DPX' in the CID item record"
            error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
            sys.exit()
        # Filename format correct
        split_name = dpx_sequence.split('_')
        if len(split_name[-1]) != 6 or 'of' not in str(split_name[-1]):
            LOGGER.warning("Part whole has incorrect formatting, moving folder to dpx_to_review for further inspection")
            splitting_log(f"DPX sequence number's part whole is incorrectly formatted: {dpx_sequence}")
            splitting_log(f"Moving DPX sequence {dpx_sequence} to 'dpx_for_review/' folder")
            shutil.move(dpx_path, os.path.join(DPX_REVIEW, dpx_sequence))
            error_mssg1 = "DPX sequence part whole is incorrectly formatted. Moving DPX sequence to dpx_for_review/ folder."
            error_mssg2 = None
            error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
            sys.exit()
        # Check folder depths accurate for three/four depth folders
        # This only works for single scan folders, needs alternative that checks from second folder
        check_depth = folder_depth(dpx_path)
        if not check_depth:
            LOGGER.warning("Incorrect internal folder structure. Cannot split. Moving to dpx_for_review/")
            splitting_log("WARNING! Incorrect internal folder structures. Cannot split this folder")
            splitting_log(f"Moving DPX sequence {dpx_sequence} to 'dpx_for_review/' folder")
            shutil.move(dpx_path, os.path.join(DPX_REVIEW, dpx_sequence))
            error_mssg1 = "DPX internal folder structure is incorrect and cannot be split safely. Moving DPX sequence to dpx_for_review/ folder."
            error_mssg2 = "if you are unable to adjust the internal folder structure to meet BFI policy"
            error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
            sys.exit()

        # Separate singleton files from part wholes
        if dpx_sequence.endswith('_01of01'):
            singleton = True
        else:
            new_num = read_csv(dpx_sequence)
            singleton = False

            # Renumber part whole folder, update dpx_path / dpx_sequence
            if len(new_num) == 0:
                LOGGER.info("Sequence %s not found in CSV so proceeding with processing:\n%s", dpx_sequence, dpx_path)
            elif len(new_num) > 0:
                try:
                    new_path = renumber(dpx_path, new_num)
                    LOGGER.info("Folder %s successfully renamed %s from CSV", dpx_sequence, new_num)
                    LOGGER.info("DPX sequence path %s will be reconfigured to %s", dpx_path, new_path)
                    splitting_log("\n*** DPX sequence found in CSV and needs renumbering")
                    splitting_log(f"DPX sequence path {dpx_path} being renamed to {new_path}")
                    dpx_path = new_path
                    dpx_sequence = new_num
                except Exception as err:
                    LOGGER.warning("Renumbering failed, exiting script to avoid processing incorrect files\n%s", err)
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    error_mssg1 = "DPX folder renumbering failed. Script exited to avoid incorrect actions against file."
                    error_mssg2 = "to inform of this renumbering error"
                    error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
                    sys.exit()

        # Does this sequence need splitting?
        division = workout_division(encoding, kb_size)
        # Name preparations for folder splitting
        path_split = os.path.split(dpx_path)
        root_path = path_split[0]

        # No division needed, sequence is below 1.3TB/1TB and folder is 01of01
        if (division is None and singleton):
            LOGGER.info("No splitting necessary for: %s\nMoving to encoding path for %s", dpx_path, encoding)
            if ('rawcooked' in encoding or 'luma_4k' in encoding):
                LOGGER.info("Moving DPX sequence to RAWcooked path: %s", dpx_sequence)
                try:
                    shutil.move(dpx_path, RAWCOOKED_PATH)
                    LOGGER.info("Move %s to RAWcooked encoding path: %s", dpx_sequence, RAWCOOKED_PATH)
                    LOGGER.info("Script exiting")
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    sys.exit()
                except Exception as err:
                    LOGGER.warning("Unable to move folder to RAWcooked path: %s\n%s", dpx_path, err)
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    error_mssg1 = f"DPX could not be moved to RAWcooked path: {RAWCOOKED_PATH}."
                    error_mssg2 = "and inform of this failure to move the folder"
                    error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
                    sys.exit()
            elif 'tar' in encoding:
                LOGGER.info("Folder %s is not oversized.\nMoving sequence to TAR path", dpx_path)
                try:
                    shutil.move(dpx_path, TAR_PATH)
                    LOGGER.info("Move completed to TAR encoding path: %s", dpx_path)
                    LOGGER.info("Script exiting")
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    sys.exit()
                except Exception as err:
                    LOGGER.warning("Unable to move folder to TAR path: %s\n%s", dpx_path, err)
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    error_mssg1 = f"DPX could not be moved to TAR path: {TAR_PATH}."
                    error_mssg2 = "and inform of this failure to move the folder"
                    error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
                    sys.exit()

        # No splitting, but item is part whole and needs processing by part_whole_move.py
        elif (division is None and not singleton):
            LOGGER.info("No splitting necessary for: %s\nMoving to part_whole_split path", dpx_path)
            if ('rawcooked' in encoding or 'luma_4k' in encoding):
                LOGGER.info("Moving DPX sequence to part_whole_split/rawcooked path: %s", dpx_sequence)
                try:
                    shutil.move(dpx_path, PART_RAWCOOK)
                    LOGGER.info("Move completed to part_whole_split/rawcooked path: %s", dpx_sequence)
                    LOGGER.info("Script exiting")
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    sys.exit()
                except Exception as err:
                    LOGGER.warning("Unable to move folder to part_whole_split path: %s\n%s", dpx_path, err)
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    error_mssg1 = f"DPX could not be moved to part_whole_split RAWcooked path: {PART_RAWCOOK}."
                    error_mssg2 = "and inform of this failure to move the folder"
                    error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
                    sys.exit()
            elif 'tar' in encoding:
                LOGGER.info("Folder %s is not oversized.\nMoving DPX sequence to part_whole_split path", dpx_path)
                try:
                    shutil.move(dpx_path, PART_TAR)
                    LOGGER.info("Move completed to part_whole_split/tar encoding path: %s", dpx_path)
                    LOGGER.info("Script exiting")
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    sys.exit()
                except Exception as err:
                    LOGGER.warning("Unable to move folder to part_whole_split path: %s\n%s", dpx_path, err)
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    error_mssg1 = f"DPX could not be moved to part_whole_split TAR path: {PART_TAR}."
                    error_mssg2 = "and inform of this failure to move the folder"
                    error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
                    sys.exit()

        # Folder is larger than 6TB (TAR/Luma/4K) / 6.5TB (RAWcooked) script exit
        elif 'oversize' in division:
            LOGGER.warning("OVERSIZE FOLDER: Too large for splitting script %s", dpx_path)
            splitting_log(f"OVERSIZE FOLDER: {dpx_sequence}. Moving to current_errors/oversized_sequence/ folder")
            LOGGER.info("Moving oversized folder %s to current_errors/oversized_sequence folder")
            LOGGER.info("Adding {} sequence number to part_whole log in current_errors/ folder")
            part_whole_log(dpx_sequence)
            oversize_path = os.path.join(OVERSIZED_SEQ, dpx_sequence)
            error_mssg1 = f"RAWcooked DPX too large for ingest to DPI - size {file_stats.st_size} bytes\n\t{dpx_path}\n\tDPX sequence moved to oversized_sequences folder."
            error_mssg2 = "as this file is too large for ingest and will need repeat splitting"
            error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
            try:
                shutil.move(dpx_path, oversize_path)
            except Exception as err:
                LOGGER.warning("Unable to move %s to oversized_sequence/ folder\n%s", dpx_sequence, err)
            LOGGER.warning("Script will exit, manual intervention needed for this file")
            LOGGER.critical("========= SCRIPT EXIT - MANUAL ASSISTANCE NEEDED =========")
            sys.exit()

        # Folder requires splitting activities
        else:
            cid_data = []
            LOGGER.info("Splitting folders with division %s necessary for: %s", division, dpx_path)
            splitting_log(f"\n------ {dpx_sequence} SPLIT START ------ {str(datetime.datetime.now())}")
            splitting_log(f"NEW FOLDER FOUND THAT REQUIRES SPLITTING:\n{dpx_path}")
            splitting_log(f"DPX sequence encoding <{encoding}> is {kb_size} KB in size, requiring {division} divisions\n")
            cid_data.append(f"DPX folder required splitting: {dpx_sequence}")
            cid_data.append(f"DPX folder size: {kb_size} kb required {division} divisions\n")
            LOGGER.info("Adding %s sequence number to part_whole log in current_errors/ folder", dpx_sequence)
            part_whole_log(dpx_sequence)
            # Generate new folder names from dpx_sequence/division
            pre_foldername_list = []
            post_foldername_list = []
            foldername_list_new = []
            data = []
            folder1 = folder2 = folder3 = folder4 = folder5 = ''
            pre_foldername_list = return_range_prior(dpx_sequence, division)
            data = folder_update_creation(dpx_sequence, division, root_path)
            foldername_list_new = data[0]
            folder1 = data[1]
            if data[2]:
                folder2 = data[2]
            if data[3]:
                folder3 = data[3]
            if data[4]:
                folder4 = data[4]
            if data[5]:
                folder5 = data[5]
            post_foldername_list = return_range_following(dpx_sequence, division)
            # Append all new numbers to CSV
            splitting_log("New folder numbers:")
            cid_data.append("New folder numbers:")
            if len(pre_foldername_list) > 0:
                for dic in pre_foldername_list:
                    for key, val in dic.items():
                        write_csv(key, val)
                        splitting_log(f"{key} will be renamed {val}")
                        cid_data.append(f"{key} has been renamed {val}")
            for dic in foldername_list_new:
                for key, val in dic.items():
                    if dpx_sequence in key:
                        # Extract new sequence number and change dpx_path / dpx_sequence
                        new_dpx_sequence = val
                        new_path = renumber(dpx_path, new_dpx_sequence)
                        dpx_sequence = new_dpx_sequence
                        dpx_path = new_path
                        LOGGER.info("New sequence number retrieved for this DPX sequence: %s", dpx_sequence)
                        LOGGER.info("DPX path updated: %s", dpx_path)
                for key, val in dic.items():
                    write_csv(key, val)
                    splitting_log(f"{key} will be renamed {val}")
                    cid_data.append(f"{key} has been renamed {val}")
            if len(post_foldername_list) > 0:
                for dic in post_foldername_list:
                    for key, val in dic.items():
                        write_csv(key, val)
                        splitting_log(f"{key} will be renamed {val}")
                        cid_data.append(f"{key} has been renamed {val}")

            # Find path for all scan folders containing DPX files
            new_folder1 = new_folder2 = new_folder3 = new_folder4 = new_folder5 = ''
            for root, dirs, files in os.walk(dpx_path):
                for file in files:
                    if file.endswith((".dpx", ".DPX")):
                        folder_paths = root.split(f"{dpx_sequence}")
                        LOGGER.info("*** Folder path for splitting %s", root)
                        splitting_log(f"\n*** Making new folders with new sequence names for path: {root}")
                        cid_data.append(f"\nNew division folders created using: {dpx_sequence}{folder_paths[1]}")
                        new_folder1 = os.path.join(folder1, os.path.relpath(root, dpx_path))
                        make_dirs(new_folder1)
                        if folder2:
                            new_folder2 = os.path.join(folder2, os.path.relpath(root, dpx_path))
                            make_dirs(new_folder2)
                        if folder3:
                            new_folder3 = os.path.join(folder3, os.path.relpath(root, dpx_path))
                            make_dirs(new_folder3)
                        if folder4:
                            new_folder4 = os.path.join(folder4, os.path.relpath(root, dpx_path))
                            make_dirs(new_folder4)
                        if folder5:
                            new_folder5 = os.path.join(folder5, os.path.relpath(root, dpx_path))
                            make_dirs(new_folder5)

                        # Obtain: file_count, cuts, dpx_block data
                        LOGGER.info("Folder %s will be divided into %s divisions now", dpx_sequence, division)
                        block_data = count_files(root, division)
                        block_list = block_data[0]
                        cid_data.append(f"Total DPX in sequence: {block_list[0]}, DPX per new division: {block_list[1]}\n")
                        # Output data to splitting log
                        splitting_log(f"\nFolder {dpx_sequence} contains {block_list[0]} DPX items. Requires {division} divisions, {block_list[1]} items per folder")
                        splitting_log(f"First DPX number is {block_list[2]} remaining in original folder: {dpx_sequence}{folder_paths[1]}")
                        splitting_log(f"First DPX number is {block_list[3]} for new folder: {new_folder1}")
                        cid_data.append(f"First DPX number is {block_list[2]} remaining in original folder: {dpx_sequence}{folder_paths[1]}")
                        cid_data.append(f"First DPX number is {block_list[3]} for new folder: {new_folder1}")
                        if folder2:
                            splitting_log(f"First DPX number is {block_list[4]} for new folder: {new_folder2}")
                            cid_data.append(f"First DPX number is {block_list[4]} for new folder: {new_folder2}")
                        if folder3:
                            splitting_log(f"First DPX number is {block_list[5]} for new folder: {new_folder3}")
                            cid_data.append(f"First DPX number is {block_list[5]} for new folder: {new_folder3}")
                        if folder4:
                            splitting_log(f"First DPX number is {block_list[6]} for new folder: {new_folder4}")
                            cid_data.append(f"First DPX number is {block_list[6]} for new folder: {new_folder4}")
                        if folder5:
                            splitting_log(f"First DPX number is {block_list[7]} for new folder: {new_folder5}")
                            cid_data.append(f"First DPX number is {block_list[7]} for new folder: {new_folder5}")

                        LOGGER.info("Block data being calculated and DPX moved to final destinations")
                        dpx_list = block_data[1]
                        for dictionary in dpx_list:
                            for key, val in dictionary.items():

                                if 'block2' in key:
                                    splitting_log(f"\nMoving block 2 DPX data to {new_folder1}")
                                    cid_data.append(f"\nMoving block 2 DPX data to {new_folder1}")
                                    for dpx in val:
                                        dpx_to_move = os.path.join(root, dpx)
                                        shutil.move(dpx_to_move, new_folder1)
                                    # Check move function
                                    missing_list = move_check(val, new_folder1)
                                    if len(missing_list) > 0:
                                        move_retry(missing_list, root, new_folder1)
                                    else:
                                        splitting_log(f"All DPX files copied and checked in {folder1}")

                                if 'block3' in key:
                                    splitting_log(f"Moving block 3 DPX data to {new_folder2}")
                                    cid_data.append(f"Moving block 3 DPX data to {new_folder2}")
                                    for dpx in val:
                                        dpx_to_move = os.path.join(root, dpx)
                                        shutil.move(dpx_to_move, new_folder2)
                                    # Check move function
                                    missing_list = move_check(val, new_folder2)
                                    if len(missing_list) > 0:
                                        move_retry(missing_list, root, new_folder2)
                                    else:
                                        splitting_log(f"All DPX files copied and checked in {folder2}")

                                if 'block4' in key:
                                    splitting_log(f"Moving block 4 DPX data to {new_folder3}")
                                    cid_data.append(f"Moving block 4 DPX data to {new_folder3}")
                                    for dpx in val:
                                        dpx_to_move = os.path.join(root, dpx)
                                        shutil.move(dpx_to_move, new_folder3)
                                    # Check move function
                                    missing_list = move_check(val, new_folder3)
                                    if len(missing_list) > 0:
                                        move_retry(missing_list, root, new_folder3)
                                    else:
                                        splitting_log(f"All DPX files copied and checked in {folder3}")

                                if 'block5' in key:
                                    splitting_log(f"Moving block 5 DPX data to {new_folder4}")
                                    cid_data.append(f"Moving block 5 DPX data to {new_folder4}")
                                    for dpx in val:
                                        dpx_to_move = os.path.join(root, dpx)
                                        shutil.move(dpx_to_move, new_folder4)
                                    # Check move function
                                    missing_list = move_check(val, new_folder4)
                                    if len(missing_list) > 0:
                                        move_retry(missing_list, root, new_folder4)
                                    else:
                                        splitting_log(f"All DPX files copied and checked in {folder4}")

                                if 'block6' in key:
                                    splitting_log(f"Moving block 6 DPX data to {new_folder5}")
                                    cid_data.append(f"Moving block 6 DPX data to {new_folder5}")
                                    for dpx in val:
                                        dpx_to_move = os.path.join(root, dpx)
                                        shutil.move(dpx_to_move, new_folder5)
                                    # Check move function
                                    missing_list = move_check(val, new_folder5)
                                    if len(missing_list) > 0:
                                        move_retry(missing_list, root, new_folder5)
                                    else:
                                        splitting_log(f"All DPX files copied and checked in {folder5}")

                        for dictionary in dpx_list:
                            for key, val in dictionary.items():
                                if 'block1' in key:
                                    # Check first sequence has correct remaining files from block list
                                    missing_list = move_check(val, root)
                                    if len(missing_list) > 0:
                                        LOGGER.warning("DPX missing from original sequence %s", missing_list)
                                        error_mssg1 = "DPX items are missing from original sequence following move."
                                        error_mssg2 = "and inform of this failure with the DPX movements"
                                        error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
                                    else:
                                        splitting_log(f"All DPX files checked and remain in place in original folder {dpx_sequence}")
                                    # Check correct total of files in first sequence, no stragglers
                                    length_check = len([f for f in os.listdir(root) if f.endswith(('.dpx', '.DPX'))])
                                    if int(length_check) == int(block_list[1]):
                                        LOGGER.info("Correct number of DPX remain in original sequence %s", root)
                                    else:
                                        LOGGER.warning("Incorrect number of DPX files remain in original sequence: %s", root)
                                        LOGGER.warning("There are %s files in folder, when there should be %s DPX files", int(block_list[1]), length_check)
                                        error_mssg1 = f"Incorrect number of DPX files remain in original sequence folder: {root}."
                                        error_mssg2 = "and inform of this failure with the DPX movements"
                                        error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
                        cid_data.append(f"\n------ {dpx_sequence}{folder_paths[1]} SPLIT COMPLETE ------\n")
                        break

            # Update splitting data in text file to each new folder for encoding
            cid_data_string = '\n'.join(cid_data)
            make_text_file(cid_data_string, dpx_path)
            make_text_file(cid_data_string, folder1)
            if folder2:
                make_text_file(cid_data_string, folder2)
            if folder3:
                make_text_file(cid_data_string, folder3)
            if folder4:
                make_text_file(cid_data_string, folder4)
            if folder5:
                make_text_file(cid_data_string, folder5)

            # Update splitting data to CID item record UTB.content (temporary)
            LOGGER.info("Updating split information to CID Item record")
            utb_content = get_utb(priref)
            old_payload = utb_content[0].replace('\r\n', '\n')
            success = record_append(priref, cid_data_string, old_payload)
            if success:
                LOGGER.info("CID splitting data appended to priref %s", priref)
                splitting_log(f"Splitting data written to CID priref {priref}")
            else:
                LOGGER.warning("FAIL: CID Splitting data not appended to priref %s", priref)
                splitting_log(f"*** Failed to write data to CID. Please manually append above to priref: {priref}")
                error_mssg1 = f"Failed to write splitting data to CID item record: {priref}"
                error_mssg2 = "and inform of this failure to write the splitting data to CID item record"
                error_log(os.path.join(ERRORS, f"{dpx_sequence}_errors.log"), error_mssg1, error_mssg2)
            splitting_log(f"\n------ SPLIT END {dpx_sequence} ------ {str(datetime.datetime.now())}")
            LOGGER.info("Splitting completed for path: %s", root)

    LOGGER.info("==================== END Python3 DPX splitting script END ====================")


def move_check(dpx_list, folder):
    '''
    Check all DPX in list have made it to new folder
    Where missing add to missing_list and return
    '''
    missing_list = []
    for dpx in dpx_list:
        dpx_exist = os.path.join(folder, dpx)
        if not os.path.exists(dpx_exist):
            missing_list.append(dpx)

    # If any missing from move, attempt a re-move
    if len(missing_list) == 0:
        LOGGER.info("move_check(): All DPX copied and checked in folder: %s", folder)

    return missing_list


def move_retry(missing_list, root, folder):
    '''
    Function to attempt copy retries if some fail
    '''
    LOGGER.warning("move_retry(): MISSING DPX AFTER MOVE TO FOLDER: %s\n%s", folder, missing_list)
    splitting_log(f"MISSING DPX: Some DPX are missing after move from {folder}:\n{missing_list}")
    for item in missing_list:
        dpx_missing = os.path.join(root, item)
        if os.path.exists(dpx_missing):
            LOGGER.info("move_retry(): Retry move for missing dpx - %s", dpx_missing)
            try:
                shutil.move(dpx_missing, os.path.join(folder, item))
            except Exception:
                LOGGER.warning("move_retry(): Move retry failed: %s to %s", dpx_missing, folder)
        else:
            LOGGER.critical("move_retry(): Missing DPX not found: %s", item)


def make_text_file(cid_data, folderpath):
    '''
    Appends splitting message if file created, otherwise creates
    new text file and appends new string message to it
    '''
    text_path = os.path.join(folderpath, 'splitting_information.txt')
    if not os.path.isfile(text_path):
        with open(text_path, 'x') as log_data:
            log_data.close()
    with open(text_path, 'a') as log_data:
        log_data.write(
            f'-------------- Splitting activity: {str(datetime.datetime.now())[:19]} --------------\n'
        )

        log_data.write(f"{cid_data}")
        log_data.write("\n")
        log_data.close()


def mass_move(dpx_path, new_path):
    '''
    Function to move individual DPX file to new directory
    '''
    if os.path.exists(dpx_path) and os.path.exists(new_path):
        try:
            shutil.move(dpx_path, new_path)
        except Exception as err:
            LOGGER.warning("mass_move(): %s", err)
    else:
        LOGGER.warning("mass_move(): One of supplied paths does not exist:\n%s - %s", dpx_path, new_path)


def renumber(dpx_path, new_num):
    '''
    Split dpx_number from path and append new number for new_dpx_path
    os.rename to change the name over.
    '''
    dpx_path = dpx_path.rstrip('/')
    path = os.path.split(dpx_path)
    new_path = os.path.join(path[0], new_num)
    try:
        os.rename(dpx_path, new_path)
        LOGGER.info("renumber(): Rename paths:\n%s changed to %s", dpx_path, new_path)
        return new_path
    except Exception as error:
        LOGGER.warning("renumber(): Unable to rename paths:\n%s NOT CHANGED TO %s\n%s", dpx_path, new_path, error)
        return False


def make_dirs(new_path):
    '''
    Makes new folder path directory for each split path
    One at a time, if multiple splits then this function to be
    called multiple times to create directory
    '''
    try:
        os.makedirs(new_path, mode=0o777, exist_ok=True)
        LOGGER.info("make_dirs(): New path mkdir: %s", new_path)
        return True
    except Exception as error:
        LOGGER.warning("make_dirs(): Unable to make new directory: %s\n%s", new_path, error)
        return None


def write_csv(fname, new_fname):
    '''
    Write filename and changed filenames following splitting to CSV which maps
    the alterations, but also allows for late comers to be updated if they are
    not processed at the time of the splitting.
    '''
    data = [fname, new_fname, TODAY]

    with open(CSV_PATH, 'a', newline='') as csvfile:
        datawriter = csv.writer(csvfile)
        datawriter.writerow(data)
        csvfile.close()


def splitting_log(data):
    '''
    Write the specific splitting information to a log file (in name of folder originally split)
    Including date, original folder name, updated folder name and new folder name(s).
    Each DPX start and end file per sequence.
    '''
    if len(data) > 0:
        with open(SPLITTING_LOG, 'a') as log:
            log.write(data + "\n")
            log.close()


def part_whole_log(fname):
    '''
    Output ob_num to part_whole_search.log in DPX Errors folder. Used by various encoding scripts
    to avoid moving files if their numbers are present in the list. Original name ONLY to be added
    to this list. Handles if ob_num already listed, or if log removed, replaces and appends number.
    '''
    ob_num = fname[:-7]
    object_number = f"{ob_num}\n"
    with open(PART_WHOLE_LOG, 'a+') as log:
        log.write(object_number)
        log.close()


def record_append(priref, cid_data, original_data):
    '''
    Writes splitting data to CID UTB content field
    Temporary location, awaiting permanent CID location
    '''
    name = 'datadigipres'
    date = str(datetime.datetime.now())[:10]
    time = str(datetime.datetime.now())[11:19]
    notes = 'Automated DPX splitting script, record of actions against this item.'
    summary = 'DPX splitting summary'
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_addition = f"<utb.fieldname>{summary}</utb.fieldname><utb.content><![CDATA[{cid_data}{original_data}]]></utb.content>"
    payload_edit = f"<edit.name>{name}</edit.name><edit.date>{date}</edit.date><edit.time>{time}</edit.time><edit.notes>{notes}</edit.notes>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_head + payload_addition + payload_edit + payload_end

    lock_success = write_lock(priref)
    if lock_success:
        write_success = write_payload(priref, payload)
        if write_success:
            return True
        else:
            unlock_record(priref)
            return False
    else:
        return False


def write_lock(priref):
    '''
    Apply a writing lock to the record before updating metadata to Headers
    '''
    try:
        post_response = requests.post(
            CID_API,
            params={'database': 'items', 'command': 'lockrecord', 'priref': f'{priref}', 'output': 'json'})
        return True
    except Exception as err:
        LOGGER.warning("write_lock(): Lock record wasn't applied to record %s\n%s", priref, err)


def write_payload(priref, payload):
    '''
    Receive header, parser data and priref and write to CID items record
    '''
    post_response = requests.post(
        CID_API,
        params={'database': 'items', 'command': 'updaterecord', 'xmltype': 'grouped', 'output': 'json'},
        data={'data': payload})

    LOGGER.info(str(post_response.text))
    if "<error><info>" in str(post_response.text):
        LOGGER.warning("write_payload(): Error returned for requests.post to %s\n%s", priref, payload)
        return False
    else:
        LOGGER.info("No error warning in post_response. Assuming payload successfully written")
        return True


def unlock_record(priref):
    '''
    Only used if write fails and lock was successful, to guard against file remaining locked
    '''
    try:
        post_response = requests.post(
            CID_API,
            params={'database': 'items', 'command': 'unlockrecord', 'priref': f'{priref}', 'output': 'json'})
        return True
    except Exception as err:
        LOGGER.warning("unlock_record(): Post to unlock record failed. Check Media record %s is unlocked manually\n%s", priref, err)


def error_log(fpath, message, kandc):
    '''
    Logs for outputting to error file
    '''
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not kandc:
        with open(fpath, 'a+') as log:
            log.write(f"splitting_script {ts}: {message}.\n\n")
            log.close()
    else:
        with open(fpath, 'a+') as log:
            log.write(f"splitting_script {ts}: {message}.\n")
            log.write(f"\tPlease contact the Knowledge and Collections Developer {kandc}.\n\n")
            log.close()


if __name__ == '__main__':
    main()
