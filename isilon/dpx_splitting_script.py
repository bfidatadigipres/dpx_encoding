#!/usr/bin/env LANG=en_UK.UTF-8 /usr/local/bin/python3

'''
*** THIS SCRIPT MUST RUN FROM SHELL LAUNCH SCRIPT RUNNING PARALLEL 1 JOB AT A TIME ***

Receives three sys.argv items from shell script:
KB size, path to folder, and encoding type (tar or luma/rawcooked)
Script functions:
1. Checks if dpx sequence name in CSV_PATH file
   If yes, renumbers folder and updates dpx_path/dpx_sequence variables
2. Division requirements of DPX sequence calculated:
   a. None, folder is under splitting size. DPX sequences moved to encoding paths
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
   h. Each block is moved to it's corresponding new folder, one DPX at a time using shutil.move

State of script:
Configured and ready to use
Implementation across VMs to be completed

Joanna White 2021
'''

import os
import sys
import shutil
import logging
import csv
import datetime

# Global variables
ISILON = "/mnt/isilon_lt8"
ERRORS = os.path.join(ISILON, os.environ['CURRENT_ERRORS'])
OVERSIZED_SEQ = os.path.join(ERRORS, 'oversized_sequences/')
SCRIPT_LOG = os.path.join(ISILON, os.environ['DPX_SCRIPT_LOG'])
CSV_PATH = os.path.join(SCRIPT_LOG, 'splitting_document.csv')
PART_WHOLE_LOG = os.path.join(ERRORS, 'part_whole_search.log')
SPLITTING_LOG = os.path.join(SCRIPT_LOG, 'DPX_splitting.log')
TAR_PATH = os.path.join(ISILON, os.environ['DPX_WRAP'])
RAWCOOKED_PATH = os.path.join(ISILON, os.environ['DPX_COOK'])
TODAY = str(datetime.datetime.now())[:10]
TODAY_FULL = str(datetime.datetime.now())

# Setup logging
LOGGER = logging.getLogger('dpx_splitting_script.log')
HDLR = logging.FileHandler(os.path.join(SCRIPT_LOG, 'dpx_splitting_script.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def read_csv(dpx_sequence):
    '''
    Does fname entry exist in CSV, if yes retrieve data and return in tuple
    '''
    new_number = ''
    with open(CSV_PATH, newline='') as fname:
        readme = csv.DictReader(fname)
        for row in readme:
            orig_num = row['original']
            if str(orig_num) == str(dpx_sequence):
                new_number = row['new_number']
                return str(new_number)
            else:
                return new_number


def count_files(dirpath, division):
    '''
    Counts total DPX files in supplied sorted folder and returns the division totals
    Including first DPX in each block as lists, eg for single split:
    ['38411', '9602', '0084600.dpx', '0096003.dpx', '0105605.dpx', '0115207.dpx']
    Plus returns list of dictionary blocks of dpx numbers for move
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
        dpx_list.append({'block2': dpx_sequence[start_num:]})

    elif division == '3':
        cuts = int(file_count) // 3
        dpx_block1 = dpx_sequence[0]
        start_block3 = (cuts * 2)
        dpx_block2 = dpx_sequence[cuts]
        dpx_block3 = dpx_sequence[start_block3]
        block_list = [file_count, cuts, dpx_block1, dpx_block2, dpx_block3]
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
        dpx_list.append({'block2': dpx_sequence[start_block2:start_block3]})
        dpx_list.append({'block3': dpx_sequence[start_block3:start_block4]})
        dpx_list.append({'block4': dpx_sequence[start_block4:start_block5]})
        dpx_list.append({'block5': dpx_sequence[start_block5:]})

    return (block_list, dpx_list)


def fname_split(fname):
    '''
    Receive a filename extract part whole from end
    Return items split up
    '''
    fname = fname.rstrip('/')
    name_split = fname.split('_')
    part_whole = name_split[2]
    part, whole = part_whole.split('of')

    return (name_split[0] + '_' + name_split[1] + '_', part, whole)


def workout_division(arg, kb_size):
    '''
    Pass encoding argument and which is passed from shell launcher script
    Kilobyte calculated as byte = 1024
    '''
    division = ''
    kb_size = int(kb_size)

    # Size calculation for rawcooked RGB encodings
    if 'rawcooked' in arg:
        if kb_size <= 1503238552:
            division = None
        elif 1503238553 <= kb_size <= 3006477107:
            division = '2'
        elif 3006477108 <= kb_size <= 4509715660:
            division = '3'
        elif 4509715661 <= kb_size <= 6012954214:
            division = '4'
        elif kb_size >= 6012954215:
            LOGGER.warning("workout_division(): RAWcooked file is too large for DPX splitting: %s KB", kb_size)
            division = 'oversize'

    # Size calculation for luma or tar encoding sizes
    elif 'tar' in arg or 'luma' in arg:
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
        elif kb_size >= 5368709121:
            LOGGER.warning("workout_division(): TAR or Luma Y file is too large for DPX splitting: %s KB", kb_size)
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
    '''
    fname, part, whole = fname_split(dpx_sequence)
    part = int(part)
    whole = int(whole)
    change_list = []
    folder1, folder2, folder3, folder4 = '', '', '', ''

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

    return (change_list, folder1, folder2, folder3, folder4)


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
    if len(sys.argv) < 2:
        LOGGER.warning("SCRIPT EXITING: Error with shell script input:\n %s", sys.argv)
        sys.exit()
    else:
        LOGGER.info("================== START Python3 DPX splitting script START ==================")
        data = sys.argv[1]
        data = data.split(', ')
        kb_size = int(data[0])
        dpx_path = str(data[1])
        encoding = str(data[2])
        dpx_path = dpx_path.rstrip('/')
        dpx_sequence = os.path.basename(dpx_path)
        LOGGER.info("Processing DPX sequence: %s", dpx_sequence)
        ## Check if sequence has new numbering that should be updated
        new_num = read_csv(dpx_sequence)
        # Renumber folder and update dpx_path / dpx_sequence
        if new_num is None:
            LOGGER.info("Sequence %s not found in CSV so proceeding with processing:\n%s", dpx_sequence, dpx_path)
            print("No renaming necessary for sequence {}".format(dpx_path))
        elif len(new_num) > 0:
            try:
                new_path = renumber(dpx_path, new_num)
                LOGGER.info("Folder %s successfully renamed %s from CSV", dpx_sequence, new_num)
                LOGGER.info("DPX sequence path %s will be reconfigured to %s", dpx_path, new_path)
                splitting_log("\n*** DPX sequence found in CSV and needs renumbering")
                splitting_log("DPX sequence path {} being renumbered to {}".format(dpx_path, new_path))
                dpx_path = new_path
                dpx_sequence = new_num
            except Exception as err:
                LOGGER.warning("Renumbering failed, exiting script to avoid processing incorrect files", err)
                LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                sys.exit()
        # No renumbering needed
        else:
            LOGGER.info("Sequence %s not found in CSV so proceeding with processing:\n%s", dpx_sequence, dpx_path)

        # Does this sequence need splitting?
        division = workout_division(encoding, kb_size)

        # Name preparations for folder splitting
        path_split = os.path.split(dpx_path)
        root_path = path_split[0]

        # No division needed, sequence is below 1.4TB
        if division is None:
            LOGGER.info("No splitting necessary for: %s\nMoving to encoding path for %s", dpx_path, encoding)
            if 'rawcooked' in encoding or 'luma' in encoding:
                LOGGER.info("Moving DPX sequence to RAWcooked path: %s", dpx_sequence)
                try:
                    shutil.move(dpx_path, RAWCOOKED_PATH)
                    LOGGER.info("Move %s to RAWcooked encoding path: %s", dpx_sequence, RAWCOOKED_PATH)
                    LOGGER.info("Script exiting")
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    sys.exit()
                except Exception as err:
                    LOGGER.warning("Unable to move folder to RAWcooked path: %s", dpx_path)
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    sys.exit()
            elif 'tar' in encoding:
                LOGGER.info("Folder %s is not oversized.\nMoving DPX sequence to TAR path", dpx_path)
                try:
                    shutil.move(dpx_path, TAR_PATH)
                    LOGGER.info("Move completed to TAR encoding path: %s", dpx_path)
                    LOGGER.info("Script exiting")
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    sys.exit()
                except Exception as err:
                    LOGGER.warning("Unable to move folder to TAR path: %s", dpx_path)
                    LOGGER.info("==================== END Python3 DPX splitting script END ====================")
                    sys.exit()
        # Folder is larger than 5TB (TAR/Luma) / 5.6TB (RAWcooked) script exit
        elif 'oversize' in division:
            LOGGER.warning("OVERSIZE FOLDER: Too large for splitting script %s", dpx_path)
            splitting_log("OVERSIZE FOLDER: {}. Moving to current_errors/oversized_sequence/ folder".format(dpx_sequence))
            LOGGER.info("Moving oversized folder %s to current_errors/oversized_sequence folder")
            LOGGER.info("Adding {} sequence number to part_whole log in current_errors/ folder")
            part_whole_log(dpx_sequence)
            try:
                shutil.move(dpx_path, OVERSIZED_SEQ)
            except Exception as err:
                LOGGER.warning("Unable to move %s to oversized_sequence/ folder", dpx_sequence, err)
            LOGGER.warning("Script will exit, manual intervention needed for this file")
            LOGGER.critical("========= SCRIPT EXIT - MANUAL ASSISTANCE NEEDED =========")
            sys.exit()

        # Folder requires splitting activities (can this function serve all divisions if lists are iterable?)
        else:
            LOGGER.info("Splitting folders with division %s necessary for: %s", division, dpx_path)
            splitting_log("\n-------------------------------- SPLIT START ----------------------------------- {}".format(TODAY_FULL))
            splitting_log("NEW FOLDER FOUND THAT REQUIRES SPLITTING:\n{}".format(dpx_path))
            splitting_log("DPX sequence encoding <{}> is {} KB in size, requiring {} divisions".format(encoding, kb_size, division))
            LOGGER.info("Adding {} sequence number to part_whole log in current_errors/ folder")
            part_whole_log(dpx_sequence)
            # Generate new folder names from dpx_sequence/division
            folder1, folder2, folder3, folder4 = '', '', '', ''
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
            post_foldername_list = return_range_following(dpx_sequence, division)

            # Append all new numbers to CSV
            splitting_log("\nNew folder numbers:")
            if len(pre_foldername_list) > 0:
                for dic in pre_foldername_list:
                    for key, val in dic.items():
                        write_csv(key, val)
                        splitting_log("{} will be renumbered {}".format(key, val))
            for dic in foldername_list_new:
                for key, val in dic.items():
                    if dpx_sequence in key:
                        # Extract new sequence number and change dpx_path / dpx_sequence
                        new_dpx_sequence = val
                        new_path = renumber(dpx_path, new_dpx_sequence)
                        dpx_sequence = new_dpx_sequence
                        dpx_path = new_path
                        print("!!! NEW DPX SEQ NUMBER: {}, {}".format(dpx_sequence, dpx_path))
                        LOGGER.info("New sequence number retrieved for this DPX sequence: %s", dpx_sequence)
                        LOGGER.info("DPX path updated: %s", dpx_path)
                for key, val in dic.items():
                    write_csv(key, val)
                    splitting_log("{} will be renumbered {}".format(key, val))
            if len(post_foldername_list) > 0:
                for dic in post_foldername_list:
                    for key, val in dic.items():
                        write_csv(key, val)
                        splitting_log("{} will be renumbered {}".format(key, val))

            new_folder1, new_folder2, new_folder3, new_folder4 = '', '', '', ''
            # Find dpx_dirpath for all scan folders containing DPX files
            for root, dirs, files in os.walk(dpx_path):
                for file in files:
                    if file.endswith((".dpx", ".DPX")):
                        dpx_dirpath = root
                        LOGGER.info("*** Folder path for splitting %s", root)
                        splitting_log("\n*** Making new folders with new sequence names for path: {}".format(root))
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
                        print("New folders created:\n{}\n{}\n{}\n{}".format(new_folder1, new_folder2, new_folder3, new_folder4))

                        # Obtain: file_count, cuts, dpx_block data
                        LOGGER.info("Folder %s will be divided into %s divisions now", dpx_sequence, division)
                        block_data = count_files(dpx_dirpath, division)
                        block_list = block_data[0]
                        splitting_log("\n Block list data (total DPX, DPX per sequence, first DPX per block):\n{}".format(block_list))

                        # Output data to splitting log
                        splitting_log("\nFolder {} contains {} DPX items. Requires {} divisions, {} items per new folder".format(dpx_sequence, block_list[0], division, block_list[1]))
                        splitting_log("First DPX number is {} for new folder: {}".format(block_list[3], new_folder1))
                        if folder2:
                            splitting_log("First DPX number is {} for new folder: {}".format(block_list[4], new_folder2))
                        if folder3:
                            splitting_log("First DPX number is {} for new folder: {}".format(block_list[5], new_folder3))
                        if folder4:
                            splitting_log("First DPX number is {} for new folder: {}".format(block_list[6], new_folder4))

                        LOGGER.info("Block data being calculated and DPX moved to final destinations")
                        print("Block data for slices: {}".format(block_list))
                        dpx_list = block_data[1]
                        for dictionary in dpx_list:
                            for key, val in dictionary.items():
                                if 'block2' in key:
                                    splitting_log("\nMoving block 2 DPX data to {}".format(new_folder1))
                                    for dpx in val:
                                        dpx_to_move = os.path.join(root, dpx)
                                        shutil.move(dpx_to_move, new_folder1)
                                if 'block3' in key:
                                    splitting_log("Moving block 3 DPX data to {}".format(new_folder2))
                                    for dpx in val:
                                        dpx_to_move = os.path.join(root, dpx)
                                        shutil.move(dpx_to_move, new_folder2)
                                if 'block4' in key:
                                    splitting_log("Moving block 4 DPX data to {}".format(new_folder3))
                                    for dpx in val:
                                        dpx_to_move = os.path.join(root, dpx)
                                        shutil.move(dpx_to_move, new_folder3)
                                if 'block5' in key:
                                    splitting_log("Moving block 5 DPX data to {}".format(new_folder4))
                                    for dpx in val:
                                        dpx_to_move = os.path.join(root, dpx)
                                        shutil.move(dpx_to_move, new_folder4)
                        splitting_log("\n-------------------------------- SPLIT END -------------------------------------")
                        LOGGER.info("Splitting completed for path: %s", root)
                        break
            LOGGER.info("==================== END Python3 DPX splitting script END ====================")


def mass_move(dpx_path, new_path):
    '''
    Function just to move individual DPX file to new directory
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
    Shutil move / or os.rename to change the name over.
    DPX path must be supplied without trailing '/'
    '''
    dpx_path = dpx_path.rstrip('/')
    path = os.path.split(dpx_path)
    new_path = os.path.join(path[0], new_num)
    try:
        os.rename(dpx_path, new_path)
        LOGGER.info("renumber(): Rename paths:\n%s changed to %s", dpx_path, new_path)
        return new_path
    except Exception as error:
        LOGGER.warning("renumber(): Unable to rename paths:\n%s NOT CHANGED TO %s", dpx_path, new_path, error)
        return False


def make_dirs(new_path):
    '''
    Makes new folder path directory for each split path
    One at a time, if multiple splits then this function to be
    called multiple times to create directory
    '''
    try:
        os.makedirs(new_path, exist_ok=True)
        LOGGER.info("make_dirs(): New path mkdir: %s", new_path)
        return True
    except Exception as error:
        print("Unable to make new directory {}".format(new_path))
        LOGGER.warning("make_dirs(): Unable to make new directory: %s", new_path, error)
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
            LOGGER.info("splitting_log(): Filename splitting data appended to splitting log:\n< %s >", data)
            log.write(data + "\n")
            log.close()


def part_whole_log(fname):
    '''
    Output ob_num to part_whole_search.log in DPX Errors folder. Used by various encoding scripts
    to avoid moving files if their numbers are present in the list. Original name ONLY to be added
    to this list. Handles if ob_num already listed, or if log removed, replaces and appends number.
    '''
    ob_num = fname[:-7]

    with open(PART_WHOLE_LOG, 'a+') as log:
        log.seek(0)
        data = log.read()
        for line in data:
            if ob_num in str(line):
                LOGGER.info("part_whole_log(): Object number %s already in part whole search log, skipping", ob_num)
            else:
                if len(data) > 0:
                    log.write("\n")
                LOGGER.info("part_whole_log(): Object number %s appended to part_whole log in errors folder", ob_num)
                log.write(ob_num)
        log.close()


if __name__ == '__main__':
    main()
