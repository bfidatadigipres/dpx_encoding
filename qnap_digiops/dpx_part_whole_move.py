#!/usr/bin/env LANG=en_UK.UTF-8 /usr/local/bin/python3

'''
dpx_part_whole_move.py

Script functions:
1. Look in part_whole_split/rawcooked and part_whole_split/tar
   for sequences moved to holding point that are under 1TB encoding size
2. Checks through all folder names, if any appear in left
   column of CSV then rename each file appropriately.
3. Sorts all files into part whole and process 01of* only
4. Taking sequence whole extracts range and builds list of names for all parts.
5. Checks in both rawcook/tar folders for evidence that all parts
   have been size checked, split and moved.
6. If all present, move them to their encoding destination which is
   dependent upon if they are in tar/rawcook folder.
7. If range are not all present, skip all and move onto next in sequence.
8. Update log throughout.

Notes:
A sequence will not be in this folder unless it is beneath 1TB/1.3TB.
A sequence will not be in this folder if it has one part whole - 01of01
It's unlikely part wholes will be split across tar/rawcook paths, but
the script is prepared for this and will look for and move to correct
encoding paths accordingly.

Joanna White 2021
'''
#Global import
import os
import re
import shutil
import logging
import csv
import datetime

# Global variables
DPX_PATH = os.environ['QNAP_DIGIOPS']
SCRIPT_LOG = os.path.join(DPX_PATH, os.environ['DPX_SCRIPT_LOG'])
CSV_PATH = os.path.join(SCRIPT_LOG, 'splitting_document.csv')
TAR_PATH = os.path.join(DPX_PATH, os.environ['DPX_WRAP'])
RAWCOOKED_PATH = os.path.join(DPX_PATH, os.environ['DPX_COOK'])
PART_RAWCOOK = os.path.join(DPX_PATH, os.environ['PART_RAWCOOK'])
PART_TAR = os.path.join(DPX_PATH, os.environ['PART_TAR'])
TODAY = str(datetime.datetime.now())[:10]

# Setup logging
LOGGER = logging.getLogger('dpx_part_whole_move.log')
HDLR = logging.FileHandler(os.path.join(SCRIPT_LOG, 'dpx_part_whole_move.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def read_csv(dpx_sequence):
    '''
    Does fname entry exist in CSV, if yes retrieve latest sequence
    number for that entry and return
    '''
    number_present = True
    new_sequence = dpx_sequence
    with open(CSV_PATH, newline='') as fname:
        readme = csv.DictReader(fname)

        while (number_present is True):
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


def fname_split(fname):
    '''
    Receive a filename extract part whole from end
    Return items split up
    '''
    name_split = fname.split('_')
    part_whole = name_split[2]
    part, whole = part_whole.split('of')

    return (name_split[0] + '_' + name_split[1] + '_', part, whole)


def return_range(dpx_sequence):
    '''
    Receive file being processed, extract part whole data
    create all fnames in that range for list comparison
    '''
    fname, part, whole = fname_split(dpx_sequence)
    part = int(part)
    whole = int(whole)
    range_list = []

    # Create new numbered files
    for count in range(1, whole + 1):
        name = fname + str(count).zfill(2) + 'of' + str(whole).zfill(2)
        # output name to full range list
        range_list.append(name)

    return range_list


def rename(part_whole_path):
    '''
    Search in path for folder list, check CSV for entry in 'original'
    column, if found rename folder to 'new_name' column
    '''
    folder_list = [ i for i in os.listdir(part_whole_path) if os.path.isdir(os.path.join(part_whole_path, i)) ]
    for item in folder_list:
        item_path = os.path.join(part_whole_path, item)
        new_name = read_csv(item)
        if len(new_name) > 0:
            LOGGER.info("rename(): Folder found that requires renaming: %s", item)
            LOGGER.info("rename(): Renaming the folder to: %s", new_name)
            new_path = renumber(item_path, new_name)
            if os.path.exists(new_path):
                LOGGER.info("rename(): Successfully renamed file %s\n", new_path)
            else:
                LOGGER.warning("rename(): New path not rename: %s to %s\n", item_path, new_path)


def main():
    '''
    Search paths for files, and check if they need renaming
    When all renamed, check for complete part wholes and move to destination
    '''
    LOGGER.info("============ DPX PART WHOLE MOVE START ============")
    rename(PART_TAR)
    rename(PART_RAWCOOK)

    part_path = os.path.split(os.path.dirname(PART_TAR))[0]
    for root, dirs, files in os.walk(part_path):
        for directory in dirs:
            range_list = []
            seq_paths = []
            dirpath = os.path.join(root, directory)
            if directory.startswith('N_'):
                if directory.endswith('01of01'):
                    LOGGER.info("Directory with 01of01 found: %s", directory)
                    LOGGER.info("Moving straight to encoding path")
                    if '/tar/' in str(dirpath):
                        shutil.move(dirpath, TAR_PATH)
                    elif '/rawcook/' in str(dirpath):
                        shutil.move(dirpath, RAWCOOKED_PATH)
                elif re.match(".+01of*", directory):
                    LOGGER.info("First directory in sequence found: %s", directory)
                    range_list = return_range(directory)
                    LOGGER.info("Range list extracted: %s", range_list)
                else:
                    continue

            seq_paths = check_sequence_range(range_list, part_path)
            if directory.startswith('N_') and seq_paths:
                LOGGER.info("All sequences present in part_whole_split and can be moved to encoding path")
                folder_moves(seq_paths)
            elif directory.startswith('N_') and not seq_paths:
                LOGGER.info("NOT MOVING: %s sequence parts missing from folder\n", directory)

    LOGGER.info("============= DPX PART WHOLE MOVE END =============\n")


def check_sequence_range(sequence_range, part_path):
    '''
    Checks for all present in path and returns None
    if any one is missing, or returns whole sequence path list
    '''
    seq_paths = []
    for item in sequence_range:
        dirpath_tar = os.path.join(part_path, 'tar', item)
        dirpath_raw = os.path.join(part_path, 'rawcook', item)
        if os.path.isdir(dirpath_tar):
            seq_paths.append(dirpath_tar)
        elif os.path.isdir(dirpath_raw):
            seq_paths.append(dirpath_raw)
        else:
            LOGGER.info("check_sequence_range(): Directory %s missing from %s folder", item, part_path)
            return None
    return seq_paths


def renumber(dpx_path, new_num):
    '''
    Split dpx_number from path and append new number for new_dpx_path
    os.rename to change the name over.
    '''
    path = os.path.split(dpx_path)
    new_path = os.path.join(path[0], new_num)
    try:
        os.rename(dpx_path, new_path)
        LOGGER.info("renumber(): Path %s changed to %s", dpx_path, new_path)
        return new_path
    except OSError:
        LOGGER.warning("renumber(): Unable to rename path %s", dpx_path)
        return False


def folder_moves(seq_path):
    '''
    Move sequnce paths to transcode paths
    '''
    for item in seq_path:
        if '/tar/' in str(item):
            try:
                shutil.move(item, TAR_PATH)
                LOGGER.info("Moved to TAR PATH: %s", item)
            except Exception as err:
                LOGGER.warning("Unable to move files %s to %s\n%s", item, TAR_PATH, err)
        if '/rawcook/' in str(item):
            try:
                shutil.move(item, RAWCOOKED_PATH)
                LOGGER.info("Moved to RAWCOOK PATH: %s", item)
            except Exception as err:
                LOGGER.warning("Unable to move files %s to %s\n%s", item, TAR_PATH, err)


if __name__ == '__main__':
    main()
