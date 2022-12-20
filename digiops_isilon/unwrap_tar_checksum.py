#!/usr/bin/env python3

'''
unwrap_tar_checksum.py

Script functions:
1. Check in unwrap_tar folder for any .tar packages
2. Launch Linux tar to unwrap the file
3. Check for success/fail statements (to be ascertained)
4. If fail, attempt unwrap again with Python tarfile
5. For passed files look for presence of MD5 manifest
6. Load any manifest present into dictionary in code
7. Create new MD5 manifest for unpacked TAR file
8. Compare the two and return result to log alongside
   untarred file.
9. If no MD5 checksum manifest in tar return statement
   to log alongside untarred file.

Joanna White
2022
'''

#Global import
import os
import sys
import time
import json
import shutil
import hashlib
import logging
import datetime
import subprocess

# Global variables
DPX_PATH = os.environ['IS_DIGITAL']
SCRIPT_LOG = os.path.join(DPX_PATH, os.environ['DPX_SCRIPT_LOG'])
UNTAR_PATH = os.path.join(DPX_PATH, os.environ['UNWRAP_TAR'])
COMPLETED = os.path.join(UNTAR_PATH, 'completed/')
FAILED = os.path.join(UNTAR_PATH, 'failed/')
LOCAL_LOG = os.path.join(UNTAR_PATH, 'unwrapped_tar_checksum.log')
TODAY = str(datetime.datetime.now())[:10]

# Setup logging
LOGGER = logging.getLogger('unwrap_tar_checksum')
HDLR = logging.FileHandler(os.path.join(SCRIPT_LOG, 'unwrap_tar_checksum.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def untar_file(fpath):
    '''
    Subprocess action to unwrap a file
    '''
    cmd = [
        'tar', '-xf',
        fpath
    ]

    status = subprocess.call(cmd,shell=True,stderr=subprocess.STDOUT)
    if 'Exiting with failure status' not in status:
        return True


def python_tarfile(fpath):
    '''
    If Linux TAR fails, try with python tarfile
    '''
    pass


def main():
    '''
    Check unwrap_tar path and iterate contents unwrapping/checksum
    verify testing if manifest found in package
    '''
    log_list = []
    tar_files = [x for x in os.listdir(UNTAR_PATH) if os.path.isfile(os.path.join(UNTAR_PATH, x))]
    if len(tar_file) == 0:
        sys.exit(f"{UNTAR_PATH} EMPTY. SCRIPT EXITING.")

    LOGGER.info("========= UNWRAP TAR CHECKSUM SCRIPT START =====================")

    for fname in tar_files:
        tarfile_retry = False
        if 'unwrapped_tar_checksum.log' in str(fname):
            continue
        if not fname.endswith(('.tar', '.TAR')):
            log_list.append(f"{str(datetime.datetime.now())[:10]}\tSKIPPING - File is not a TAR file: {fname}.")
            log_list.append(f"{str(datetime.datetime.now())[:10]}\tPlease remove non TAR files from 'unwrap_tar' folder.")
            LOGGER.info("Skipping file, not a TAR: %s", fname)
            build_log(log_list)
            continue

        fpath = os.path.join(UNTAR_PATH, fname)
        log_list.append(f"{str(datetime.datetime.now())[:10]}\tNew file found: {fpath}")
        LOGGER.info("File found to process: %s", fname)
        tic = time.start()
        success = untar_file(fpath)
        if not success:
            LOGGER.warning("Unwrapping failed with Linux TAR. Adding to Python tarfile retry list.")
            tarfile_retry = True
        toc = time.stop()
        minutes_taken = (toc - tic) // 60

        # Folder/file location checks
        untar_file = fname.split('.tar')[0]
        untar_fpath = os.path.join(UNTAR_PATH, untar_file)

        if not os.path.exists(untar_fpath):
            LOGGER.warning("Unwrapped folder/file not found. Adding to Python tarfile retry list: %s", untar_file)
            tarfile_retry = True

        if tarfile_retry is True:
            LOGGER.info("Untar has failed using Linux TAR. Attemping Python tarfile unwrap now")
            tic = time.start()
            py_success = python_tarfile(fpath)
            toc = time.stop()
        # UPTO HERE JOANNA

        # File MD5 checks
        # Build checksum manifest of un_tarred file
        local_manifest = get_checksum(untar_fpath)
        local_manifest_path = dump_to_file(untar_fpath, local_manifest)
        # Fetch enclosed MD5 manifest if present
        md5_manifest = os.path.join(UNTAR_PATH, f"{fname}_manifest.md5")
        if os.path.exist(md5_manifest):
            LOGGER.info("MD5 manifest for untar item exists: %s", untar_file)
            manifest_contents = fetch_checksum_dict(md5_manifest)

        if manifest_contents == local_manifest:
            LOGGER.info("MD5 manifest matches local MD5 manifest. Bit perfect restoration of TARRED file.")
        elif manifest_contents != local_manifest:
            LOGGER.info("MD5 manifest does not match. See manifest for details:")


    LOGGER.info("========= UNWRAP TAR CHECKSUM SCRIPT END =======================")


def fetch_checksum_dict(md5_manifest):
    '''
    Collect contents of Manifest using JSON load
    '''
    with open(md5_manifest, 'r') as file:
        data = json.load(file)

        if isinstance(data, dict):
            return data


def get_checksum(fpath):
    '''
    Using file path, generate file checksum
    return as dictionary
    '''

    md5s = {}
    for root, _, files in os.walk(fpath):
        for file in files:
            h = hashlib.md5()
            with open(os.path.join(root, file), "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
                md5s[file] = h.hexdigest()
    return md5s


def dump_to_file(untar_path, md5_dct):
    '''
    Write md5 manifest to file locally
    '''
    md5_path = f"{untar_path}_unwrap_manifest.md5"

    try:
        with open(md5_path, 'w+') as json_file:
            json_file.write(json.dumps(md5_dct, indent=4))
            json_file.close()
    except Exception as exc:
        LOGGER.warning("make_manifest(): FAILED to create JSON %s", exc)

    if os.path.exists(md5_path):
        return md5_path


def build_log(message_list):
    '''
    Add local log messages to file
    '''
    if not os.path.exists(LOCAL_LOG):
        with open(LOCAL_LOG, 'x') as file:
            file.close()
    with open(LOCAL_LOG, 'a') as file:
        for line in message_list:
            file.write(f"{line}\n"}


if __name__ == '__main__':
    main()
