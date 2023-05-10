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
10.Move TAR files to completed/ failed/ folders depending
   on successful/unsuccessful results

Joanna White
2023
'''

#Global import
import os
import sys
import time
import json
import shutil
import tarfile
import hashlib
import logging
import datetime
import subprocess

# Global variables
DPX_PATH = os.environ['QNAP_FILMOPS']
ERRORS = os.path.join(DPX_PATH, os.environ['CURRENT_ERRORS'])
SCRIPT_LOG = os.path.join(DPX_PATH, os.environ['DPX_SCRIPT_LOG'])
UNTAR_PATH = os.path.join(DPX_PATH, os.environ['UNWRAP_TAR'])
COMPLETED = os.path.join(UNTAR_PATH, 'completed/')
FAILED = os.path.join(UNTAR_PATH, 'failed/')
LOCAL_LOG = os.path.join(UNTAR_PATH, 'unwrapped_tar_checksum.log')
TODAY = str(datetime.datetime.now())[:10]

# Setup logging
LOGGER = logging.getLogger('unwrap_tar_checksum_qnap_filmops')
HDLR = logging.FileHandler(os.path.join(SCRIPT_LOG, 'unwrap_tar_checksum.log'))
FORMATTER = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def linux_untar_file(fpath):
    '''
    Subprocess action to unwrap a file
    Change directory and place file into
    folder if not folder already.
    '''
    cwd = os.getcwd()
    new_wd, fname = os.path.split(fpath)
    os.chdir(new_wd)
    file = fname.split('.tar')[0]
    extract_path = os.path.join(new_wd, file)
    os.makedirs(extract_path, exist_ok=True)

    cmd = [
        "tar", "-xf",
        fname, "-C",
        extract_path
    ]
    try:
        stats = subprocess.call(cmd,stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise Exception from e
    os.chdir(cwd)

    if stats == 0:
        return extract_path


def python_tarfile(fpath, untar_fpath):
    '''
    If Linux TAR fails, try with python tarfile
    extract to folder named after tar file
    '''
    if not os.path.exists(untar_fpath):
        os.makedirs(untar_fpath)

    tar_file = tarfile.open(fpath)
    tar_file.extractall(untar_fpath)
    tar_file.close()

    if os.listdir(untar_fpath):
        return True


def main():
    '''
    Check unwrap_tar path and iterate contents unwrapping/checksum
    verify testing if manifest found in package
    '''
    log_list = []
    tar_files = [x for x in os.listdir(UNTAR_PATH) if os.path.isfile(os.path.join(UNTAR_PATH, x))]
    if len(tar_files) == 0:
        sys.exit(f"{UNTAR_PATH} EMPTY. SCRIPT EXITING.")

    LOGGER.info("========= UNWRAP TAR CHECKSUM SCRIPT START =====================")

    for fname in tar_files:
        fname_log = fname.split(".")[0]
        if 'unwrapped_tar_checksum.log' in str(fname):
            continue
        if not fname.endswith(('.tar', '.TAR')):
            log_list.append(f"{str(datetime.datetime.now())[:10]}\tSKIPPING - File is not a TAR file: {fname}.")
            log_list.append(f"{str(datetime.datetime.now())[:10]}\tPlease remove non TAR files from 'unwrap_tar' folder.")
            LOGGER.info("Skipping file, not a TAR: %s", fname)
            error_mssg1 = "File/folder placed in unwrap_tar/ folder is not a TAR file. Please remove this item from this path"
            error_mssg2 = None
            error_log(os.path.join(ERRORS, f"{fname_log}_errors.log"), error_mssg1, error_mssg2)
            build_log(log_list)
            continue

        fpath = os.path.join(UNTAR_PATH, fname)
        log_list.append(f"{str(datetime.datetime.now())[:10]}\tNew file found: {fpath}")
        LOGGER.info("File found to process: %s", fname)
        log_list.append(f"{str(datetime.datetime.now())[:10]}\tAttempting extraction using Linux TAR programme...")
        tic = time.perf_counter()
        untar_fpath = linux_untar_file(fpath)
        toc = time.perf_counter()
        minutes_taken = (toc - tic) // 60

        if not untar_fpath:
            LOGGER.warning("Unwrapping failed with Linux TAR. Adding to Python tarfile retry list.")
            untar_file = fname.split('.tar')[0]
            untar_fpath = os.path.join(UNTAR_PATH, untar_file)
            if not os.path.exists(untar_fpath):
                os.makedirs(untar_fpath, exist_ok=True)
            log_list.append(f"{str(datetime.datetime.now())[:10]}\tLinux TAR extraction failed... trying with Python tarfile")
            LOGGER.warning("Unwrapped folder/file not found. Adding to Python tarfile retry list: %s", untar_file)
            LOGGER.info("Attemping Python tarfile unwrap now...")

            # Try with Python tarfile
            tic = time.perf_counter()
            py_success = python_tarfile(fpath, untar_fpath)
            toc = time.perf_counter()
            minutes_taken = (toc - tic) // 60
            if not py_success:
                LOGGER.warning("Python tarfile has failed to extract content of TAR. Script exiting, TAR needs manual assistance.")
                shutil.move(fpath, FAILED)
                error_mssg1 = f"Linux TAR and Python tarfile cannot extract data. Please try alternative software. File location: {fpath}"
                error_mssg2 = None
                error_log(os.path.join(ERRORS, f"{fname_log}_errors.log"), error_mssg1, error_mssg2)
                if os.path.exists(untar_fpath) and not os.listdir(untar_fpath):
                    LOGGER.info("Moved TAR to failed/ folder. Deleted empty folder: %s", untar_file)
                    log_list.append(f"{str(datetime.datetime.now())[:10]}\tMoved TAR to failed/ folder. Deleted empty extraction folder: {untar_file}")
                    os.remove(untar_fpath)
                elif os.path.exist(untar_fpath) and os.listdir(untar_fpath):
                    LOGGER.info("Moved TAR to failed/ folder. Folder %s has contents, moving to failed/ folder for review", untar_file)
                    log_list.append(f"{str(datetime.datetime.now())[:10]}\tMoved TAR to failed/ folder. Folder {untar_file} has contents. Moving to failed/ folder for review")
                    shutil.move(untar_fpath, FAILED)
                log_list.append(f"{str(datetime.datetime.now())[:10]}\tSkipping further actions for {fname}. Manual assistance needed")
                log_list.append(f"{str(datetime.datetime.now())[:10]}\t-------------------------------------------------------------------")
                LOGGER.warning("Skipping futher actions for %s, TAR needs manual assistance.", fname)
                build_log(log_list)
                continue
            LOGGER.info("Python tarfile extracted file to path: %s", untar_fpath)
        else:
            LOGGER.info("Linux TAR programme extracted file to path: %s", untar_fpath)

        log_list.append(f"{str(datetime.datetime.now())[:10]}\tExtracted TAR file successful: {untar_fpath}")
        log_list.append(f"{str(datetime.datetime.now())[:10]}\tExtraction took {minutes_taken} minutes to complete")
        LOGGER.info("It took %s minutes to perform this extraction.", minutes_taken)

        # Build checksum manifest of un_tarred file
        local_manifest = get_checksum(untar_fpath)
        local_manifest_path = dump_to_file(untar_fpath, local_manifest)
        log_list.append(f"{str(datetime.datetime.now())[:10]}\tGenerating local MD5 manifest for extracted data: {local_manifest_path}")

        # Fetch enclosed MD5 manifest if present
        md5_manifest = os.path.join(untar_fpath, f"{fname}_manifest.md5")
        if os.path.exists(md5_manifest):
            match = True
            LOGGER.info("MD5 manifest for untar item exists: %s", md5_manifest)
            manifest_contents = fetch_checksum_dict(md5_manifest)
            log_list.append(f"{str(datetime.datetime.now())[:10]}\tMD5 manifest extracted from TAR file for comparison")

            for k, v in manifest_contents.items():
                if local_manifest[k] == v:
                    print(f"MD5 match: {k}")
                else:
                    print(f"MD5 does not match: {k}")
                    match = False

            if match:
                log_list.append(f"{str(datetime.datetime.now())[:10]}\tLocal manifest matches extracted MD5 manifest. File identical to preservation original.")
                LOGGER.info("MD5 manifest matches local MD5 manifest. Bit perfect restoration of TARRED file.")
            else:
                LOGGER.info("MD5 manifest does not match all items. See manifest for details: %s", local_manifest_path)
                log_list.append(f"{str(datetime.datetime.now())[:10]}\tMD5 manifest cannot be fully matched to extracted MD5 manifest.")
                error_mssg1 = f"MD5 manifests do not match from TAR file, and unwrapped TAR folder contents: {local_manifest_path}"
                error_mssg2 = None
                error_log(os.path.join(ERRORS, f"{fname_log}_errors.log"), error_mssg1, error_mssg2)
        else:
            LOGGER.info("MD5 manifest was not extracted from TAR file. No comparison possible.")
            log_list.append(f"{str(datetime.datetime.now())[:10]}\tNo MD5 manifest extracted from TAR file. No comparison possible.")

        shutil.move(fpath, COMPLETED)
        LOGGER.info("%s file moved to COMPLETED path: %s", fname, COMPLETED)
        log_list.append(f"{str(datetime.datetime.now())[:10]}\tMoved TAR to completed/ folder for manual deletion.")
        log_list.append(f"{str(datetime.datetime.now())[:10]}\t-------------------------------------------------------------------")
        build_log(log_list)

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
            hsh = hashlib.md5()
            with open(os.path.join(root, file), "rb") as md5_file:
                for chunk in iter(lambda: md5_file.read(65536), b""):
                    hsh.update(chunk)
                md5s[file] = hsh.hexdigest()
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
            file.write(f"{line}\n")


def error_log(fpath, message, kandc):
    '''
    If needed, write error log
    for incomplete sequences.
    '''
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not kandc:
        with open(fpath, 'a+') as log:
            log.write(f"splitting_script {ts}: {message}.")
            log.close()
    else:
        with open(fpath, 'a+') as log:
            log.write(f"splitting_script {ts}: {message}.")
            log.write(f"\tPlease contact the Knowledge and Collections Developer {kandc}.")
            log.close()


if __name__ == '__main__':
    main()
