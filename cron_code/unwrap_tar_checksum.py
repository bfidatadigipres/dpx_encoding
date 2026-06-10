#!/usr/bin/env python3

"""
unwrap_tar_checksum.py

Script functions:
0. Receive path to storage/build unwrap_tar path
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
"""

import datetime
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import time

if not len(sys.argv) >= 2:
    sys.exit("Missing argument for python launch")

# Global variables
TARGET = sys.argv[1]
UNTAR_PATH = os.path.join(TARGET, os.environ["UNWRAP_TAR"])
SCRIPT_LOG = os.environ["LOG_PATH"]
COMPLETED = os.path.join(UNTAR_PATH, "completed/")
FAILED = os.path.join(UNTAR_PATH, "failed/")
LOCAL_LOG = os.path.join(UNTAR_PATH, "unwrapped_tar_checksum.log")
TODAY = str(datetime.datetime.now())[:10]

# Setup logging
LOGGER = logging.getLogger("unwrap_tar_checksum_qnap_11_digiops")
HDLR = logging.FileHandler(os.path.join(SCRIPT_LOG, "unwrap_tar_checksum.log"))
FORMATTER = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
HDLR.setFormatter(FORMATTER)
LOGGER.addHandler(HDLR)
LOGGER.setLevel(logging.INFO)


def linux_untar_file(fpath):
    """
    Subprocess action to unwrap a file
    Change directory and place file into
    folder if not folder already.
    """
    cwd = os.getcwd()
    new_wd, fname = os.path.split(fpath)
    os.chdir(new_wd)
    file = fname.split(".tar")[0]
    extract_path = os.path.join(new_wd, file)
    os.makedirs(extract_path, mode=0o777, exist_ok=True)

    cmd = ["tar", "-xf", fname, "-C", extract_path]
    try:
        stats = subprocess.call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise Exception from e
    os.chdir(cwd)

    if stats == 0:
        return extract_path


def python_tarfile(fpath, untar_fpath):
    """
    If Linux TAR fails, try with python tarfile
    extract to folder named after tar file
    """
    if not os.path.exists(untar_fpath):
        os.makedirs(untar_fpath, mode=0o777)

    if "automation/unwrap_tar" in fpath:
        tar_file = tarfile.open(fpath)
        tar_file.extractall(untar_fpath)
        tar_file.close()

    if os.listdir(untar_fpath):
        return True


def recursive_chmod(tpath):
    """
    For untarred item contents
    to make them accessible to all
    """
    os.chmod(tpath, 0o777)
    for root, dirs, files in os.walk(tpath):
        for dir in dirs:
            dpath = os.path.join(root, dir)
            os.chmod(dpath, 0o777)
        for file in files:
            fpath = os.path.join(root, file)
            os.chmod(fpath, 0o777)


def main():
    """
    Check unwrap_tar path and iterate contents unwrapping/checksum
    verify testing if manifest found in package
    """

    if not os.path.exists(UNTAR_PATH):
        sys.exit(f"Exiting: Error with supplied path: {UNTAR_PATH}")

    log_list = []
    tar_files = [
        x for x in os.listdir(UNTAR_PATH) if x.endswith((".tar", ".TAR"))
    ]
    if len(tar_files) == 0:
        sys.exit(f"{UNTAR_PATH} EMPTY. SCRIPT EXITING.")

    LOGGER.info("========= UNWRAP TAR SCRIPT %s START =====================", TARGET)

    for fname in tar_files:
        fname_log = fname.split(".")[0]
        fpath = os.path.join(UNTAR_PATH, fname)
        log_list.append(f"{str(datetime.datetime.now())[:10]}\tNew file found: {fpath}")
        LOGGER.info("File found to process: %s", fname)
        log_list.append(
            f"{str(datetime.datetime.now())[:10]}\tAttempting extraction using Linux TAR programme..."
        )

        manifest_name = f"{fname}_manifest.md5"
        print(manifest_name, fname)
        data = tarfile.open(fpath)
        print(data.getnames())
        if manifest_name in data.getnames():
            LOGGER.info("Python TAR file needs tarfile extraction - manifest %s matched in TAR", manifest_name)
            extraction_method = "tarfile"
        else:
            LOGGER.info("Linux TAR extraction selected - no manifest found in TAR file")
            extraction_method = "linux"

        extraction_failed = False
        if extraction_method == "linux":
            log_list.append(
                f"{str(datetime.datetime.now())[:10]}\tLinux TAR selected for extraction and starting"
            )
            tic = time.perf_counter()
            untar_fpath = linux_untar_file(fpath)
            toc = time.perf_counter()
            minutes_taken = (toc - tic) // 60
            if not untar_fpath:
                extraction_failed = True
                LOGGER.warning(
                    "Unwrapping failed with Linux TAR."
                )

        else:
            untar_file = fname.split(".tar")[0]
            untar_fpath = os.path.join(UNTAR_PATH, untar_file)
            if not os.path.exists(untar_fpath):
                os.makedirs(untar_fpath, mode=0o777, exist_ok=True)
            log_list.append(
                f"{str(datetime.datetime.now())[:10]}\tPython tarfile selected and starting"
            )
            LOGGER.info("Attemping Python tarfile unwrap now...")

            # Try with Python tarfile
            tic = time.perf_counter()
            py_success = python_tarfile(fpath, untar_fpath)
            toc = time.perf_counter()
            minutes_taken = (toc - tic) // 60
            if not py_success:
                extraction_failed = True
                LOGGER.warning(
                    "Python tarfile has failed to extract content of TAR. Script exiting, TAR needs manual assistance."
                )

        if extraction_failed is True:
            shutil.move(fpath, FAILED)
            error_mssg1 = f"Linux TAR / Python tarfile cannot extract data. Please contact KLC Developers for review. File location: {fpath}"
            error_mssg2 = None
            error_log(
                os.path.join(FAILED, f"{fname_log}_errors.log"),
                error_mssg1,
                error_mssg2,
            )
            if os.path.exists(untar_fpath) and not os.listdir(untar_fpath):
                LOGGER.info(
                    "Moved TAR to failed/ folder. Deleted empty folder: %s",
                    untar_file,
                )
                log_list.append(
                    f"{str(datetime.datetime.now())[:10]}\tMoved TAR to failed/ folder. Deleted empty extraction folder: {untar_file}"
                )
                os.rmdir(untar_fpath)
            elif os.path.exists(untar_fpath) and os.listdir(untar_fpath):
                LOGGER.info(
                    "Moved TAR to failed/ folder. Folder %s has contents, moving to failed/ folder for review",
                    untar_file,
                )
                log_list.append(
                    f"{str(datetime.datetime.now())[:10]}\tMoved TAR to failed/ folder. Folder {untar_file} has contents. Moving to failed/ folder for review"
                )
                shutil.move(untar_fpath, FAILED)
            log_list.append(
                f"{str(datetime.datetime.now())[:10]}\tSkipping further actions for {fname}. Manual assistance needed"
            )
            log_list.append(
                f"{str(datetime.datetime.now())[:10]}\t-------------------------------------------------------------------"
            )
            LOGGER.warning(
                "Skipping futher actions for %s, TAR needs manual assistance.",
                fname,
            )
            build_log(log_list)
            continue

        # Success actions
        recursive_chmod(untar_fpath)

        if extraction_method == "tarfile":
            LOGGER.info("Python tarfile extracted file to path: %s", untar_fpath)
        else:
            LOGGER.info("Linux TAR programme extracted file to path: %s", untar_fpath)
        log_list.append(
            f"{str(datetime.datetime.now())[:10]}\tExtracted TAR file successful: {untar_fpath}"
        )
        log_list.append(
            f"{str(datetime.datetime.now())[:10]}\tExtraction took {minutes_taken} minutes to complete"
        )
        LOGGER.info("It took %s minutes to perform this extraction.", minutes_taken)

        # Do manifest assessment
        if extraction_method == "tarfile":
            # Build checksum manifest of un_tarred file
            local_manifest = get_checksum(untar_fpath)
            local_manifest_path = dump_to_file(untar_fpath, local_manifest)
            log_list.append(
                f"{str(datetime.datetime.now())[:10]}\tGenerating local MD5 manifest for extracted data: {local_manifest_path}"
            )

            # Fetch enclosed MD5 manifest if present
            md5_manifest = os.path.join(untar_fpath, f"{fname}_manifest.md5")
            if os.path.exists(md5_manifest):
                match = True
                LOGGER.info("MD5 manifest for untar item exists: %s", md5_manifest)
                manifest_contents = fetch_checksum_dict(md5_manifest)
                log_list.append(
                    f"{str(datetime.datetime.now())[:10]}\tMD5 manifest extracted from TAR file for comparison"
                )
                for k, v in manifest_contents.items():
                    if local_manifest.get(k) == v:
                        print(f"MD5 match: {k}")
                    elif "ASSETMAP.xml" in k:
                        for key, value in local_manifest.items():
                            if key.endswith("ASSETMAP.xml"):
                                if value == v:
                                    print(f"MD5 match: {k} - {key}")
                                else:
                                    match = False
                    elif "VOLINDEX.xml" in k:
                        for key, value in local_manifest.items():
                            if key.endswith("VOLINDEX.xml"):
                                if value == v:
                                    print(f"MD5 match: {k} - {key}")
                                else:
                                    match = False
                    else:
                        print(f"MD5 does not match: {k}")
                        match = False

                if match:
                    log_list.append(
                        f"{str(datetime.datetime.now())[:10]}\tLocal manifest matches extracted MD5 manifest. File identical to preservation original."
                    )
                    LOGGER.info(
                        "MD5 manifest matches local MD5 manifest. Bit perfect restoration of TARRED file."
                    )
                else:
                    LOGGER.info(
                        "MD5 manifest does not match all items. See manifest for details: %s",
                        local_manifest_path,
                    )
                    log_list.append(
                        f"{str(datetime.datetime.now())[:10]}\tMD5 manifest cannot be fully matched to extracted MD5 manifest."
                    )
                    error_mssg1 = f"MD5 manifests do not match from TAR file, and unwrapped TAR folder contents: {local_manifest_path}"
                    error_mssg2 = None
                    error_log(
                        os.path.join(FAILED, f"{fname_log}_errors.log"),
                        error_mssg1,
                        error_mssg2,
                    )
        else:
            LOGGER.info(
                "MD5 manifest was not extracted from TAR file. No comparison possible."
            )
            log_list.append(
                f"{str(datetime.datetime.now())[:10]}\tNo MD5 manifest extracted from TAR file. No comparison possible."
            )

        shutil.move(fpath, COMPLETED)
        LOGGER.info("%s file moved to COMPLETED path: %s", fname, COMPLETED)
        log_list.append(
            f"{str(datetime.datetime.now())[:10]}\tMoved TAR to completed/ folder for manual deletion."
        )
        log_list.append(
            f"{str(datetime.datetime.now())[:10]}\t-------------------------------------------------------------------"
        )
        if os.path.exists(os.path.join(FAILED, f"{fname_log}_errors.log")):
            os.rename(
                os.path.join(FAILED, f"{fname_log}_errors.log"),
                os.path.join(COMPLETED, f"{fname_log}.log"),
            )
        build_log(log_list)

    LOGGER.info("========= UNWRAP TAR CHECKSUM SCRIPT END =======================")


def fetch_checksum_dict(md5_manifest):
    """
    Collect contents of Manifest using JSON load
    """
    with open(md5_manifest, "r") as file:
        data = json.load(file)

        if isinstance(data, dict):
            return data


def get_checksum(fpath):
    """
    Using file path, generate file checksum
    return as dictionary
    """

    md5s = {}
    for root, _, files in os.walk(fpath):
        for file in files:
            hsh = hashlib.md5()
            with open(os.path.join(root, file), "rb") as md5_file:
                for chunk in iter(lambda: md5_file.read(65536), b""):
                    hsh.update(chunk)
                if file in ["ASSETMAP", "VOLINDEX", "ASSETMAP.xml", "VOLINDEX.xml"]:
                    folder_prefix = os.path.basename(root)
                    file = f"{folder_prefix}_{file}"
                md5s[file] = hsh.hexdigest()
    return md5s


def dump_to_file(untar_path, md5_dct):
    """
    Write md5 manifest to file locally
    """
    md5_path = f"{untar_path}_unwrap_manifest.md5"

    try:
        with open(md5_path, "w+") as json_file:
            json_file.write(json.dumps(md5_dct, indent=4))
            json_file.close()
    except Exception as exc:
        LOGGER.warning("make_manifest(): FAILED to create JSON %s", exc)

    if os.path.exists(md5_path):
        return md5_path


def build_log(message_list):
    """
    Add local log messages to file
    """
    if not os.path.exists(LOCAL_LOG):
        with open(LOCAL_LOG, "x") as file:
            file.close()
    with open(LOCAL_LOG, "a") as file:
        for line in message_list:
            file.write(f"{line}\n")


def error_log(fpath, message, kandc):
    """
    If needed, write error log
    for incomplete sequences.
    """
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not kandc:
        with open(fpath, "a+") as log:
            log.write(f"unwrap_tar {ts}: {message}.\n\n")
            log.close()
    else:
        with open(fpath, "a+") as log:
            log.write(f"unwrap_tar {ts}: {message}.\n")
            log.write(
                f"\tPlease contact the Knowledge and Collections Developer {kandc}.\n\n"
            )
            log.close()


if __name__ == "__main__":
    main()
