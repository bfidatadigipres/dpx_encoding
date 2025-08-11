#!/usr/bin/env python3

"""
USES SYS.ARGV[] to receive path to item for TAR.
Complete TAR wrapping using Python3 tarfile
on folder or file supplied in tar watch folder.
Compare TAR contents to original using MD5 hash.

Steps:
1. Assess if item supplied is folder or file
   Ensure the filename matches CID item record
2. Initiate TAR wrapping with zero compression
3. Generate MD5 dict for original folder
4. Generate MD5 dict for internals of TAR
5. Compare to ensure identical:
   Yes. Output MD5 to manifest and add into TAR file
        Delete the source file/folder
        Update details to local log / CID item record.
        Move TAR file to autoingest. Update all logs.
   No. Delete faulty TAR.
       Output warning to Local log and leave file
       for retry at later date.
       Move TAR file to failures folder for retry
6. Clean up source folder if successful
7. Update logs

2025
"""

import datetime
import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
import time

# import tarfile
import py7zr

if not len(sys.argv) >= 2:
    sys.exit("Exiting. Supplied path argument is missing.")
if not os.path.exists(sys.argv[1]):
    sys.exit(f"Exiting. Supplied path does not exist: {sys.argv[1]}")

# Global pathsL
LOCAL_PATH = sys.argv[1]
AUTO_TAR = LOCAL_PATH.split("7zip")[0]
TAR_FAIL = os.path.join(LOCAL_PATH, "failures/")
COMPLETED = os.path.join(LOCAL_PATH, "completed/")
CHECKSUM = os.path.join(LOCAL_PATH, "checksum_manifests/")
LOCAL_LOG = os.environ["LOG_PATH"]
# if "/mnt/bp_nas" in LOCAL_PATH:
#     parent_path = LOCAL_PATH.split("tar_preservation")[0]
# else:
parent_path = LOCAL_PATH.split("/automation")[0]
LOG = os.path.join(LOCAL_LOG, "tar_wrapping_7zip_checksum.log")
CID_API = os.environ["CID_API4"]

# Logging config
LOGGER = logging.getLogger(f"tar_wrapping_checksum_{LOCAL_PATH.replace('/', '_')}")
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


def tar_item(fpath):
    """
    Make tar path from supplied filepath
    Use tarfile to create TAR
    """
    split_path = os.path.split(fpath)
    print(split_path)
    tfile = f"{split_path[1]}.tar"
    tar_path = os.path.join(split_path[0], tfile)
    print(f"the tar path isss: {tar_path}")
    if os.path.exists(tar_path):
        print(f"tar already exists in {tar_path}")
        LOGGER.warning("tar_item(): FILE ALREADY EXISTS %s", tar_path)
        return None

    try:
        with py7zr.SevenZipFile(
            tar_path, mode="w", filters=[{"id": py7zr.FILTER_COPY}]
        ) as z:
            z.writeall(fpath, arcname=f"{split_path[1]}")
        return tar_path

    except Exception as exc:
        print("error with tar wrapping")
        LOGGER.warning("tar_item(): ERROR WITH TAR WRAP %s", exc)
        return None


def get_tar_checksums(tar_path, folder):
    """
    Open tar file and read/generate MD5 sums
    and return dct {filename: hex}
    """
    data = {}
    with tempfile.TemporaryDirectory() as tmpdir:
        with py7zr.SevenZipFile(tar_path, mode="r") as archive:
            archive.extractall(path=tmpdir)

        for root, _, files in os.walk(tmpdir):
            for f in files:
                rel_path = os.path.relpath(os.path.join(root, f), tmpdir)

                hash_md5 = hashlib.md5()
                with open(os.path.join(root, f), "rb") as file:
                    for chunk in iter(lambda: file.read(65536), b""):
                        hash_md5.update(chunk)

                if not folder:
                    file_key = os.path.basename(rel_path)
                    data[file_key] = hash_md5.hexdigest()
                else:
                    data[rel_path] = hash_md5.hexdigest()

        return data


def get_checksum(fpath):
    """
    Using file path, generate file checksum
    return as list with filename
    """
    data = {}
    pth, file = os.path.split(fpath)
    if file in ["ASSETMAP", "VOLINDEX", "ASSETMAP.xml", "VOLINDEX.xml"]:
        folder_prefix = os.path.basename(pth)
        file = f"{folder_prefix}_{file}"
    hash_md5 = hashlib.md5()
    with open(fpath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hash_md5.update(chunk)
            data[fpath] = hash_md5.hexdigest()
        f.close()
    return data


def make_manifest(tar_path, md5_dct):
    """
    Output md5 to JSON file format and add to TAR file
    """
    md5_path = f"{tar_path}_manifest.md5"

    try:
        with open(md5_path, "w+") as json_file:
            json_file.write(json.dumps(md5_dct, indent=4))
            json_file.close()
    except Exception as exc:
        LOGGER.warning("make_manifest(): FAILED to create JSON %s", exc)

    if os.path.exists(md5_path):
        return md5_path


def get_valid_folder_and_files(fullpath):
    skip_folder = {"completed", "failures", "checksum_manifests"}

    valid_folder_and_file = []

    for dirpath, dirname, files in os.walk(fullpath):
        dirname = [d for d in dirname if d.lower() not in skip_folder]
        for dirs in dirname:
            valid_folder_and_file.append(os.path.join(dirpath, dirs))

        for file in files:
            full_file_path = os.path.join(dirpath, file)
            if os.path.isfile(full_file_path) and not file.endswith(
                (
                    ".dpx",
                    ".DPX",
                    ".tif",
                    ".TIF",
                    ".tiff",
                    ".TIFF",
                    ".jp2",
                    ".j2k",
                    ".jpf",
                    ".jpm",
                    ".jpg2",
                    ".j2c",
                    ".jpc",
                    ".jpx",
                    ".mj2",
                    ".log",
                    ".tar",
                    ".md5",
                )
            ):
                valid_folder_and_file.append(full_file_path)
        return valid_folder_and_file


def main():
    """
    Receive SYS.ARGV and check path exists or is file/folder
    Generate checksums for all folder contents/single file
    TAR Wrap, then make checksum for inside of TAR contents
    Compare checksum manifests, if match add into TAR and close.
    Delete original file, move TAR to completed folder.
    """

    if len(sys.argv) != 2:
        LOGGER.warning("SCRIPT EXIT: Error with shell script input:\n %s", sys.argv)
        sys.exit()

    fullpath = sys.argv[1]
    file_folder_list = get_valid_folder_and_files(fullpath)

    if not os.path.exists(fullpath):
        sys.exit("Supplied path does not exists. Please try again.")

    if fullpath.endswith(".md5"):
        sys.exit("Supplied path is MD5. Skipping.")
    # print(f"auto tar: {AUTO_TAR}")
    # # print(f"logs: {LOG}")
    # # print(f"completed: {COMPLETED}")
    # # print(f"checksum: {CHECKSUM}")
    # # print(f"filepaths: {LOCAL_PATH}")
    # # print(f"parent files: {parent_path}")
    # # print(f'filesss :{os.path.split(sys.argv[1])}')

    for struct_file in file_folder_list:
        log = []
        log.append(f"==== New path for TAR wrap: {struct_file} ====")
        LOGGER.info(
            "==== TAR Wrapping Check script start ==============================="
        )
        LOGGER.info("Path received for TAR wrap using Python3 py7zr: %s", struct_file)
        tar_source = os.path.basename(struct_file)
        # Calculate checksum manifest for supplied fullpath
        local_md5 = {}
        print(struct_file)
        directory = False
        if os.path.isdir(struct_file):
            print("its a folder")
            log.append("Supplied path for TAR wrap is directory")
            LOGGER.info("Supplied path for TAR wrap is directory")
            directory = True

        if directory:
            for root, _, files in os.walk(struct_file):
                for file in files:
                    dct = get_checksum(os.path.join(root, file))
                    local_md5.update(dct)
        else:
            local_md5 = get_checksum(struct_file)
            log.append("Path is not a directory and will be wrapped alone")

        LOGGER.info("Checksums for local files (excluding DPX, TIF):")
        log.append("Checksums for local files (excluding DPX, TIF):")
        for key, val in local_md5.items():
            if not key.endswith(
                (
                    ".dpx",
                    ".DPX",
                    ".tif",
                    ".TIF",
                    ".TIFF",
                    ".tiff",
                    ".jp2",
                    ".j2k",
                    ".jpf",
                    ".jpm",
                    ".jpg2",
                    ".j2c",
                    ".jpc",
                    ".jpx",
                    ".mj2",
                    ".md5",
                )
            ):
                data = f"{val} -- {key}"
                LOGGER.info("\t%s", data)
                log.append(f"\t{data}")

        # Tar folder
        log.append("Beginning TAR wrap now...")
        tar_path = tar_item(struct_file)
        if not tar_path:
            log.append("TAR WRAP FAILED. SCRIPT EXITING!")
            LOGGER.warning("TAR wrap failed for file: %s", struct_file)
            for item in log:
                local_logs(LOCAL_PATH, item)
            error_mssg1 = f"TAR wrap failed for folder {tar_source}. No TAR file found:\n\t{tar_path}"
            error_mssg2 = "if the TAR wrap has failed for an inexplicable reason"
            error_log(
                os.path.join(TAR_FAIL, f"{tar_source}_errors.log"),
                error_mssg1,
                error_mssg2,
            )
            sys.exit(f"EXIT: TAR wrap failed for {struct_file}")

        # Calculate checksum manifest for TAR folder
        if directory:
            tar_content_md5 = get_tar_checksums(tar_path, tar_source)
        else:
            tar_content_md5 = get_tar_checksums(tar_path, "")

        log.append(
            "Checksums from TAR wrapped contents (excluding DPX, TIF, JPEG2000):"
        )
        LOGGER.info(
            "Checksums for TAR wrapped contents (excluding DPX, TIF, JPEG2000):"
        )
        for key, val in tar_content_md5.items():
            if not key.endswith(
                (
                    ".dpx",
                    ".DPX",
                    ".tif",
                    ".TIF",
                    ".tiff",
                    ".TIFF",
                    ".jp2",
                    ".j2k",
                    ".jpf",
                    ".jpm",
                    ".jpg2",
                    ".j2c",
                    ".jpc",
                    ".jpx",
                    ".mj2",
                    ".md5",
                )
            ):
                data = f"{val} -- {key}"
                LOGGER.info("\t%s", data)
                log.append(f"\t{data}")

        for key in tar_content_md5.keys():
            for value in list(local_md5.keys()):
                if key in value:
                    local_md5[key] = local_md5.pop(value)
                else:
                    local_logs(struct_file, "it doesnt match")

        # Compare manifests
        if local_md5 == tar_content_md5:
            log.append(
                "MD5 Manifests match, adding manifest to TAR file and moving to autoingest."
            )
            LOGGER.info("MD5 manifests match.")
            md5_manifest = make_manifest(tar_path, tar_content_md5)
            print(md5_manifest)
            if not md5_manifest:
                LOGGER.warning("Failed to write TAR checksum manifest to JSON file.")
                shutil.move(tar_path, os.path.join(TAR_FAIL, f"{tar_source}.tar"))
                for item in log:
                    local_logs(LOCAL_PATH, item)
                error_mssg1 = f"TAR checksum manifest was not created for new TAR file:\n\t{tar_path}\n\tTAR file moved to failures folder"
                error_mssg2 = "if no explicable reason for this failure (ie, file was moved mid way through processing)"
                error_log(
                    os.path.join(TAR_FAIL, f"{tar_source}_errors.log"),
                    error_mssg1,
                    error_mssg2,
                )
                sys.exit("Script exit: TAR file MD5 Manifest failed to create")

            LOGGER.info(
                "TAR checksum manifest created. Adding to TAR file %s", tar_path
            )
            # # check with dms team to see if they want a manifest file in the root directory of the TAR?
            try:
                arc_path = os.path.split(md5_manifest)
                with py7zr.SevenZipFile(tar_path, mode="w") as z:
                    z.write(md5_manifest, arcname=f"{arc_path[1]}")
            except Exception as exc:
                LOGGER.warning(
                    "Unable to add MD5 manifest to TAR file. Moving TAR file to failures folder.\n%s",
                    exc,
                )
                shutil.move(tar_path, os.path.join(TAR_FAIL, f"{tar_source}.tar"))
                # Write all log items in block
                for item in log:
                    local_logs(LOCAL_PATH, item)
                error_mssg1 = f"TAR checksum manifest could not be added to TAR file:\n\t{tar_path}\n\tTAR file moved to failures folder"
                error_mssg2 = "if no explicable reason for this failure (ie, file was moved mid way through processing)"
                error_log(
                    os.path.join(TAR_FAIL, f"{tar_source}_errors.log"),
                    error_mssg1,
                    error_mssg2,
                )
                sys.exit("Failed to add MD5 manifest To TAR file. Script exiting")

            LOGGER.info(
                "TAR MD5 manifest added to TAR file. Getting wholefile TAR checksum for logs"
            )
            whole_md5 = md5_hash(tar_path)
            if whole_md5:
                log.append(f"Whole TAR MD5 checksum for TAR file: {whole_md5}")
                LOGGER.info("Whole TAR MD5 checksum for TAR file: %s", whole_md5)
            else:
                LOGGER.warning("Failed to retrieve whole TAR MD5 sum")

            # Get complete size of file following TAR wrap
            file_stats = os.stat(tar_path)
            file_size = file_stats.st_size
            log.append(f"File size is {file_size} bytes")
            LOGGER.info("File size is %s bytes.", file_size)

            try:
                LOGGER.info("Moving sequence to completed/: %s", struct_file)
                shutil.move(struct_file, COMPLETED)
            except Exception as err:
                LOGGER.warning(
                    "Source folder failed to move to completed/ path:\n%s", err
                )
            try:
                LOGGER.info(
                    "Moving MD5 manifest to checksum_manifest folder %s", CHECKSUM
                )
                print(CHECKSUM)
                print(f"{md5_manifest}, {CHECKSUM}")
                shutil.move(md5_manifest, CHECKSUM)
            except Exception as err:
                LOGGER.warning("MD5 manifest move failed:\n%s", err)

            # Tidy away error_log following successful creation
            resolved_error_log = f"resolved_{tar_source}_errors.log"
            if os.path.isfile(os.path.join(TAR_FAIL, f"{tar_source}_errors.log")):
                os.rename(
                    os.path.join(TAR_FAIL, f"{tar_source}_errors.log"),
                    os.path.join(TAR_FAIL, resolved_error_log),
                )
        else:
            LOGGER.warning(
                "Manifests do not match.\nLocal:\n%s\nTAR:\n%s",
                local_md5,
                tar_content_md5,
            )
            LOGGER.warning(
                "Moving TAR file to failures, leaving file/folder for retry."
            )
            log.append(
                "MD5 manifests do not match. Moving TAR file to failures folder for retry"
            )
            shutil.move(tar_path, os.path.join(TAR_FAIL, f"{tar_source}.tar"))
            error_mssg1 = f"MD5 checksum manifests do not match for source folder and TAR file:\n\t{tar_path}\n\tTAR file moved to failures folder"
            error_mssg2 = "if this checksum comparison fails multiple times"
            error_log(
                os.path.join(TAR_FAIL, f"{tar_source}_errors.log"),
                error_mssg1,
                error_mssg2,
            )

        log.append(f"==== Log actions complete: {struct_file} ====")
        log.append(
            "==== TAR Wrapping Check script END ================================="
        )
        # Write all log items in block
        for item in log:
            local_logs(LOCAL_PATH, item)

        LOGGER.info(
            "==== TAR Wrapping Check script END ================================="
        )


def md5_hash(tar_file):
    """
    Make whole file TAR MD5 checksum
    """
    try:
        hash_md5 = hashlib.md5()
        with open(tar_file, "rb") as fname:
            for chunk in iter(lambda: fname.read(65536), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    except Exception as err:
        print(err)
        return None


def local_logs(fpth, data):
    """
    Output local log data for team
    to monitor TAR wrap process
    """
    local_log = os.path.join(fpth, "tar_wrapping_7zip_checksum.log")
    timestamp = str(datetime.datetime.now())

    if not os.path.isfile(local_log):
        with open(local_log, "x") as log:
            log.close()

    with open(local_log, "a") as log:
        log.write(f"{timestamp[0:19]} - {data}\n")
        log.close()


def error_log(fpath, message, kandc):
    """
    If needed, write error log
    for incomplete sequences.
    """
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not kandc:
        with open(fpath, "a+") as log:
            log.write(f"tar_wrapping {ts}: {message}.\n\n")
            log.close()
    else:
        with open(fpath, "a+") as log:
            log.write(f"tar_wrapping {ts}: {message}.\n")
            log.write(
                f"- Please contact the Knowledge and Collections Developer {kandc}.\n\n"
            )
            log.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"time it took to run the code: {end_time-start_time}")
