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


# run validation if tar path is valid,
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

    except py7zr.exceptions.CrcError as crc:
        print("checksum mismatch incoming")
        LOGGER.warning("Tar path: %s: checksum mismatch error!!!", tar_path)
        return None

    except Exception as exc:
        print("error with tar wrapping")
        LOGGER.warning("tar_item(): ERROR WITH TAR WRAP %s", exc)
        return None


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


def full_integrity_check_with_extraction(archive_path):
    """
    Full integrity check by extracting entire archive
    """
    errors = []
    extracted_files = []
    
    try:
        if not os.path.exists(archive_path):
            errors.append(f"Archive file not found: {archive_path}")
            return False, errors, []

        with tempfile.TemporaryDirectory() as tmpdir:
            print(f"Extracting archive to temporary directory...")
            
            # Extract everything
            with py7zr.SevenZipFile(archive_path, mode="r") as archive:
                # Get expected file list first
                file_list = archive.list()
                expected_files = [f for f in file_list if f.uncompressed > 0]
                expected_dirs = len(file_list) - len(expected_files)

                expected_files_dict = {entry.filename: entry for entry in expected_files}
                
                print(f"Expected: {len(expected_files)} files, {expected_dirs} directories")
                
                # Extract all
                archive.extractall(path=tmpdir)

            # Verify extracted files
            print("Verifying extracted files...")
            for root, _, files in os.walk(tmpdir):
                for f in files:
                    full_path = os.path.join(root, f)
                    rel_path = os.path.relpath(full_path, tmpdir)
                    normalized_path = rel_path.replace(os.sep, '/')
                    
                    # Check if file exists and is readable
                    if os.path.exists(full_path):
                        try:
                            file_size = os.path.getsize(full_path)
                            matching_entry = expected_files_dict.get(normalized_path)
                            
                            if matching_entry:
                                if file_size == matching_entry.uncompressed:
                                    extracted_files.append({
                                        'path': rel_path,
                                        'size': file_size,
                                        'verified': True
                                    })
                                else:
                                    errors.append(f"Size mismatch in {rel_path}: {file_size:,} vs {matching_entry.uncompressed:,}")
                            else:
                                # File extracted but not in archive listing (shouldn't happen)
                                extracted_files.append({
                                    'path': rel_path,
                                    'size': file_size,
                                    'verified': False
                                })
                                
                        except Exception as e:
                            errors.append(f"Error reading extracted file {rel_path}: {e}")
                    else:
                        errors.append(f"Expected file not found: {rel_path}")

            # Summary
            verified_count = sum(1 for f in extracted_files if f['verified'])
            total_size = sum(f['size'] for f in extracted_files)
            
            print(f"✓ Extraction completed: {len(extracted_files)} files extracted")
            print(f"✓ Verification: {verified_count}/{len(extracted_files)} files verified")
            print(f"✓ Total size: {total_size:,} bytes")
            
            # Check if we got all expected files
            if len(extracted_files) != len(expected_files):
                errors.append(f"File count mismatch: extracted {len(extracted_files)}, expected {len(expected_files)}")
            
            success = len(errors) == 0 and verified_count == len(extracted_files)
            return success, errors

    except py7zr.exceptions.Bad7zFile:
        errors.append("Invalid or corrupted 7z archive format")
        return False, errors, []
    except Exception as e:
        errors.append(f"Archive extraction failed: {e}")
        return False, errors, []


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

    for tar_file in file_folder_list:
        log = []
        log.append(f"==== New path for TAR wrap: {tar_file} ====")
        LOGGER.info(
            "==== TAR Wrapping Check script start ==============================="
        )
        LOGGER.info("Path received for TAR wrap using Python3 py7zr: %s", tar_file)
        tar_source = os.path.basename(tar_file)
        print(tar_file)
        if os.path.isdir(tar_file):
            print(f"It's a folder: {tar_file}")
            log.append("Supplied path for TAR wrap is directory")
            LOGGER.info("Supplied path for TAR wrap is directory")
        else:
            print(f"It's a file: {tar_file}")
            log.append("Supplied path for TAR wrap is a file.")
            LOGGER.info("Supplied path for TAR wrap is a file.")

        log.append("Beginning TAR wrap now...")
        tar_path = tar_item(tar_file)
        ## do validationnnnn hereeeeee!!!!!
        success, errors = full_integrity_check_with_extraction(tar_path)
        if not success:
            log.append(f"Integrity test failed for TAR file: {tar_path}")
            LOGGER.warning("Integrity test failed for TAR file: %s", tar_path)
            for err in errors:
                log.append(f"Error: {err}")
                LOGGER.error("Error during integrity test: %s", err)

            # Move the faulty TAR to failures folder
            shutil.move(tar_path, os.path.join(TAR_FAIL, f"{tar_source}.tar"))
            for item in log:
                local_logs(LOCAL_PATH, item)
            sys.exit(f"EXIT: Integrity test failed for {tar_file}")

        ### if tar_path is None, then we have a problem
        if not tar_path:
            log.append("TAR WRAP FAILED. SCRIPT EXITING!")
            LOGGER.warning("TAR wrap failed for file: %s", tar_file)
            for item in log:
                local_logs(LOCAL_PATH, item)
            error_mssg1 = f"TAR wrap failed for folder {tar_source}. No TAR file found:\n\t{tar_path}"
            error_mssg2 = "if the TAR wrap has failed for an inexplicable reason"
            error_log(
                os.path.join(TAR_FAIL, f"{tar_source}_errors.log"),
                error_mssg1,
                error_mssg2,
            )
            shutil.move(tar_path, os.path.join(TAR_FAIL, f"{tar_source}.tar"))
            for item in log:
                local_logs(LOCAL_PATH, item)
            sys.exit(f"EXIT: TAR wrap failed for {tar_file}")

        if os.path.isfile(tar_path):
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
                LOGGER.info("Moving sequence to completed/: %s", tar_file)
                shutil.move(tar_file, COMPLETED)
            except Exception as err:
                LOGGER.warning(
                    "Source folder failed to move to completed/ path:\n%s", err
                )

        log.append(f"==== Log actions complete: {tar_file} ====")
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
