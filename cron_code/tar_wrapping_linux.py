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
import subprocess
import sys
import tempfile
import time
from deepdiff import DeepDiff

sys.path.append(os.environ["CODE"])
import adlib_v3 as adlib

if not len(sys.argv) >= 2:
    sys.exit("Exiting. Supplied path argument is missing.")
if not os.path.exists(sys.argv[1]):
    sys.exit(f"Exiting. Supplied path does not exist: {sys.argv[1]}")

# Global paths
# LOCAL_PATH = os.path.split(sys.argv[1])[0]
LOCAL_PATH = sys.argv[1]
TAR_FAIL = os.path.join(LOCAL_PATH, "failures/")
COMPLETED = os.path.join(LOCAL_PATH, "completed/")
CHECKSUM = os.path.join(LOCAL_PATH, "checksum_manifests/")
LOCAL_LOG = os.environ["LOG_PATH"]
LOG = os.path.join(LOCAL_LOG, "tar_wrapping_linux_checksum.log")
CID_API = os.environ["CID_API4"]

# Logging config
LOGGER = logging.getLogger(f"tar_wrapping_checksum_{LOCAL_PATH.replace('/', '_')}")
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


def tar_item(fpath, retries=3, delay=2):
    """
    Create a tar archive of the supplied file/directory using GNU tar.
    Adds retry logic to handle NFS 'Stale file handle' errors.
    """
    split_path = os.path.split(fpath)
    tfile = f"{split_path[1]}.tar"
    tar_path = os.path.join(split_path[0], tfile)

    os.makedirs(os.path.dirname(tar_path), exist_ok=True)

    try:
        os.stat(fpath)
    except FileNotFoundError:
        print(f"[ERROR] tar_item(): path does not exist → {fpath}")
        return None
    except OSError as e:
        print(f"[ERROR] tar_item(): cannot stat {fpath}: {e}")
        return None

    command = [
        "/bin/tar",
        "-cvf",
        tar_path,
        "-C",
        os.path.dirname(fpath),
        os.path.basename(fpath),
    ]
    print("Command:", " ".join(command))

    for attempt in range(1, retries + 1):
        try:
            result = subprocess.run(command, check=True, text=True, capture_output=True)
            print("[OK] Tar creation successful on attempt", attempt)
            print("Files included:\n", result.stdout)
            return tar_path
        except subprocess.CalledProcessError as e:
            if "Stale file handle" in e.stderr and attempt < retries:
                print(
                    f"[WARN] Stale file handle → retrying in {delay}s (attempt {attempt})"
                )
                time.sleep(delay)
                continue
            print(f"[ERROR] tar failed (attempt {attempt}):\n{e.stderr}")
            return None
        except Exception as exc:
            print("tar_item(): unexpected error:", exc)
            return None

    return tar_path


def get_tar_checksums(tar_path, folder):
    """
    Extract tar file into a temp dir and compute MD5 for each file.
    Returns {filename: checksum}.
    """
    data = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        cmd = ["/bin/tar", "-C", tmpdir, "-xvf", tar_path]
        result = subprocess.run(cmd, capture_output=True, text=True)

        extracted_files = result.stdout.strip().split("\n")
        print(extracted_files)

        
        print("tar stdout:\n", result.stdout)
        print("tar stderr:\n", result.stderr)
        print("exit code:", result.returncode)

        if result.returncode != 0:
            raise RuntimeError(
                f"[ERROR] tar extraction failed for {tar_path} (exit {result.returncode})"
            )

        for file in extracted_files:
            item = os.path.join(tmpdir, file)
            if os.path.isdir(item):
                print(f"{item} is a dir")
                continue

            pth, fname = os.path.split(item)
            if fname in ["ASSETMAP", "VOLINDEX", "ASSETMAP.xml", "VOLINDEX.xml"]:
                folder_prefix = os.path.basename(pth)
                fname = f"{folder_prefix}_{fname}"

            hash_md5 = hashlib.md5()
            with open(item, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    hash_md5.update(chunk)

            if not folder:
                data[os.path.basename(fname)] = hash_md5.hexdigest()
            else:
                data[fname] = hash_md5.hexdigest()

    return data


def get_checksum(fpath):
    """
    Compute MD5 checksum of a single file.
    Returns {filename: checksum}.
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

    data[file] = hash_md5.hexdigest()
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
        return md5_path.replace("/generate_tar", "")


# N_10635929_08of08_short
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
    Delete original file, move TAR to autoingest path.
    """

    if len(sys.argv) != 2:
        LOGGER.warning("SCRIPT EXIT: Error with shell script input:\n %s", sys.argv)
        sys.exit()

    fullpath = sys.argv[1]
    file_folder_list = get_valid_folder_and_files(fullpath)

    if not os.path.exists(fullpath):
        sys.exit("Supplied path does not exists. Please try again.")

    for tar_file in file_folder_list:
        print(f"file processing: {tar_file}")
        log = []
        log.append(f"==== New path for TAR wrap: {fullpath} ====")
        LOGGER.info(
            "==== TAR Wrapping Check script start ==============================="
        )
        LOGGER.info("Path received for TAR wrap using Python3 tarfile: %s", fullpath)
        tar_source = os.path.basename(fullpath)

        # Calculate checksum manifest for supplied fullpath
        local_md5 = {}
        directory = False
        if os.path.isdir(tar_file):
            log.append("Supplied path for TAR wrap is directory")
            LOGGER.info("Supplied path for TAR wrap is directory")
            directory = True

        if directory:
            for root, _, files in os.walk(tar_file):
                for file in files:
                    print(file)
                    dct = get_checksum(os.path.join(root, file))
                    local_md5.update(dct)

        else:
            local_md5 = get_checksum(fullpath)
            log.append("Path is not a directory and will be wrapped alone")
        print(f"local_md5: {local_md5}")

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
                )
            ):
                data = f"{val} -- {key}"
                LOGGER.info("\t%s", data)
                log.append(f"\t{data}")

        # Tar folder
        log.append(f"Beginning TAR wrap now for {tar_file}...")
        print(f"tar_file: {tar_file}")
        tar_path = tar_item(tar_file)
        print(tar_path)
        tar_files = os.path.split(tar_path)[1]
        print(tar_files)
        if not tar_path:
            log.append("TAR WRAP FAILED. SCRIPT EXITING!")
            LOGGER.warning("TAR wrap failed for file: %s", tar_files)
            for item in log:
                local_logs(LOCAL_PATH, item)
            error_mssg1 = f"TAR wrap failed for folder {tar_source}. No TAR file found:\n\t{tar_files}"
            error_mssg2 = "if the TAR wrap has failed for an inexplicable reason"
            error_log(
                os.path.join(TAR_FAIL, f"{tar_source}_errors.log"),
                error_mssg1,
                error_mssg2,
            )
            sys.exit(f"EXIT: TAR wrap failed for {tar_files}")

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
                )
            ):
                data = f"{val} -- {key}"
                LOGGER.info("\t%s", data)
                log.append(f"\t{data}")
        print(f"tar_content_md5: {tar_content_md5}")

        log.append(f"tar_content_md5: {tar_content_md5}")
        log.append(f"local_md5: {local_md5}")

        # Compare manifests
        if local_md5 == tar_content_md5:
            log.append(
                "MD5 Manifests match, adding manifest to TAR file and moving to autoingest."
            )
            LOGGER.info("MD5 manifests match.")
            md5_manifest = make_manifest(tar_path, tar_content_md5)
            if not md5_manifest:
                LOGGER.warning("Failed to write TAR checksum manifest to JSON file.")
                shutil.move(tar_path, os.path.join(TAR_FAIL, f"{tar_file}.tar"))
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
                LOGGER.info("Moving sequence to completed/: %s", tar_file)
                shutil.move(tar_file, COMPLETED)
            except Exception as err:
                LOGGER.warning(
                    "Source folder failed to move to completed/ path:\n%s", err
                )
            try:
                LOGGER.info(
                    "Moving MD5 manifest to checksum_manifest folder %s", CHECKSUM
                )
                shutil.move(md5_manifest, CHECKSUM)
            except Exception as err:
                LOGGER.warning("MD5 manifest move failed:\n%s", err)

            # Tidy away error_log following successful creation
            resolved_error_log = f"resolved_{tar_file}_errors.log"
            if os.path.isfile(os.path.join(TAR_FAIL, f"{tar_file}_errors.log")):
                os.rename(
                    os.path.join(TAR_FAIL, f"{tar_file}_errors.log"),
                    os.path.join(TAR_FAIL, resolved_error_log),
                )

            # Delete source for TAR wrapped file/folder
            if os.path.isdir(os.path.join(COMPLETED, tar_source)):
                LOGGER.info("Deleting source folder %s", tar_source)
                shutil.rmtree(os.path.join(COMPLETED, tar_source))
            elif os.path.isfile(os.path.join(COMPLETED, tar_source)):
                LOGGER.info("Deleting source file %s", tar_source)
                os.remove(os.path.join(COMPLETED, tar_source))

        else:
            diff_md5 = DeepDiff(local_md5, tar_content_md5)
            LOGGER.warning(
                "Manifests do not match.\nLocal:\n%s\nTAR:\n%s",
                local_md5,
                tar_content_md5,
            )
            LOGGER.warning("Difference between md5: \n", diff_md5)
            LOGGER.warning(
                "Moving TAR file to failures, leaving file/folder for retry."
            )
            log.append(
                "MD5 manifests do not match. Moving TAR file to failures folder for retry"
            )
            shutil.move(tar_path, os.path.join(TAR_FAIL, f"{tar_file}.tar"))
            error_mssg1 = f"MD5 checksum manifests do not match for source folder and TAR file:\n\t{tar_path}\n\tTAR file moved to failures folder"
            error_mssg2 = "if this checksum comparison fails multiple times"
            error_log(
                os.path.join(TAR_FAIL, f"{tar_source}_errors.log"),
                error_mssg1,
                error_mssg2,
            )

        log.append(f"==== Log actions complete: {fullpath} ====")
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


def local_logs(fullpath, data):
    """
    Output local log data for team
    to monitor TAR wrap process
    """
    local_log = os.path.join(fullpath, "tar_wrapping_linux_checksum.log")
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
