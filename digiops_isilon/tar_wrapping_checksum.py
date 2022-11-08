#!/usr/bin/env python3

'''
USES SYS.ARGV[] to receive path to item for TAR.
Complete TAR wrapping using Python3 tarfile
on folder or file supplied in tar watch folder.
Compare TAR contents to original using MD5 hash.

Steps:
1. Assess if item supplied is folder or file
2. Initiate TAR wrapping with zero compression
3. Generate MD5 dict for original folder
4. Generate MD5 dict for internals of TAR
5. Compare to ensure identical:
   Yes. Output MD5 to manifest and add into TAR file
        Move original folder to 'delete' folder
        Move completed closed() TAR to autoingest.
        Update details to local log.
   No. Delete faulty TAR.
       Output warning to Local log and leave file
       for retry at later date.

Joanna White
2022
'''

import os
import sys
import json
import shutil
import tarfile
import logging
import hashlib
import datetime

# Global paths
LOCAL_PATH = os.environ['IS_DIGITAL']
AUTO_TAR = os.path.join(LOCAL_PATH, os.environ['DPX_WRAP'])
DELETE_TAR = os.path.join(LOCAL_PATH, os.environ['TO_DELETE'])
TAR_FAIL = os.path.join(LOCAL_PATH, os.environ['TAR_FAIL'])
CHECKSUM = os.path.join(LOCAL_PATH, os.environ['TAR_CHECKSUM'])
OVERSIZE = os.path.join(LOCAL_PATH, os.environ['CURRENT_ERRORS'], 'oversized_sequences/')
ERROR_LOG = os.path.join(LOCAL_PATH, os.environ['CURRENT_ERRORS'], 'oversize_files_error.log')
AUTOINGEST = os.path.join(LOCAL_PATH, 'Finished', os.environ['AUTOINGEST_STORE'])
LOG = os.path.join(LOCAL_PATH, os.environ['DPX_SCRIPT_LOG'], 'tar_wrapping_checksum.log')

# Logging config
LOGGER = logging.getLogger('tar_wrapping_digiops')
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


def tar_file(fpath):
    '''
    Make tar path from supplied filepath
    Use tarfile to create TAR
    '''
    split_path = os.path.split(fpath)
    tfile = f"{split_path[1]}.tar"
    tar_path = os.path.join(split_path[0], tfile)
    if os.path.exists(tar_path):
        LOGGER.warning("tar_file(): FILE ALREADY EXISTS %s", tar_path)
        return None

    try:
        tarring = tarfile.open(tar_path, 'w:')
        tarring.add(fpath)
        tarring.close()
        return tar_path

    except Exception as exc:
        LOGGER.warning("tar_file(): ERROR WITH TAR WRAP %s", exc)
        tarring.close()
        return None


def get_tar_checksums(tar_path, folder):
    '''
    Open tar file and read/generate MD5 sums
    and return dct {filename: hex}
    '''
    data = {}
    tar = tarfile.open(tar_path, "r|")

    for item in tar:
        item_name = item.name
        if item.isdir():
            continue

        fname = os.path.basename(item_name)
        print(item_name, fname, item)

        try:
            f = tar.extractfile(item)
        except Exception as exc:
            LOGGER.warning("get_tar_checksums(): Unable to extract from tar file\n%s", exc)
            continue

        hash_md5 = hashlib.md5()
        for chunk in iter(lambda: f.read(65536), b""):
            hash_md5.update(chunk)

        if not folder:
            file = os.path.basename(fname)
            data[file] = hash_md5.hexdigest()
        else:
            data[fname] = hash_md5.hexdigest()

    return data


def get_checksum(fpath):
    '''
    Using file path, generate file checksum
    return as list with filename
    '''
    data = {}
    file = os.path.split(fpath)[1]
    hash_md5 = hashlib.md5()
    with open(fpath, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hash_md5.update(chunk)
        data[file] = hash_md5.hexdigest()
        f.close()
    return data


def make_manifest(tar_path, md5_dct):
    '''
    Output md5 to JSON file format and add to TAR file
    '''
    md5_path = f"{tar_path}_manifest.md5"

    try:
        with open(md5_path, 'w+') as json_file:
            json_file.write(json.dumps(md5_dct, indent=4))
            json_file.close()
    except Exception as exc:
        LOGGER.warning("make_manifest(): FAILED to create JSON %s", exc)

    if os.path.exists(md5_path):
        return md5_path


def main():
    '''
    Receive SYS.ARGV and check path exists or is file/folder
    Generate checksums for all folder contents/single file
    TAR Wrap, then make checksum for inside of TAR contents
    Compare checksum manifests, if match add into TAR and close.
    Delete original file, move TAR to autoingest path.
    '''

    if len(sys.argv) != 2:
        LOGGER.warning("SCRIPT EXIT: Error with shell script input:\n %s", sys.argv)
        sys.exit()

    fullpath = sys.argv[1]
    print(fullpath)

    if not os.path.exists(fullpath):
        sys.exit("Supplied path does not exists. Please try again.")

    log = []
    log.append(f"==== New path for TAR wrap: {fullpath} ====")
    LOGGER.info("==== TAR Wrapping Check script start ===============================")
    LOGGER.info("Path received for TAR wrap using Python3 tarfile: %s", fullpath)
    split_path = os.path.split(fullpath)
    tar_source = split_path[1]

    # Calculate checksum manifest for supplied fullpath
    local_md5 = {}
    directory = False
    if os.path.isdir(fullpath):
        directory = True

    if directory:
        files = [ x for x in os.listdir(fullpath) if os.path.isfile(os.path.join(fullpath, x)) ]
        for root, _, files in os.walk(fullpath):
            LOGGER.info("Path is directory.")
            log.append("Path is directory.")
            for file in files:
                dct = get_checksum(os.path.join(root, file))
                local_md5.update(dct)

    else:
        local_md5 = get_checksum(fullpath)
        log.append("Path is not a directory and will be wrapped alone")

    LOGGER.info("Checksums for local files:")
    log.append("Checksums for local files:")
    for key, val in local_md5.items():
        data = f"{val} -- {key}"
        LOGGER.info("\t%s", data)
        log.append(f"\t{data}")

    # Tar folder
    log.append("Beginning TAR wrap now...")
    tar_path = tar_file(fullpath)
    if not tar_path:
        log.append("TAR WRAP FAILED. SCRIPT EXITING!")
        LOGGER.warning("TAR wrap failed for file: %s", fullpath)
        for item in log:
            local_logs(AUTO_TAR, item)
        sys.exit(f"EXIT: TAR wrap failed for {fullpath}")

    # Calculate checksum manifest for TAR folder
    if directory:
        tar_content_md5 = get_tar_checksums(tar_path, tar_source)
    else:
        tar_content_md5 = get_tar_checksums(tar_path, '')

    log.append("Checksums from TAR wrapped contents:")
    LOGGER.info("Checksums for TAR wrapped contents:")
    for key, val in tar_content_md5.items():
        data = f"{val} -- {key}"
        LOGGER.info("\t%s", data)
        log.append(f"\t{data}")

    # Compare manifests
    if local_md5 == tar_content_md5:
        log.append("MD5 Manifests match, adding manifest to TAR file and moving to autoingest.")
        LOGGER.info("MD5 manifests match.")
        md5_manifest = make_manifest(tar_path, tar_content_md5)
        if not md5_manifest:
            LOGGER.warning("Failed to write TAR checksum manifest to JSON file.")
            shutil.move(tar_path, os.path.join(TAR_FAIL, f'{tar_source}.tar'))
            for item in log:
                local_logs(AUTO_TAR, item)
            sys.exit("Script exit: TAR file MD5 Manifest failed to create")

        LOGGER.info("TAR checksum manifest created. Adding to TAR file %s", tar_path)
        try:
            tar = tarfile.open(tar_path, 'a:')
            tar.add(md5_manifest)
            tar.close()
        except Exception as exc:
            LOGGER.warning("Unable to add MD5 manifest to TAR file. Moving TAR file to errors folder.\n%s", exc)
            shutil.move(tar_path, os.path.join(TAR_FAIL, f'{tar_source}.tar'))
            # Write all log items in block
            for item in log:
                local_logs(AUTO_TAR, item)
            sys.exit("Failed to add MD5 manifest To TAR file. Script exiting")

        LOGGER.info("TAR MD5 manifest added to TAR file. Getting wholefile TAR checksum for logs")

        # Get complete TAR wholefile Checksums for logs
        tar_md5 = get_tar_checksums(tar_path, '')
        log.append(f"TAR checksum: {tar_md5} for TAR file: {tar_path}")
        LOGGER.info("TAR checksum: %s", tar_md5)
        # Get complete size of file following TAR wrap
        file_stats = os.stat(tar_path)
        log.append(f"File size is {file_stats.st_size} bytes")
        LOGGER.info("File size is %s bytes.", file_stats.st_size)
        if file_stats.st_size > 1099511627770:
            log.append("FILE IS TOO LARGE FOR INGEST TO BLACK PEARL. Moving to oversized folder path")
            LOGGER.warning("MOVING TO OVERSIZE PATH: Filesize too large for ingest to DPI")
            shutil.move(tar_path, os.path.join(OVERSIZE, f'{tar_source}.tar'))
            oversize_log(f"{tar_path}\tTAR file too large for ingest to DPI - size {file_stats.st_size} bytes")

        log.append("Moving TAR file to Autoingest, and moving source file to deletions path.")
        shutil.move(tar_path, AUTOINGEST)
        LOGGER.info("Moving original file to deletions folder: %s", fullpath)
        shutil.move(fullpath, os.path.join(DELETE_TAR, tar_source))
        LOGGER.info("Moving MD5 manigest to checksum_manifest folder")
        shutil.move(md5_manifest, CHECKSUM)

    else:
        LOGGER.warning("Manifests do not match.\nLocal:\n%s\nTAR:\n%s", local_md5, tar_content_md5)
        LOGGER.warning("Moving TAR file to failures, leaving file/folder for retry.")
        log.append("MD5 manifests do not match. Moving TAR file to failures folder for retry")
        shutil.move(tar_path, os.path.join(TAR_FAIL, f'{tar_source}.tar'))

    log.append(f"==== Log actions complete: {fullpath} ====")
    # Write all log items in block
    for item in log:
        local_logs(AUTO_TAR, item)

    LOGGER.info("==== TAR Wrapping Check script END =================================\n")


def local_logs(fullpath, data):
    '''
    Output local log data for team
    to monitor TAR wrap process
    '''
    local_log = os.path.join(fullpath, 'tar_wrapping_checksum.log')
    timestamp = str(datetime.datetime.now())

    if not os.path.isfile(local_log):
        with open(local_log, 'x') as log:
            log.close()

    with open(local_log, 'a') as log:
        log.write(f"{timestamp[0:19]} - {data}\n")
        log.close()


def oversize_log(message):
    '''
    Logs for oversize_files_error.log
    '''
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if not os.path.isfile(ERROR_LOG):
        with open(ERROR_LOG, 'x') as log:
            log.close()

    with open(ERROR_LOG, 'a') as log:
        log.write(f"{ts}\t{message}\n")
        log.close()


if __name__ == '__main__':
    main()
