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

# Global paths
'''
AUTOINGEST = os.path.join(os.environ[''], 'ingest/store/document/')
TAR_FAILURES = os.environ['']
DELETE_PATH = os.environ['']
'''
LOG_PATH = os.environ['LOG_PATH']

# Logging config
LOGGER = logging.getLogger('tar_wrap_check')
hdlr = logging.FileHandler(os.path.join(LOG_PATH, 'tar_wrapping_check.log'))
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
        fname = item.name
        if folder == fname:
            continue
        print(fname, item)

        try:
            f = tar.extractfile(item)
        except Exception as exc:
            LOGGER.warning("get_tar_checksums(): Unable to extract from tar file\n%s", exc)
            continue

        hash_md5 = hashlib.md5()
        for chunk in iter(lambda: f.read(65536), b""):
            hash_md5.update(chunk)
        data[fname] = hash_md5.hexdigest()

    return data


def get_checksum(fpath, source):
    '''
    Using file path, generate file checksum
    return as list with filename
    '''
    data = {}
    fname = os.path.split(fpath)[1]
    if source != '':
        dct_name = f"{source}/{fname}"
    else:
        dct_name = fname

    try:
        hash_md5 = hashlib.md5()
        with open(fpath, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hash_md5.update(chunk)
        data[dct_name] = hash_md5.hexdigest()

    except Exception as exc:
        LOGGER.warning("get_checksum(): FAILED TO GET CHECKSUM %s", exc)

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
        print(exc)

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

    if not os.path.exists(fullpath):
        sys.exit("Supplied path does not exists. Please try again.")

    LOGGER.info("==== TAR Wrapping Check script start ===============================")
    LOGGER.info("Path received for TAR wrap using Python3 tarfile: %s", fullpath)
    tar_source = os.path.split(fullpath)[1]

    # Calculate checksum manifest for supplied fullpath
    local_md5 = {}
    directory = False
    if os.path.isdir(fullpath):
        directory = True

    if directory:
        files = [ x for x in os.listdir(fullpath) if os.path.isfile(os.path.join(fullpath, x)) ]
        print(files)

        for file in files:
            dct = get_checksum(os.path.join(fullpath, file), tar_source)
            local_md5.update(dct)

    else:
        local_md5 = get_checksum(fullpath, '')

    print(f"Local MD5: {local_md5}")

    # Tar folder
    tar_path = tar_file(fullpath)
    if not tar_path:
        LOGGER.warning("TAR wrap failed for file: %s", fullpath)
        sys.exit(f"EXIT: TAR wrap failed for {fullpath}")

    # Calculate checksum manifest for TAR folder
    if directory:
        tar_md5 = get_tar_checksums(tar_path, tar_source)
    else:
        tar_md5 = get_tar_checksums(tar_path, '')

    print(f"TAR MD5: {tar_md5}")

    # Compare manifests
    if local_md5 == tar_md5:
        print("Manifests match, adding manifest to TAR file and moving to autoingest.")
        LOGGER.info("MD5 manifests match.\nLocal path manifest:\n%s\nTAR file manifest:\n%s", local_md5, tar_md5)
        md5_manifest = make_manifest(tar_path, tar_md5)
        if not md5_manifest:
            LOGGER.warning("Failed to write TAR checksum manifest to JSON file.")
#            shutil.move(tar_path, TAR_FAILURES)
            sys.exit("Script exit: TAR file MD5 Manifest failed to create")

        LOGGER.info("TAR checksum manifest created. Adding to TAR file %s", tar_path)
        try:
            tar = tarfile.open(tar_path, 'a:')
            tar.add(md5_manifest)
            tar.close()
        except Exception as exc:
            LOGGER.warning("Unable to add MD5 manifest to TAR file. Moving TAR file to errors folder.\n%s", exc)
#            shutil.move(tar_path, TAR_FAILURES)
            sys.exit()

        LOGGER.info("TAR MD5 manifest added to TAR file. Moving to Autoingest.")
#        shutil.move(tar_path, AUTOINGEST)
        LOGGER.info("Moving original file to deletions folder: %s", fullpath)
#        shutil.move(fullpath, DELETE_PATH)

    else:
        LOGGER.warning("Manifests do not match.\nLocal:\n%s\nTAR:\n%s", local_md5, tar_md5)
        LOGGER.warning("Moving TAR file to failures, leaving file/folder for retry.")
#        shutil.move(tar_path, TAR_FAILURES)


if __name__ == '__main__':
    main()
