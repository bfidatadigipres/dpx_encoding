#!/usr/bin/env python3

'''
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
        Move original folder to 'delete' folder
        Update details to local log.
   No. Delete faulty TAR.
       Output warning to Local log and leave file
       for retry at later date.
6. Check if file is over 1TB in size:
   Yes. Move to oversize folder, update oversize log
        and all local logs then exit.
   No. Continue to move source to 'to_delete' folder
       and TAR file to autoingest. Update all logs.

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

sys.path.append(os.environ['CODE'])
import adlib_v3 as adlib

# Global paths
LOCAL_PATH = os.environ['FILM_OPS']
AUTO_TAR = os.path.join(LOCAL_PATH, os.environ['TAR_PRES'])
DELETE_TAR = os.path.join(LOCAL_PATH, os.environ['TO_DELETE'])
TAR_FAIL = os.path.join(LOCAL_PATH, os.environ['TAR_FAIL'])
CHECKSUM = os.path.join(LOCAL_PATH, os.environ['TAR_CHECKSUM'])
OVERSIZE = os.path.join(LOCAL_PATH, os.environ['CURRENT_ERRORS'], 'oversized_sequences/')
ERRORS = os.path.join(LOCAL_PATH, os.environ['CURRENT_ERRORS'])
AUTOINGEST = os.path.join(LOCAL_PATH, 'Finished/', os.environ['AUTOINGEST_STORE'])
LOG = os.path.join(LOCAL_PATH, os.environ['DPX_SCRIPT_LOG'], 'tar_wrapping_checksum.log')
CID_API = os.environ['CID_API4']

# Logging config
LOGGER = logging.getLogger('tar_wrapping_film_ops')
hdlr = logging.FileHandler(LOG)
formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
hdlr.setFormatter(formatter)
LOGGER.addHandler(hdlr)
LOGGER.setLevel(logging.INFO)


def get_cid_data(fname):
    '''
    Use requests to retrieve priref for associated item object number
    '''
    ob_num_split = fname.split('_')
    if len(ob_num_split) == 3:
        ob_num = '-'.join(ob_num_split[0:2])
    elif len(ob_num_split) == 4:
        ob_num = '-'.join(ob_num_split[0:3])
    else:
        LOGGER.warning("Incorrect filename formatting. Script exiting.")
        sys.exit(f"Incorrect filename formatting {fname}. Script exiting.")

    search = f"object_number='{ob_num}'"
    record = adlib.retrieve_record(CID_API, 'items', search, '1', ['priref', 'file_type'])[1]
    if not record:
        return None

    try:
        priref = adlib.retrieve_field_name(record[0], 'priref')[0]
    except (IndexError, KeyError):
        priref = ''
    try:
        file_type = adlib.retrieve_field_name(record[0], 'file_type')[0]
    except (IndexError, KeyError):
        file_type = ''

    return priref, file_type


def tar_item(fpath):
    '''
    Make tar path from supplied filepath
    Use tarfile to create TAR
    '''
    split_path = os.path.split(fpath)
    tfile = f"{split_path[1]}.tar"
    tar_path = os.path.join(split_path[0], tfile)
    if os.path.exists(tar_path):
        LOGGER.warning("tar_item(): FILE ALREADY EXISTS %s", tar_path)
        return None

    try:
        tarring = tarfile.open(tar_path, 'w:')
        tarring.add(fpath, arcname=f"{split_path[1]}")
        tarring.close()
        return tar_path

    except Exception as exc:
        LOGGER.warning("tar_item(): ERROR WITH TAR WRAP %s", exc)
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

    if fullpath.endswith('.md5'):
        sys.exit("Supplied path is MD5. Skipping.")

    log = []
    log.append(f"==== New path for TAR wrap: {fullpath} ====")
    LOGGER.info("==== TAR Wrapping Check script start ===============================")
    LOGGER.info("Path received for TAR wrap using Python3 tarfile: %s", fullpath)
    split_path = os.path.split(fullpath)
    tar_source = split_path[1]

    # Validate filename and retrieve priref/file_type
    fname_split = tar_source.split('_')
    if 'of' not in str(fname_split[-1:]):
        LOGGER.warning("Script exiting. Poorly formed partWhole for filename %s", tar_source)
        error_mssg1 = f"Part whole for TAR file is poorly formed {tar_source}\n\tPlease correct the part whole for this DPX folder"
        error_mssg2 = None
        error_log(os.path.join(ERRORS, f"{tar_source}_errors.log"), error_mssg1, error_mssg2)
        sys.exit("Filename  not formed correctly, script exiting.")

    priref, file_type = get_cid_data(tar_source)
    if not priref:
        LOGGER.warning("No CID item record found to match TAR source file. Script exitings")
        error_mssg1 = f"No CID item record found to match {tar_source}\n\tPlease check the folder number is named after the CID item record for the DPX sequence"
        error_mssg2 = None
        error_log(os.path.join(ERRORS, f"{tar_source}_errors.log"), error_mssg1, error_mssg2)
        sys.exit(f"No CID item record found to match TAR file {tar_source}. Exiting.")
    LOGGER.info("File for TAR wrapping %s has matching CID Item record: %s. File type: %s", tar_source, priref, file_type)

    if file_type.lower() not in ['dpx', 'dcp', 'dcdm', 'wav']:
        LOGGER.warning("File type absent or doesn't match DPX, DCP, DCDM or WAV. Script exiting")
        error_mssg1 = f"File type absent or doesn't match DPX, DCP, DCDM or WAV: {file_type}\n\tPlease check 'file_type' field for CID item record {priref}"
        error_mssg2 = None
        error_log(os.path.join(ERRORS, f"{tar_source}_errors.log"), error_mssg1, error_mssg2)
        sys.exit(f"File type absent or doesn't match those for TAR: {file_type}. Exiting.")

    # Calculate checksum manifest for supplied fullpath
    local_md5 = {}
    directory = False
    if os.path.isdir(fullpath):
        log.append("Supplied path for TAR wrap is directory")
        LOGGER.info("Supplied path for TAR wrap is directory")
        directory = True

    if directory:
        for root, _, files in os.walk(fullpath):
            for file in files:
                dct = get_checksum(os.path.join(root, file))
                local_md5.update(dct)

    else:
        local_md5 = get_checksum(fullpath)
        log.append("Path is not a directory and will be wrapped alone")

    LOGGER.info("Checksums for local files (excluding DPX, TIF):")
    log.append("Checksums for local files (excluding DPX, TIF):")
    for key, val in local_md5.items():
        if not key.endswith(('.dpx', '.DPX', '.tif', '.TIF', '.TIFF', '.tiff', '.jp2', '.j2k', '.jpf', '.jpm', '.jpg2', '.j2c', '.jpc', '.jpx', '.mj2')):
            data = f"{val} -- {key}"
            LOGGER.info("\t%s", data)
            log.append(f"\t{data}")

    # Tar folder
    log.append("Beginning TAR wrap now...")
    tar_path = tar_item(fullpath)
    tar_file = os.path.split(tar_path)[1]
    if not tar_path:
        log.append("TAR WRAP FAILED. SCRIPT EXITING!")
        LOGGER.warning("TAR wrap failed for file: %s", fullpath)
        for item in log:
            local_logs(AUTO_TAR, item)
        error_mssg1 = f"TAR wrap failed for folder {tar_source}. No TAR file found:\n\t{tar_path}"
        error_mssg2 = "if the TAR wrap has failed for an inexplicable reason"
        error_log(os.path.join(ERRORS, f"{tar_source}_errors.log"), error_mssg1, error_mssg2)
        sys.exit(f"EXIT: TAR wrap failed for {fullpath}")

    # Calculate checksum manifest for TAR folder
    if directory:
        tar_content_md5 = get_tar_checksums(tar_path, tar_source)
    else:
        tar_content_md5 = get_tar_checksums(tar_path, '')

    log.append("Checksums from TAR wrapped contents (excluding DPX, TIF, JPEG2000):")
    LOGGER.info("Checksums for TAR wrapped contents (excluding DPX, TIF, JPEG2000):")
    for key, val in tar_content_md5.items():
        if not key.endswith(('.dpx', '.DPX', '.tif', '.TIF', '.tiff', '.TIFF', '.jp2', '.j2k', '.jpf', '.jpm', '.jpg2', '.j2c', '.jpc', '.jpx', '.mj2')):
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
            error_mssg1 = f"TAR checksum manifest was not created for new TAR file:\n\t{tar_path}\n\tTAR file moved to failures folder"
            error_mssg2 = "if no explicable reason for this failure (ie, file was moved mid way through processing)"
            error_log(os.path.join(ERRORS, f"{tar_source}_errors.log"), error_mssg1, error_mssg2)
            sys.exit("Script exit: TAR file MD5 Manifest failed to create")

        LOGGER.info("TAR checksum manifest created. Adding to TAR file %s", tar_path)
        try:
            arc_path = os.path.split(md5_manifest)
            tar = tarfile.open(tar_path, 'a:')
            tar.add(md5_manifest, arcname=f"{arc_path[1]}")
            tar.close()
        except Exception as exc:
            LOGGER.warning("Unable to add MD5 manifest to TAR file. Moving TAR file to failures folder.\n%s", exc)
            shutil.move(tar_path, os.path.join(TAR_FAIL, f'{tar_source}.tar'))
            # Write all log items in block
            for item in log:
                local_logs(AUTO_TAR, item)
            error_mssg1 = f"TAR checksum manifest could not be added to TAR file:\n\t{tar_path}\n\tTAR file moved to failures folder"
            error_mssg2 = "if no explicable reason for this failure (ie, file was moved mid way through processing)"
            error_log(os.path.join(ERRORS, f"{tar_source}_errors.log"), error_mssg1, error_mssg2)
            sys.exit("Failed to add MD5 manifest To TAR file. Script exiting")

        LOGGER.info("TAR MD5 manifest added to TAR file. Getting wholefile TAR checksum for logs")
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
        if int(file_size) > 1099511627770:
            log.append("FILE IS TOO LARGE FOR INGEST TO BLACK PEARL. Moving TAR and source folder to oversized folder path")
            LOGGER.warning("MOVING TO OVERSIZE PATH: Filesize too large for ingest to DPI: %s", os.path.join(OVERSIZE, f'{tar_source}.tar'))
            shutil.move(tar_path, os.path.join(OVERSIZE, f'{tar_source}.tar'))
            error_mssg1 = f"TAR file too large for ingest to DPI - size {file_stats.st_size} bytes\n\t{tar_path}\n\tDPX sequence moved to oversized_sequences/ folder."
            error_mssg2 = "as this file is too large for ingest and will need repeat splitting"
            error_log(os.path.join(ERRORS, f"{tar_source}_errors.log"), error_mssg1, error_mssg2)
            LOGGER.warning("Moving sequence to current_error/ folder for manual assistance: %s", tar_source)
            shutil.move(fullpath, os.path.join(OVERSIZE, tar_source))
            log.append(f"==== Log actions complete: {fullpath} ====")
            LOGGER.info("==== TAR Wrapping Check script END =================================")
            # Write all log items in block
            for item in log:
                local_logs(AUTO_TAR, item)
            sys.exit()

        log.append("Moving TAR file to Autoingest, and moving source file to deletions path.")
        try:
            LOGGER.info("Moving %s to %s", tar_path, AUTOINGEST)
            shutil.move(tar_path, AUTOINGEST)
        except Exception as err:
            LOGGER.warning("File move to autoingest failed:\n%s", err)
        try:
            LOGGER.info("Moving and deleting DPX sequence: %s", os.path.join(DELETE_TAR, tar_source))
            shutil.move(fullpath, os.path.join(DELETE_TAR, tar_source))
            os.rmdir(os.path.join(DELETE_TAR, tar_source))
        except Exception as err:
            LOGGER.warning("Source move to 'to_delete' folder failed:\n%s", err)
        try:
            LOGGER.info("Moving MD5 manifest to checksum_manifest folder %s", CHECKSUM)
            shutil.move(md5_manifest, CHECKSUM)
        except Exception as err:
            LOGGER.warning("MD5 manifest move failed:\n%s", err)

        # Tidy away error_log following successful creation
        if os.path.isfile(os.path.join(ERRORS, f"{tar_source}_errors.log")):
            shutil.move(os.path.join(ERRORS, f"{tar_source}_errors.log"), os.path.join(ERRORS, "completed", f"{tar_source}_errors.log"))

        # Write note to CID Item record that file has been wrapped using Python tarfile module.
        success = write_to_cid(priref, tar_file)
        if not success:
            error_mssg1 = f"Failed to write Python tarfile message to CID item record: {priref} {tar_file}. Please add manually."
            error_mssg2 = None
            error_log(os.path.join(ERRORS, f"{tar_source}_errors.log"), error_mssg1, error_mssg2)

    else:
        LOGGER.warning("Manifests do not match.\nLocal:\n%s\nTAR:\n%s", local_md5, tar_content_md5)
        LOGGER.warning("Moving TAR file to failures, leaving file/folder for retry.")
        log.append("MD5 manifests do not match. Moving TAR file to failures folder for retry")
        shutil.move(tar_path, os.path.join(TAR_FAIL, f'{tar_source}.tar'))
        error_mssg1 = f"MD5 checksum manifests do not match for source folder and TAR file:\n\t{tar_path}\n\tTAR file moved to failures folder"
        error_mssg2 = "if this checksum comparison fails multiple times"
        error_log(os.path.join(ERRORS, f"{tar_source}_errors.log"), error_mssg1, error_mssg2)

    log.append(f"==== Log actions complete: {fullpath} ====")
    # Write all log items in block
    for item in log:
        local_logs(AUTO_TAR, item)

    LOGGER.info("==== TAR Wrapping Check script END =================================")


def md5_hash(tar_file):
    '''
    Make whole file TAR MD5 checksum
    '''
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


def error_log(fpath, message, kandc):
    '''
    If needed, write error log
    for incomplete sequences.
    '''
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not kandc:
        with open(fpath, 'a+') as log:
            log.write(f"tar_wrapping {ts}: {message}.\n\n")
            log.close()
    else:
        with open(fpath, 'a+') as log:
            log.write(f"tar_wrapping {ts}: {message}.\n")
            log.write(f"\tPlease contact the Knowledge and Collections Developer {kandc}.\n\n")
            log.close()


def write_to_cid(priref, fname):
    '''
    Make payload and write to CID
    '''
    name = 'datadigipres'
    method = "TAR wrapping method:"
    text = f"For preservation to DPI the item {fname} was wrapped using Python tarfile module, and the TAR includes checksum manifests of all contents."
    date = str(datetime.datetime.now())[:10]
    time = str(datetime.datetime.now())[11:19]
    notes = 'Automated TAR wrapping script.'
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_addition = f"<utb.fieldname>{method}</utb.fieldname><utb.content>{text}</utb.content>"
    payload_edit = f"<edit.name>{name}</edit.name><edit.date>{date}</edit.date><edit.time>{time}</edit.time><edit.notes>{notes}</edit.notes>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_head + payload_addition + payload_edit + payload_end

    record = adlib.post(CID_API, payload, 'items', 'updaterecord')
    if record is None:
        return False
    return True


if __name__ == '__main__':
    main()
