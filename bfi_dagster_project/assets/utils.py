import collections
import datetime
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Dict, Final, List, Optional

import ffmpeg
import tenacity

# Local BFI_scripts library for writing to BFI database
# Code is an environment variable for the BFI_scripts repository
sys.path.append(os.environ.get("CODE"))
import adlib_v3 as ad

# Import paths
METADATA_PATH = os.environ.get("CID_MEDIAINFO")
CID_API = os.environ.get("CID_API4")
PREFIX: Final = ["N", "C", "PD", "SPD", "PBS", "PBM", "PBL", "SCR", "CA"]


def get_object_number(fname: str) -> Optional[str]:
    """
    Extract object number from name formatted
    with partWhole, eg N_123456_01of03.ext
    """
    if not any(fname.startswith(px) for px in PREFIX):
        return False
    try:
        splits: list[str] = fname.split("_")
        object_number: Optional[str] = "-".join(splits[:-1])
    except Exception:
        object_number = None
    return object_number


def accepted_file_type(ext):
    """
    Receive extension and returnc
    matching accepted file_type
    """
    ftype = {
        "avi": "avi",
        "imp": "mxf, xml",
        "tar": "dpx, dcp, dcdm, wav",
        "mxf": "mxf, 50i, imp",
        "mkv": "mkv, dpx, dcdm",
        "wav": "wav",
        "tif": "tif, tiff",
        "tiff": "tif, tiff",
        "jpg": "jpg, jpeg",
        "jpeg": "jpg, jpeg",
    }

    ext = ext.lower()
    for key, val in ftype.items():
        if key == ext:
            return val

    return None


def get_metadata(arg: str, dpath: str) -> str:
    """
    Retrieve metadata with subprocess
    for supplied stream/field arg
    """
    probe = ffmpeg.probe(dpath)
    if "streams" not in probe:
        return False
    try:
        if probe["streams"][0][arg]:
            return probe["streams"][0][arg]
    except KeyError as err:
        print(f"KeyError: {err}")
        return False


def write_dir_tree(dpath: str) -> str:
    """
    Call subprocess tree to map directory
    into file for inclusion in source folder
    """

    seq = os.path.basename(dpath)
    fpath = os.path.join(dpath, f"{seq}_directory_contents.txt")
    cmd = ["tree", dpath, "-o", fpath]
    try:
        subprocess.run(cmd, text=True, check=True, shell=False)
        return fpath
    except Exception as err:
        print(err)
        return False


@tenacity.retry(stop=tenacity.stop_after_attempt(5))
def metadata_dump(dpath: str, file_path: str, ext: str) -> str:
    """
    Capture metadata for file into source folder
    if ext supplied copy to metadata folder for
    inclusion in CID digital media record
    """
    command = command2 = []
    file = os.path.basename(file_path)
    directory = os.path.basename(dpath)

    if len(ext) == 0:
        outpath = os.path.join(dpath, f"{directory}_{file}_metadata.txt")
        command = [
            "mediainfo",
            "--Full",
            "--Details=0",
            "--Output=TEXT",
            f"--LogFile={outpath}",
            file_path,
        ]
        try:
            subprocess.run(command, check=True, shell=False)
        except Exception as err:
            raise err
        if os.path.isfile(outpath):
            return outpath
        else:
            return None

    elif len(ext) > 0:
        outpath = os.path.join(METADATA_PATH, f"{directory}_{file}_SOURCE.json")
        command = [
            "mediainfo",
            "--Full",
            "--Output=JSON",
            f"--LogFile={outpath}",
            file_path,
        ]
        outpath2 = os.path.join(METADATA_PATH, f"{directory}_{file}_SOURCE.txt")
        command2 = ["mediainfo", "--Full", f"--LogFile={outpath2}", file_path]

        try:
            subprocess.run(command, check=True, shell=False)
            subprocess.run(command2, check=True, shell=False)
        except Exception as err:
            raise err

        if os.path.isfile(outpath) and os.path.isfile(outpath2):
            return outpath, outpath2
        else:
            return None, None


@tenacity.retry(stop=tenacity.stop_after_attempt(10))
def mediaconch(ipath: str, arg: str) -> List:
    """
    Check against relevant policy return pass/fail, and captured result
    """
    if arg == "TIF":
        policy = os.environ.get("POLICY_TIF")
    elif arg == "DPX":
        policy = os.environ.get("POLICY_DPX")
    elif arg == "TAR":
        return ["Fail", "Image sequence extension not recognised for RAWcook"]

    cmd = ["mediaconch", "--Force", "-p", policy, ipath]

    try:
        result = subprocess.check_output(cmd, shell=False).decode()
        if len(result) == 0:
            raise Exception("No response received, attempt mediaconch retry!")
        elif str(result).startswith(f"pass! {ipath}"):
            return ["Pass", str(result)]
        else:
            return ["Fail", str(result)]
    except Exception as err:
        raise err


@tenacity.retry(stop=tenacity.stop_after_attempt(10))
def mediaconch_mkv(dpath: str) -> List:
    """
    Check FFV1 MKV against policy
    """
    policy = os.environ.get("POLICY_RAWCOOK")
    cmd = ["mediaconch", "--Force", "-p", policy, dpath]

    try:
        result = subprocess.check_output(cmd, shell=False).decode()
        if len(result) == 0:
            raise Exception("No response received, attempt mediaconch retry!")
        elif str(result).startswith(f"pass! {dpath}"):
            return ["Pass", result]
        else:
            return ["Fail", result]
    except Exception as err:
        raise err


def get_partwhole(fname: str) -> tuple[int, int]:
    """
    Check part whole well formed
    """
    match = re.search(r"(?:_)(\d{2,4}of\d{2,4})", fname)
    if not match:
        return None, None
    part, whole = [int(i) for i in match.group(1).split("of")]
    len_check = fname.split("_")
    len_check = len_check[-1].split(".")[0]
    str_part, str_whole = len_check.split("of")
    if len(str_part) != len(str_whole):
        return None, None
    if part > whole:
        return None, None

    return (part, whole)


def count_folder_depth(fpath: str) -> str | bool | None:
    """
    Check if folder is three depth of four depth
    across total scan folder contents and folders
    ordered correctly
    """
    folder_contents = []
    for root, dirs, _ in os.walk(fpath):
        for directory in dirs:
            if directory.startswith("."):
                if os.listdir(os.path.join(root, directory)) == []:
                    continue
            folder_contents.append(os.path.join(root, directory))

    # Check for dupes in folder names and length of found folders
    repeats = [
        item
        for item, count in collections.Counter(folder_contents).items()
        if count > 1
    ]
    if len(repeats) > 0:
        return False
    if len(folder_contents) < 2:
        return False
    if len(folder_contents) == 2:
        if (
            "scan" in folder_contents[0].split("/")[-1].lower()
            and "x" in folder_contents[1].split("/")[-1].lower()
        ):
            return "3"
    if len(folder_contents) == 3:
        if (
            "x" in folder_contents[0].split("/")[-1].lower()
            and "scan" in folder_contents[1].split("/")[-1].lower()
            and "R" in folder_contents[2].split("/")[-1].upper()
        ):
            return "4"
    if len(folder_contents) > 3:
        total_scans = []
        for num in range(0, len(folder_contents)):
            if "scan" in folder_contents[num].split("/")[-1].lower():
                total_scans.append(folder_contents[num].split("/")[-1])

        scan_num = len(total_scans)
        if len(folder_contents) / scan_num == 2:
            # Ensure folder naming order is correct
            if "scan" not in folder_contents[0].split("/")[-1].lower():
                return None
            sorted(folder_contents, key=len)
            return "3"
        if (len(folder_contents) - 1) / scan_num == 2:
            # Ensure folder naming order is correct
            if (
                "scan" in folder_contents[0].split("/")[-1].lower()
                and "R"
                not in folder_contents[len(folder_contents) - 1].split("/")[-1].upper()
            ):
                return None
            sorted(folder_contents, key=len)
            return "4"
    return False


def get_fps(ipath: str) -> Optional[int]:
    """
    Get frames per second from image/video stream
    """
    cmd = ["exiftool", "-framerate", ipath]
    try:
        fps = subprocess.check_output(cmd, shell=False).decode().split(": ")[-1]
    except subprocess.CalledProcessError as err:
        print(err)
        fps = ""
    fps = str(fps).rstrip('/n')
    if '.' in fps:
        fps = fps.split('.')[0]
    if len(fps) > 0 and fps.isnumeric():
        return int(fps)

    return None


def get_file_type(seq: str) -> tuple[str, str]:
    """
    Call up BFI CID database with fname
    and check item file-type (BFI specific code)
    """
    print(CID_API)
    ob_num = get_object_number(seq)
    search = f'object_number="{ob_num}"'
    print(search)
    hits, rec = ad.retrieve_record(
        CID_API, "items", search, "1", ["priref", "file_type", "reproduction.reference"]
    )
    print(hits, rec)
    if rec is None:
        return None, None, None

    ftype = ad.retrieve_field_name(rec[0], "file_type")[0]
    priref = ad.retrieve_field_name(rec[0], "priref")[0]

    print(priref, ftype)
    return priref, ftype, rec[0]


def gaps(dpath: str) -> tuple[str, str, list]:
    """
    Return abs path to first and last DPX
    in sequence then list of any numbers
    missing in DPX sequence
    """

    missing_dpx = []
    file_nums, filenames = iterate_folders(dpath)
    if file_nums is None or filenames is None:
        return None, None, None
    print(file_nums, filenames)
    # Calculate range from first/last
    file_range = list(range(min(file_nums), max(file_nums) + 1))
    print(file_range)
    # Retrieve equivalent DPX names for logs
    first_dpx = filenames[file_nums.index(min(file_nums))]
    last_dpx = filenames[file_nums.index(max(file_nums))]

    # Check for absent numbers in sequence
    missing = list(set(file_nums) ^ set(file_range))
    if len(missing) > 0:
        for missed in missing:
            missing_dpx.append(missed)

    return (first_dpx, last_dpx, missing_dpx)


def check_fname(fname: str) -> bool:
    """
    Run series of checks against BFI filenames
    check accepted prefixes, and extensions
    """
    prefix = ["N", "C", "PD", "SPD", "PBS", "PBM", "PBL", "SCR", "CA"]
    if not any(fname.startswith(px) for px in prefix):
        return False
    if not re.search("^[A-Za-z0-9_.]*$", fname):
        return False

    sname = fname.split("_")
    if len(sname) > 4 or len(sname) < 3:
        return False
    if len(sname) == 4 and len(sname[2]) != 1:
        return False
    return True


def iterate_folders(fpath: str) -> tuple[list, list]:
    """
    Iterate suppied path and return list
    of filenames re search for last numbers
    in filename
    """
    file_nums = []
    filenames = []
    for root, _, files in os.walk(fpath):
        for file in files:
            if file.endswith((".dpx", ".DPX", ".tif", ".TIF", ".tiff", ".TIFF")):
                file_nums.append(int(re.search(r"\d+(?!.*\d)", file).group()))
                filenames.append(os.path.join(root, file))
    if not file_nums or not filenames:
        return None, None
    return (file_nums, filenames)


def get_folder_size(fpath: str) -> int:
    """
    Check the size of given folder path
    return size in kb
    """
    if os.path.isfile(fpath):
        return os.path.getsize(fpath)

    byte_size = 0
    for root, _, files in os.walk(fpath):
        for file in files:
            fpath = os.path.join(root, file)
            byte_size += os.path.getsize(fpath)

    return byte_size


def move_to_failures(fpath: str) -> None:
    """
    Move a file or sequence to the failures folder.
    """
    fail_path = os.path.join(str(Path(fpath).parents[1]), "failures/")
    try:
        if not os.path.exists(fail_path):
            os.makedirs(fail_path, exist_ok=True)

        dest_path = os.path.join(fail_path, os.path.basename(fpath))
        shutil.move(fpath, dest_path)
        return f"Moved to failures folder: {dest_path}"
    except Exception as err:
        return f"Failed to move {os.path.basename(fpath)} to failures {dest_path} {err}"


def move_log_to_dest(lpath: str, arg: str) -> None:
    """
    Move a file or sequence to the failures folder.
    Prepend failed logs 'fail_'
    """
    dest = os.path.join(str(Path(lpath).parents[1]), f"logs/{arg}/")
    if arg == "failures":
        fname = f"fail_{os.path.basename(lpath)}"
    else:
        fname = os.path.basename(lpath)
    try:
        if not os.path.exists(dest):
            os.makedirs(dest, exist_ok=True)

        dest_path = os.path.join(dest, fname)
        shutil.move(lpath, dest_path)

    except Exception as e:
        print(e)


def move_to_autoingest(fpath: str) -> bool:
    """
    Move a file to the new workflow folder.
    """
    # 3 for test, 2 for BP paths where autoingest sits within automation/
    if "/mnt/qnap" in str(fpath) or "/mnt/Edit" in str(fpath):
        autoingest = os.path.join(
            str(Path(fpath).parents[3]), "autoingest/ingest/autodetect/"
        )
    else:
        autoingest = os.path.join(
            str(Path(fpath).parents[2]), "autoingest/ingest/autodetect/"
        )
    try:
        dest_path = os.path.join(autoingest, os.path.basename(fpath))
        shutil.move(fpath, dest_path)
    except Exception as e:
        print(e)
        raise
    if os.path.isfile(dest_path):
        return True


def delete_sequence(sequence_path: str) -> bool:
    """
    Delete an image sequence and any associated files.
    """
    if not os.path.exists(sequence_path):
        return None
    try:
        shutil.rmtree(sequence_path)
    except OSError as err:
        print(err)
        raise

    if not os.path.isdir(sequence_path):
        return True


def tar_item(fullpath: str) -> str:
    """
    Make tar path from supplied filepath
    Use tarfile to create TAR
    """

    fpath, fname = os.path.split(fullpath)
    tar_path = os.path.join(str(Path(fpath).parents[0]), f"tar_wrapping/{fname}.tar")
    local_log = os.path.join(tar_path, f"{fname}_tar_wrap.log")
    if os.path.exists(tar_path):
        append_to_log(local_log, "Exiting. File already exists: {tar_path}")
        return None

    try:
        tarring = tarfile.open(tar_path, "w:")
        tarring.add(fullpath, arcname=f"{fname}")
        tarring.close()
        return tar_path

    except Exception as exc:
        append_to_log(local_log, f"ERROR TARRING FILE: {exc}")
        tarring.close()
        return None


def get_checksums(tar_path: str, folder: str) -> Dict[str, str]:
    """
    Open tar file and read/generate MD5 sums
    and return dct {filename: hex}
    """
    data = {}
    tar = tarfile.open(tar_path, "r:")

    for item in tar:
        item_name = item.name
        if item.isdir():
            continue
        pth, fname = os.path.split(item_name)
        if "tar_wrap.log" in fname:
            continue
        if ".DS_Store" in fname:
            continue

        folder_prefix = os.path.basename(pth)
        fname = f"{folder_prefix}_{fname}"
        try:
            f = tar.extractfile(item)
        except Exception as exc:
            print(exc)
            continue

        hash_md5 = hashlib.md5()
        for chunk in iter(lambda: f.read(65536), b""):
            hash_md5.update(chunk)

        if not folder:
            file = os.path.basename(fname)
            data[file] = str(hash_md5.hexdigest())
        else:
            data[fname] = str(hash_md5.hexdigest())
    tar.close()
    return data


def get_checksum(fpath: str) -> Dict[str, str]:
    """
    Using file path, generate file checksum
    return as list with filename
    """
    data = {}
    pth, file = os.path.split(fpath)
    folder_prefix = os.path.basename(pth)
    file = f"{folder_prefix}_{file}"
    hash_md5 = hashlib.md5()
    with open(fpath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hash_md5.update(chunk)
        data[file] = str(hash_md5.hexdigest())
        f.close()
    return data


def make_manifest(tar_path: str, md5_dct: str) -> str:
    """
    Output md5 to JSON file format and add to TAR file
    """
    md5_path = f"{tar_path}_manifest.md5"

    try:
        with open(md5_path, "w+") as json_file:
            json_file.write(json.dumps(md5_dct, indent=4))
            json_file.close()
    except Exception as exc:
        print(exc)

    if os.path.exists(md5_path):
        return md5_path


def md5_hash(tar_file: str) -> Optional[int]:
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


def append_to_log(local_log: str, data: str) -> None:
    """
    Output local log data for team
    to monitor TAR wrap process
    """
    if not os.path.isfile(local_log):
        with open(local_log, "x") as log:
            log.close()

    with open(local_log, "a") as log:
        log.write(f"{str(datetime.datetime.now())[:19]} - {data}\n")
        log.close()


def write_to_cid(priref: str, fname: str) -> bool:
    """
    Make payload and write to BFI CID database
    (BFI specific code)
    """

    name = "datadigipres"
    method = "TAR wrapping method:"
    text = f"For preservation to DPI the item {fname} was wrapped using Python tarfile module, and the TAR includes checksum manifests of all contents."
    date = str(datetime.datetime.now())[:10]
    time = str(datetime.datetime.now())[11:19]
    notes = "Automated TAR wrapping script."
    payload_head = f"<adlibXML><recordList><record priref='{priref}'>"
    payload_addition = (
        f"<utb.fieldname>{method}</utb.fieldname><utb.content>{text}</utb.content>"
    )
    payload_edit = f"<edit.name>{name}</edit.name><edit.date>{date}</edit.date><edit.time>{time}</edit.time><edit.notes>{notes}</edit.notes>"
    payload_end = "</record></recordList></adlibXML>"
    payload = payload_head + payload_addition + payload_edit + payload_end

    record = ad.post(CID_API, payload, "items", "updaterecord")
    if record is None:
        return False
    return True


def check_for_version_two(log: str) -> bool:
    """Check if output version2 needed"""

    warnings = [
        "Error: undecodable file is becoming too big",
        "Error: the reversibility file is becoming big",
    ]

    if not os.path.isfile(log):
        log_name_clean = os.path.basename(log)
        log_name_fail = f"fail_{log_name_clean}"
        log_path = os.path.join(str(Path(log).parents[1]), "logs/failures/")
        files = os.listdir(log_path)
        for file in files:
            if log_name_clean in file:
                log = os.path.join(log_path, file)
            if log_name_fail in file:
                log = os.path.join(log_path, file)

    if not os.path.isfile(log):
        return False

    with open(log, "r") as log_file:
        for line in log_file:
            if warnings[0] in str(line) or warnings[1] in str(line):
                return True

    return False


def check_mkv_log(log_path: str) -> bool:
    """
    Check for success note in log
    """
    errors = [
        "Reversibility was checked, issues detected",
        "Error:",
        "Conversion failed!",
        "Please contact info@mediaarea.net",
        "Error while decoding stream",
    ]

    success = "Reversibility was checked, no issue detected."

    if not os.path.exists(log_path):
        return False

    with open(log_path, "r") as log_file:
        data = log_file.readlines()
        for line in data:
            if success in str(line):
                return True
            if any(elem in str(line) for elem in errors):
                return False

    return False


def check_tar_log(log_path: str) -> bool:
    """
    Look for failure message
    """
    errors = ["Failure exit", "Checksum mismatch"]

    success = "TAR wrap completed successfully."

    with open(log_path, "r") as log_file:
        lines = log_file.readlines()
        for line in lines:
            if success in str(line):
                return True
            if any(elem in str(line) for elem in errors):
                return False
    return False


def check_file(mpath: str) -> bool:
    """
    Run check, check console output for
    success statement
    """
    root, fname = os.path.split(mpath)
    log_name = f"check_log_{fname}.txt"
    log = os.path.join(root, log_name)
    cmd = ["rawcooked", "--check", f"{mpath}", ">>", f"{log}", "2>&1"]
    try:
        subprocess.run(" ".join(cmd), shell=True, check=True)
    except subprocess.CalledProcessError as err:
        print(err)
        raise err

    with open(log, "r") as file:
        logs = file.readlines()
    for line in logs:
        if "Reversibility was checked, no issue detected." in line:
            log_path = os.path.join(
                str(Path(root).parents[0]), "logs/check_logs/", log_name
            )
            shutil.move(log, log_path)
            return True

    move_log_to_dest(log, "failures")
    return False


def recursive_chmod(dpath: str, mode: int) -> None:
    """
    Recursively change permissions of directory and all contents
    """
    os.chmod(dpath, mode)
    if os.path.isdir(dpath):
        for root, dirs, files in os.walk(dpath):
            for dir in dirs:
                try:
                    os.chmod(os.path.join(root, dir), mode)
                except PermissionError as e:
                    print(f"Error changing {dir}: {e}")
            for file in files:
                try:
                    os.chmod(os.path.join(root, file), mode)
                except PermissionError as e:
                    print(f"Error changing {file}: {e}")
