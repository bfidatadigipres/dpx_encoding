import datetime
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg

from . import utils


def build_transcode_ffv1_asset(key_prefix: Optional[str] = None):
    """
    New factory function that returns the asset with optional key prefix.
    """
    # Build the asset key with optional prefix
    if key_prefix:
        asset_key = [key_prefix, "transcode_ffv1"]
        ins_dict = {"assessment": dg.AssetIn([key_prefix, "assess_sequence"])}
    else:
        asset_key = "transcode_ffv1"  # Single string key for no prefix
        ins_dict = {"assessment": dg.AssetIn("assess_sequence")}

    @dg.asset(
        key=asset_key, ins=ins_dict, required_resource_keys={"database", "process_pool"}
    )
    def transcode_ffv1(
        context: dg.AssetExecutionContext,
        assessment: Dict[str, List[str]],
    ) -> dg.Output:
        """
        Receive AssetIn data from the assessment asset. Select RAWcook
        items, retrieve row information for file from database. Using
        ParallelExecution, launch maximum of four parallel encodings.
        Update database if successfully encoded or failed.
        """
        log_prefix = f"[{key_prefix}] " if key_prefix else ""
        context.log.info(f"{log_prefix}Received new encoding data: {assessment}")
        if not assessment["RAWcook"]:
            context.log.info("No RAWcook sequences to process at this time.")
            return dg.Output(value={})

        # Check for accepted gaps / forced framerates
        for_rawcooking = []
        for fpath in assessment["RAWcook"]:
            root, seq = os.path.split(fpath)
            search = "SELECT status FROM encoding_status WHERE seq_id=?"
            result = context.resources.database.retrieve_seq_id_row(
                context, search, "fetchall", (seq,)
            )
            context.log.info(f"{log_prefix}Received row {result}")

            if "Accept gaps" in str(result):
                for_rawcooking.append(os.path.join(root, f"GAPS_{seq}"))
            elif "Force 24 FPS" in str(result):
                for_rawcooking.append(os.path.join(root, f"24FPS_{seq}"))
            elif "Force 16 FPS" in str(result):
                for_rawcooking.append(os.path.join(root, f"16FPS_{seq}"))
            else:
                for_rawcooking.append(fpath)

        # Create/execute parallel transcodes
        context.log.info(f"{log_prefix}Launcing RAWcooked multiprocessing encoding")
        transcode_tasks = [(folder,) for folder in for_rawcooking]
        results = context.resources.process_pool.map(transcode, transcode_tasks)

        # Filter out None vals
        completed_files = [r["path"] for r in results if r["success"] is not None]
        context.log.info(f"Completed {len(completed_files)} RAWcooked transcodes.")

        for data in results:
            seq = data["sequence"]
            arg = data["db_arguments"]
            entry = context.resources.database.append_to_database(context, seq, arg)
            context.log.info(f"{log_prefix}Written to Database: {entry}")
            for log in data["logs"]:
                if "WARNING" in log:
                    context.log.warning(f"{log_prefix}{log}")
                else:
                    context.log.info(f"{log_prefix}{log}")

        # Validate in function
        if not completed_files:
            return dg.Output(value={}, metadata={"successfully_complete": "0"})

        validation_tasks = [(folder,) for folder in completed_files]
        results = context.resources.process_pool.map(ffv1_validate, validation_tasks)
        validated_files = {
            "valid": [r["sequence"] for r in results if r["success"] is not False],
            "invalid": [r["sequence"] for r in results if r["success"] is False],
        }
        context.log.info(
            f"{log_prefix}Validation results: Valid={len(validated_files['valid'])}, "
            f"Invalid={len(validated_files['invalid'])}"
        )

        # Write data to log / db
        for data in results:
            seq = data["sequence"]
            args = data["db_arguments"]
            entry = context.resources.database.append_to_database(context, seq, args)
            context.log.info(f"{log_prefix}Written to Database: {entry}")
            for log in data["logs"]:
                if "WARNING" in log:
                    context.log.warning(f"{log_prefix}{log}")
                else:
                    context.log.info(f"{log_prefix}{log}")

        return dg.Output(
            value={
                "validated_files": validated_files["valid"],
                "invalid_files": validated_files["invalid"],
            },
            metadata={
                "successfully_complete": len(validated_files["valid"]),
                "failed_items": len(validated_files["invalid"]),
            },
        )

    return transcode_ffv1


def transcode(fullpath: tuple[str]) -> Dict[str, Any]:
    """Complete transcodes in parallel"""
    log_data = []

    gaps = fps24 = fps16 = False
    root, seq = os.path.split(fullpath[0])
    if seq.startswith("GAPS_"):
        gaps = True
        seq = seq.split("_", 1)[-1]
        fullpath = os.path.join(root, seq)
    elif seq.startswith("24FPS_"):
        fps24 = True
        seq = seq.split("_", 1)[-1]
        fullpath = os.path.join(root, seq)
    elif seq.startswith("16FPS_"):
        fps16 = True
        seq = seq.split("_", 1)[-1]
        fullpath = os.path.join(root, seq)
    else:
        fullpath = fullpath[0]

    transcodes_path = os.path.join(str(Path(fullpath).parents[1]), "ffv1_transcoding/")
    log_data.append("Encoding choice is RAWcooked")
    if not os.path.exists(fullpath):
        log_data.append(f"WARNING: Failed to find path {fullpath}. Exiting.")

        arguments = (
            ["status", "RAWcook failed"],
            ["encoding_complete", str(datetime.datetime.today())[:19]],
        )
        return {
            "sequence": seq,
            "success": False,
            "path": None,
            "db_arguments": arguments,
            "logs": log_data,
        }

    log_data.append(f"File path identified: {fullpath}")
    ffv1_path = os.path.join(transcodes_path, f"{seq}.mkv")
    log_data.append(f"Path for Matroska: {ffv1_path}")
    log_path = os.path.join(transcodes_path, f"{seq}.mkv.txt")
    log_data.append(f"Outputting log file to {log_path}")
    log_data.append("Calling Encoder function")

    # Set up encoding command
    output_v2 = utils.check_for_version_two(log_path)
    cmd = ["rawcooked", "-y", "--all"]

    if gaps is False:
        cmd.append("--no-accept-gaps")

    if output_v2 is True:
        cmd.extend(["--output-version", "2"])

    if fps16 is True:
        cmd.extend(["--framerate", "16"])
    if fps24 is True:
        cmd.extend(["--framerate", "24"])

    cmd.extend(
        [
            "-s",
            "5281680",
            f"{fullpath}",
            "-o",
            f"{ffv1_path}",
            ">>",
            f"{log_path}",
            "2>&1",
        ]
    )

    log_data.append(
        f"Calling RAWcooked with specific sequence command: {' '.join(cmd)}"
    )
    tic = time.perf_counter()
    try:
        subprocess.run(" ".join(cmd), shell=True, check=True)
    except subprocess.CalledProcessError as err:
        print(err)

    toc = time.perf_counter()
    mins = (toc - tic) // 60
    log_data.append(f"RAWcooked encoding took {mins} minutes")
    if not os.path.isfile(ffv1_path):
        log_data.append(
            "WARNING: RAWcooked encoding failed. Moving to failures folder."
        )
        if not os.path.isfile(ffv1_path):
            log_data.append("WARNING: Cannot find file, moving to failures folder")
            utils.move_to_failures(ffv1_path)
        utils.move_to_failures(fullpath)
        utils.move_log_to_dest(log_path, "failures")
        arguments = (
            ["status", "RAWcook failed"],
            ["encoding_complete", str(datetime.datetime.today())[:19]],
        )

        return {
            "sequence": seq,
            "success": False,
            "path": None,
            "db_arguments": arguments,
            "logs": log_data,
        }

    log_data.append("RAWcooked encoding completed. Ready for validation checks")
    checksum_data = utils.md5_hash(ffv1_path)
    log_data.append(f"Checksum: {checksum_data}")
    arguments = (
        ["status", "RAWcook completed"],
        ["encoding_complete", str(datetime.datetime.today())[:19]],
        ["encoding_retry", 0],
        ["encoding_log", log_path],
        ["derivative_path", ffv1_path],
        ["derivative_size", utils.get_folder_size(ffv1_path)],
        ["derivative_md5", checksum_data],
    )
    log_data.append(f"RAWcook completed successfully. Updating database:\n{arguments}")

    return {
        "sequence": seq,
        "success": True,
        "path": ffv1_path,
        "db_arguments": arguments,
        "logs": log_data,
    }


def ffv1_validate(fullpath):
    """
    Run validation checks against TAR
    """
    log_data = []
    error_message = []
    log_data.append(f"Received: {fullpath[0]}")
    if isinstance(fullpath, tuple):
        spath = fullpath[0]
    elif isinstance(fullpath, str):
        spath = fullpath

    if not os.path.exists(spath):
        log_data.append(f"WARNING: Failed to find path {spath}. Exiting.")
        log_data.append(utils.move_to_failures(spath))

        arguments = (
            ["status", "RAWcook failed"],
            ["validation_complete", str(datetime.datetime.today())[:19]],
        )
        return {
            "sequence": None,
            "success": False,
            "db_arguments": arguments,
            "logs": log_data,
        }

    fname = os.path.basename(spath)
    seq = fname.split(".")[0]
    dpath = os.path.join(str(Path(spath).parents[1]), "processing/", seq)
    log_data.append(f"Paths to work with:\n{dpath}\n{spath}")
    folder_size = utils.get_folder_size(dpath)
    file_size = utils.get_folder_size(spath)
    log_data.append(
        f"Found sizes in bytes:\n{folder_size} {dpath}\n{file_size} {spath}"
    )
    log = f"{spath}.txt"

    # Run chmod on MKV
    try:
        utils.recursive_chmod(spath, 0o777)
    except PermissionError as err:
        print(err)

    validation = True
    if not os.path.isfile(spath):
        log_data.append(f"WARNING: Filepath not found: {spath}")
        validation = False
        error_message = "RAWcook file not found"

    result = utils.mediaconch_mkv(spath)
    if result[0] != "Pass":
        log_data.append(result[1])
        log_data.append(f"WARNING: MKV file failed Mediaconch policy: {result[-1]}")
        validation = False
        error_message = "MKV policy failed, see validation log for details."
    log_data.append(f"MKV passed policy check: \n{result[1]}")

    # Check log for success statement
    success = utils.check_mkv_log(log)
    if success is False:
        validation = False
        log_data.append("WARNING: MKV log file returned Error warning")
        error_message = "Error found in RAWcooked log"
    log_data.append("Log for MKV passed checks")

    # Run RAWcook check pass
    success = utils.check_file(spath)
    if success is False:
        validation = False
        log_data.append("WARNING: Matroska failed --check pass")
        error_message = "FFV1 MKV failed --check pass"
    log_data.append("MKV file passed --check test")

    # Check MKV not smaller than source folder
    if file_size > folder_size:
        log_data.append(
            f"WARNING: Directory size is not smaller that folder: {file_size} <= {folder_size}"
        )
        validation = False
        error_message = "MKV file larger than original folder size"

    if validation is False:
        # Move files/logs to failure path
        log_data.append(f"WARNING: RAWcook MKV failed: {error_message}")
        utils.move_to_failures(spath)
        utils.move_to_failures(dpath)
        for line in log_data:
            utils.append_to_log(log, line)
        utils.move_log_to_dest(log, "failures")

        arguments = (
            ["status", "RAWcook failed"],
            ["validation_success", "No"],
            ["validation_complete", str(datetime.datetime.today())[:19]],
            ["error_message", error_message],
        )
        return {
            "sequence": seq,
            "success": validation,
            "db_arguments": arguments,
            "logs": log_data,
        }

    else:
        # Delete image sequence
        cpath = os.path.join(str(Path(spath).parents[1]), "processing/for_deletion/")
        if not os.path.exists(cpath):
            os.makedirs(cpath, exist_ok=True, mode=0o777)
        shutil.move(dpath, os.path.join(cpath, seq))
        seq_del = "Moved to for_deletion folder"
        log_data.append(f"Image sequence moved to {cpath}")
        # success = utils.delete_sequence(os.path.join(move_to, seq)))
        # seq_del = 'No'
        # if success:
        #     seq_del = 'Yes'

        # Move file to ingest
        success = utils.move_to_autoingest(spath)
        if not success:
            auto_move = "No"
        else:
            auto_move = "Yes"

        log_data.append("RAWcooked validation completed.")
        for line in log_data:
            utils.append_to_log(log, line)
        utils.move_log_to_dest(log, "transcode_logs")

        arguments = (
            ["status", "MKV validation complete"],
            ["validation_complete", str(datetime.datetime.today())[:19]],
            ["validation_success", "Yes"],
            ["error_message", "None"],
            ["sequence_deleted", seq_del],
            ["moved_to_autoingest", auto_move],
        )

        return {
            "sequence": seq,
            "success": validation,
            "db_arguments": arguments,
            "logs": log_data,
        }
