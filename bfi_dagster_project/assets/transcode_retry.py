import os
import time
import shutil
import datetime
import subprocess
from pathlib import Path
import dagster as dg
from typing import List, Optional
from . import utils


def build_transcode_retry_asset(key_prefix: Optional[str] = None):
    '''
    New factory function that returns the asset with key prefix.
    '''

    # Build the asset key with optional prefix
    if key_prefix:
        asset_key = [f"{key_prefix}", "reencode_failed_asset"]
    else:
        asset_key = "reencode_failed_asset"
    
    # Define config schema
    config_schema = {
        "sequence": dg.Field(
            dg.String,
            is_required=False,
            description="Path to the sequence that needs to be reencoded",
        )
    }

    @dg.asset(
        key=asset_key,
        required_resource_keys={'database'},
        config_schema=config_schema
    )
    def reencode_failed_asset(
        context: dg.AssetExecutionContext,
    ) -> dg.Output:
        '''
        Receive context op_config containting folder path for failed transcode
        attempt, retrieves database row data and begins re-encode attempt.
        List containing filepath is passed to validation asset.
        '''
        if not context.op_config.get('sequence'):
            return dg.Output(value={})
        log_prefix = f"[{key_prefix}] " if key_prefix else ""
        fullpath = context.op_config.get('sequence')
        root, seq = os.path.split(fullpath)
        context.log.info(f"{log_prefix}Received new encoding data: {fullpath}")

        search = "SELECT * FROM encoding_status WHERE seq_id=?"
        data = context.resources.database.retrieve_seq_id_row(context, search, 'fetchone', (seq,))
        context.log.info(f"{log_prefix}Row retrieved: {data}")
        status = data[2]
        choice = data[15]
        context.log.info(f"{log_prefix}==== Retry RAWcook encoding: {fullpath} ====")
        if status != "Pending retry":
            context.log.error(f"{log_prefix}Sequence not suitable for retry. Exiting.")
            return dg.Output(value={})
        context.log.info(f"{log_prefix}Status indicates selected for retry successful")
        if choice != "RAWcook":
            context.log.error(f"{log_prefix}Sequence not suitable for RAWcooked re-encoding. Exiting.")
            return dg.Output(value={})
        context.log.info("{log_prefix}Encoding choice is RAWcooked")
        if not os.path.exists(fullpath):
            context.log.error(f"{log_prefix}Failed to find path {fullpath}. Exiting.")
            return dg.Output(value={})
        context.log.info(f"{log_prefix}File path identified: {fullpath}")

        # Check for accepted gaps / forced framerates
        gaps = fps24 = fps16 = False
        if 'Accept gaps' in str(data):
            gaps = True
        elif 'Force 24 FPS' in str(data):
            fps24 = True
        elif 'Force 16 FPS' in str(data):
            fps16 = True

        transcodes_path = os.path.join(str(Path(fullpath).parents[1]), 'ffv1_transcoding/')
        ffv1_path = os.path.join(transcodes_path, f"{seq}.mkv")
        context.log.info(f"Path for Matroska: {ffv1_path}")
        if os.path.isfile(ffv1_path):
            context.log.info(f"{log_prefix}Delete existing transcode attempt.")
            os.remove(ffv1_path)

        log_path = os.path.join(transcodes_path, f"{seq}.mkv.txt")
        context.log.info(f"Outputting log file to {log_path}")
        context.log.info("Calling Encoder function")

        # Set up encoding command
        output_v2 = utils.check_for_version_two(log_path)
        cmd = [
            "rawcooked", "-y", "--all"
        ]

        if gaps is False:
            cmd.append("--no-accept-gaps")

        if output_v2 is True:
            cmd.extend(["--output-version", "2"])

        if fps16 is True:
            cmd.extend(["--framerate", "16"])
        if fps24 is True:
            cmd.extend(["--framerate", "24"])

        cmd.extend([
            "-s", "5281680", f"{fullpath}",
            "-o", f"{ffv1_path}",
            ">>", f"{log_path}", "2>&1"
        ])

        context.log.info(f"Calling RAWcooked with specific sequence command: {' '.join(cmd)}")
        tic = time.perf_counter()
        try:
            subprocess.run(" ".join(cmd), shell=True, check=True)
        except subprocess.CalledProcessError as err:
            print(err)

        toc = time.perf_counter()
        mins = (toc - tic) // 60
        context.log.info(f"RAWcooked encoding took {mins} minutes")
        if not os.path.isfile(ffv1_path):
            context.log.warning("WARNING: RAWcooked encoding failed. Moving to failures folder.")
            if not os.path.isfile(ffv1_path):
                context.log.warning("WARNING: Cannot find file, moving to failures folder")
                utils.move_to_failures(ffv1_path)
            utils.move_to_failures(fullpath)
            utils.move_log_to_dest(log_path, 'failures')
            arguments = (
                ['status', 'RAWcook failed'],
                ['encoding_complete', str(datetime.datetime.today())[:19]]
            )
            context.log.warning(f"{log_prefix}RAWcooked encoding failed. Updating database:\n{arguments}")
            entry = context.resources.database.append_to_database(context, seq, arguments)
            return dg.Output(value={})

        context.log.info("RAWcooked encoding completed. Ready for validation checks")
        checksum_data = utils.md5_hash(ffv1_path)
        context.log.info(f"Checksum: {checksum_data}")
        arguments = (
            ['status', 'RAWcook retry completed'],
            ['encoding_complete', str(datetime.datetime.today())[:19]],
            ['encoding_log', log_path],
            ['derivative_path', ffv1_path],
            ['derivative_size', utils.get_folder_size(ffv1_path)],
            ['derivative_md5', checksum_data]
        )
        context.log.info(f"RAWcook completed successfully. Updating database:\n{arguments}")
        entry = context.resources.database.append_to_database(context, seq, arguments)

        # Validate in function
        results = ffv1_validate(ffv1_path)
        validated_files = {
            "valid": [r['sequence'] for r in results if r['success'] is not False],
            "invalid": [r['sequence'] for r in results if r['success'] is False]
        }
        context.log.info(f"{log_prefix}Validation results: Valid={len(validated_files['valid'])}, "
                        f"Invalid={len(validated_files['invalid'])}")

        # Write data to log / db
        for data in results:
            seq = data['sequence']
            args = data['db_arguments']
            entry = context.resources.database.append_to_database(context, seq, args)
            context.log.info(f"{log_prefix}Written to Database: {entry}")
            for log in data['logs']:
                if 'WARNING' in log:
                    context.log.warning(f"{log_prefix} {log}")
                else:
                    context.log.info(f"{log_prefix} {log}")

        return dg.Output(
            value={
                "validated_files": validated_files['valid'],
                "invalid_files": validated_files['invalid']
            },
            metadata={
                "successfully_complete": len(validated_files['valid']),
                "failed_items": len(validated_files['invalid'])
            }
        )
    return reencode_failed_asset


def ffv1_validate(spath):
    '''
    Run validation checks against FFV1 MKV
    '''
    log_data = []
    error_message = []

    log_data.append(f"Received: {spath}")

    if not os.path.exists(spath):
        log_data.append(f"WARNING: Failed to find path {spath}. Exiting.")
        log_data.append(utils.move_to_failures(spath))

        arguments = (
            ['status', 'RAWcook failed'],
            ['validation_complete', str(datetime.datetime.today())[:19]]
        )
        return {
            "sequence": None,
            "success": False,
            "db_arguments": arguments,
            "logs": log_data
        }

    fname = os.path.basename(spath)
    seq = fname.split('.')[0]
    dpath = os.path.join(str(Path(spath).parents[1]), 'processing/', seq)
    log_data.append(f"Paths to work with:\n{dpath}\n{spath}")
    folder_size = utils.get_folder_size(dpath)
    file_size = utils.get_folder_size(spath)
    log_data.append(f"Found sizes in bytes:\n{folder_size} {dpath}\n{file_size} {spath}")
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
        error_message = 'RAWcook file not found'

    result = utils.mediaconch_mkv(spath)
    if result[0] != "Pass":
        log_data.append(result[1])
        log_data.append(f"WARNING: MKV file failed Mediaconch policy: {result[-1]}")
        validation = False
        error_message = 'MKV policy failed, see validation log for details.'
    log_data.append(f"MKV passed policy check: \n{result[1]}")

    # Check log for success statement
    success = utils.check_mkv_log(log)
    if success is False:
        validation = False
        log_data.append("WARNING: MKV log file returned Error warning")
        error_message = 'Error found in RAWcooked log'
    log_data.append("Log for MKV passed checks")

    # Run RAWcook check pass
    success = utils.check_file(spath)
    if success is False:
        validation = False
        log_data.append("WARNING: Matroska failed --check pass")
        error_message = 'FFV1 MKV failed --check pass'
    log_data.append("MKV file passed --check test")

    # Check MKV not smaller than source folder
    if file_size > folder_size:
        log_data.append(f"WARNING: Directory size is not smaller that folder: {file_size} <= {folder_size}")
        validation = False
        error_message = 'MKV file larger than original folder size'

    if validation is False:
        # Move files/logs to failure path
        log_data.append(f"WARNING: RAWcook MKV failed: {error_message}")
        utils.move_to_failures(spath)
        utils.move_to_failures(dpath)
        for line in log_data:
            utils.append_to_tar_log(log, line)
        utils.move_log_to_dest(log, 'failures')

        arguments = (
            ['status', 'RAWcook failed'],
            ['validation_success', 'No'],
            ['validation_complete', str(datetime.datetime.today())[:19]],
            ['error_message', error_message]
        )
        return {
            "sequence": seq,
            "success": validation,
            "db_arguments": arguments,
            "logs": log_data
        }

    else:
        # Move image sequence and delete
        cpath = os.path.join(str(Path(spath).parents[1]), 'processing/for_deletion/')
        if not os.path.exists(cpath):
            os.makedirs(cpath, exist_ok=True, mode=0o777)
        shutil.move(dpath, os.path.join(cpath, seq))
        seq_del = 'Moved to for_deletion folder'
        log_data.append(f"Image sequence moved to {cpath}")
        #success = utils.delete_sequence(dpath)
        #seq_del = 'No'
        #if success:
        #    seq_del = 'Yes'

        # Move file to ingest
        success = utils.move_to_autoingest(spath)
        if not success:
            auto_move = 'No'
        else:
            auto_move = 'Yes'

        log_data.append("RAWcooked validation completed.")
        for line in log_data:
            utils.append_to_tar_log(log, line)
        utils.move_log_to_dest(log, 'transcode_logs')

        arguments = (
            ['status', 'MKV validation complete'],
            ['validation_complete', str(datetime.datetime.today())[:19]],
            ['validation_success', 'Yes'],
            ['error_message', 'None'],
            ['sequence_deleted', seq_del],
            ['moved_to_autoingest', auto_move]
        )

        return {
            "sequence": seq,
            "success": validation,
            "db_arguments": arguments,
            "logs": log_data
        }

# Import note
reencode_failed_asset = build_transcode_retry_asset()