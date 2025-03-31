import os
import datetime
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
    ) -> List[str]:
        '''
        Receive context op_config containting folder path for failed transcode
        attempt, retrieves database row data and begins re-encode attempt.
        List containing filepath is passed to validation asset.
        '''
        if not context.op_config.get('sequence'):
            return []
        log_prefix = f"[{key_prefix}] " if key_prefix else ""
        fullpath = context.op_config.get('sequence')
        seq = os.path.basename(fullpath)
        context.log.info(f"{log_prefix}Received new encoding data: {fullpath}")

        search = "SELECT * FROM encoding_status WHERE seq_id=?"
        data = context.resources.database.retrieve_seq_id_row(context, search, 'fetchone', (seq,))
        context.log.info(f"{log_prefix}Row retrieved: {data}")
        status = data[2]
        choice = data[15]
        context.log.info(f"{log_prefix}==== Retry RAWcook encoding: {fullpath} ====")
        if status != "Pending retry":
            context.log.error(f"{log_prefix}Sequence not suitable for retry. Exiting.")
            return []
        context.log.info(f"{log_prefix}Status indicates selected for retry successful")
        if choice != "RAWcook":
            context.log.error(f"{log_prefix}Sequence not suitable for RAWcooked re-encoding. Exiting.")
            return []
        context.log.info("{log_prefix}Encoding choice is RAWcooked")
        if not os.path.exists(fullpath):
            context.log.error(f"{log_prefix}Failed to find path {fullpath}. Exiting.")
            return []
        context.log.info(f"{log_prefix}File path identified: {fullpath}")

        ffv1_path = os.path.join(str(Path(fullpath).parents[1]), f"ffv1_transcoding/{seq}.mkv")
        if os.path.isfile(ffv1_path):
            context.log.info(f"{log_prefix}Delete existing transcode attempt.")
            os.remove(ffv1_path)
        context.log.info(f"{log_prefix}Path for Matroska: %s", ffv1_path)

        log_path = f"{ffv1_path}.txt"
        context.log.info(f"{log_prefix}Outputting log file to %s", log_path)
        context.log.info(f"{log_prefix}Calling Encoder function")
        output_path = utils.encoder(fullpath, ffv1_path, log_path)

        if output_path is None:
            context.log.warning(f"{log_prefix}RAWcooked encoding failed. Moving to failures folder.")
            if not os.path.isfile(ffv1_path):
                context.log.warning(f"{log_prefix}Cannot find file, moving to failures folder")
                utils.move_to_failures(ffv1_path)
            utils.move_to_failures(fullpath)
            utils.move_log_to_dest(log_path, 'failures')
            arguments = (
                ['status', 'RAWcook failed'],
                ['encoding_complete', str(datetime.datetime.today())[:19]]
            )
            context.log.info(f"{log_prefix}RAWcooked encoding failed. Updating database:\n{arguments}")
            entry = context.resources.database.append_to_database(context, seq, arguments)
            context.log.info(entry)
            return []
        context.log.info(f"{log_prefix}RAWcooked encoding completed. Ready for validation checks")
        checksum_data = utils.get_checksum(ffv1_path)
        context.log.info(f"{log_prefix}Checksum: %s", data[f"{seq}.mkv"])
        arguments = (
            ['status', 'RAWcook completed'],
            ['encoding_complete', str(datetime.datetime.today())[:19]],
            ['derivative_path', ffv1_path],
            ['derivative_size', utils.get_folder_size(ffv1_path)],
            ['derivative_md5', checksum_data[f"{seq}.mkv"]]
        )
        context.log.info(f"{log_prefix}RAWcook completed successfully. Updating database:\n%s", arguments)
        entry = context.resources.database.append_to_database(context, seq, arguments)
        context.log.info(f"{log_prefix}Row data written: {entry}")

        # Validate in function
        results = context.resources.process_pool.map(ffv1_validate, [ffv1_path])
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


def ffv1_validate(fullpath):
    '''
    Run validation checks against TAR
    '''
    log_data = []
    error_message = []

    log_data.append(f"Received: {fullpath}")
    spath = fullpath[0]
    fname = os.path.basename(spath)
    seq = fname.split('.')[0]
    dpath = os.path.join(str(Path(spath).parents[1]), 'processing/', seq)
    log_data.append(f"Paths to work with:\n{dpath}\n{spath}")
    folder_size = utils.get_folder_size(dpath)
    file_size = utils.get_folder_size(spath)
    log_data.append(f"Found sizes:\n{folder_size} {dpath}\n{file_size} {spath}")

    log = os.path.join(str(Path(spath).parents[1]), f'transcode_logs/{seq}.mkv.txt')

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
            ['status', 'MKV validation failure'],
            ['validation_success', 'False'],
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
        # Move image sequence to_delete and delete
        success = utils.delete_sequence(dpath)
        seq_del = False
        if success:
            seq_del = True

        # Move file to ingest
        success = utils.move_to_autoingest(spath)
        if not success:
            auto_move = 'False'
        else:
            auto_move = 'True'

        log_data.append("RAWcooked validation completed.")
        for line in log_data:
            utils.append_to_tar_log(log, line)
        utils.move_log_to_dest(log, 'transcode_logs')

        arguments = (
            ['status', 'MKV validation complete'],
            ['validation_complete', str(datetime.datetime.today())[:19]],
            ['validation_success', 'True'],
            ['error_message', 'None'],
            ['sequence_deleted', str(seq_del)],
            ['moved_to_autoingest', str(auto_move)]
        )

        return {
            "sequence": seq,
            "success": validation,
            "db_arguments": arguments,
            "logs": log_data
        }

# Import note
reencode_failed_asset = build_transcode_retry_asset()