import os
import shutil
import datetime
import dagster as dg
from dotenv import load_dotenv
from typing import List, Dict, Optional
from ..resources import SQLiteResource, process_pool
from . import utils

# Glob vars
load_dotenv()
LOG_PATH = os.environ.get('LOGS')
PROC_PATH = os.environ.get('IMG_PROC')


def run_validate(fullpath):
    '''
    Run validation checks against TAR
    '''
    log_data = []
    errors = []

    log_data.append(f"Received: {fullpath}")
    spath = fullpath[0]
    fname = os.path.basename(spath)
    seq = fname.split('.')[0]
    dpath = os.path.join(PROC_PATH, 'processing/', seq)
    log_data.append(f"Paths to work with:\n{dpath}\n{spath}")
    folder_size = utils.get_folder_size(dpath)
    file_size = utils.get_folder_size(spath)
    log_data.append(f"Found sizes:\n{folder_size} {dpath}\n{file_size} {spath}")

    if fname.endswith('.tar'):
        log = os.path.join(LOG_PATH, f'tar_logs/{seq}_tar_wrap.log')
        log_data.append(log)
        validation = True
        if not os.path.isfile(spath):
            validation = False
            log_data.append(f"Filepath supplied does not exist: {spath}")
            errors.append('TAR file not found')

        if file_size < folder_size:
            validation = False
            log_data.append("TAR file is smaller than source. Failing TAR file.")
            errors.append('TAR file smaller than sequence')

        diff = file_size - folder_size
        if diff > 107374100:
            validation = False
            log_data.append(f"Size difference between source folder/TAR > 100MB. {diff} size - failing TAR.")
            errors.append("TAR file over 100MB larger than sequence.")

        # Check logs contain success statement
        success = utils.check_tar_log(log)
        if success is False:
            validation = False
            log_data.append("Logs contain error message, failing this TAR")
            errors.append('Error message found in log')

        if validation is False:
            # Move files to failure path
            log_data.append(utils.move_to_failures(spath))
            log_data.append(utils.move_to_failures(dpath))

            # Move log to failure
            utils.move_log_to_failures(log)

            arguments = (
                ['status', 'TAR validation failure'],
                ['validation_success', 'False'],
                ['validation_complete', str(datetime.datetime.today())[:19]],
                ['error_message', ', '.join(errors)]
            )
            log_data.append("Error: TAR file smaller than original folder size...")
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
                auto_move = False
            else:
                auto_move = True

            log_data.append("TAR wrap validation completed successfully.")
            arguments = (
                ['status', 'TAR validation complete'],
                ['validation_complete', str(datetime.datetime.today())[:19]],
                ['validation_success', 'True'],
                ['error_message', 'None'],
                ['sequence_deleted', str(seq_del)],
                ['moved_to_autoingest', str(auto_move)]
            )
            log_data.append(f"Validation completed successfully.")
            return {
                "sequence": seq,
                "success": validation,
                "db_arguments": arguments,
                "logs": log_data
            }

    elif fname.endswith('.mkv'):
        log = os.path.join(LOG_PATH, f'transcode_logs/{seq}.mkv.txt')
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
            error_message = f'MKV policy failed, see validation log for details.'
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
            utils.move_log_to_failures(log)

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
            arguments = (
                ['status', 'MKV validation complete'],
                ['validation_complete', str(datetime.datetime.today())[:19]],
                ['validation_success', 'True'],
                ['error_message', 'None'],
                ['sequence_deleted', str(seq_del)],
                ['moved_to_autoingest', str(auto_move)]
            )
            log_data.append("Validation completed successfully.")
            return {
                "sequence": seq,
                "success": validation,
                "db_arguments": arguments,
                "logs": log_data
            }


@dg.asset(
    deps=['transcode_ffv1', 'create_tar', 'reencode_failed_asset'],
    ins={"ffv1_result": dg.AssetIn("transcode_ffv1"),
         "tar_result": dg.AssetIn("create_tar"),
         "ffv1_retry": dg.AssetIn("reencode_failed_asset")},
         required_resource_keys={'database', 'process_pool'})
def validate_output(
    context: dg.AssetExecutionContext,
    ffv1_result: List[str],
    tar_result: List[str],
    ffv1_retry: Optional[List[str]] = None,
) -> dg.Output:
    '''
    Validation asset receives list of folder paths from three
    assets (ffv1, tar and ffv1 retry assets). Depending on ext
    type runs a series of validation checks, before passing or
    failing the files and updating database.
    '''
    ffv1_retry_paths = ffv1_retry or []
    all_results = ffv1_result + tar_result + ffv1_retry_paths
    if not all_results:
        context.log.info("No files handed to validation script")
        return None

    context.log.info("Received: %s", all_results)

    # Configure parallel validation
    validation_tasks = [(folder,) for folder in all_results]
    results = context.resources.process_pool.map(run_validate, validation_tasks)
    validated_files = {
        "valid": [r['sequence'] for r in results if r['success'] is not False],
        "invalid": [r['sequence'] for r in results if r['success'] is False]
    }
    context.log.info(f"Validation results: Valid={len(validated_files['valid'])}, "
                    f"Invalid={len(validated_files['invalid'])}")

    # Write data to log / db
    for data in results:
        seq = data['sequence']
        args = data['db_arguments']
        entry = context.resources.database.append_to_database(context, seq, args)
        context.log.info(f"Written to Database: {entry}")
        for log in data['logs']:
            if 'WARNING' in log:
                context.log.warning(log)
            else:
                context.log.info(log)

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


defs = dg.Definitions(assets=[validate_output])
