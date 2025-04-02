import os
import tarfile
import datetime
import dagster as dg
from pathlib import Path
from typing import List, Dict, Optional, Any
from . import utils


def build_archiving_asset(key_prefix: Optional[str] = None):
    '''
    New factory function that returns the asset with optional key prefix.
    '''
    
    # Build the asset key with optional prefix
    asset_key = [f"{key_prefix}", "create_tar"]
    
    # Build input keys with prefix if needed
    ins_dict = {}
    if key_prefix:
        asset_key = [f"{key_prefix}", "create_tar"]
        ins_dict["assess_seqs"] = dg.AssetIn([f"{key_prefix}", "assess_sequence"])
    else:
        asset_key = "create_tar"
        ins_dict["assess_seqs"] = dg.AssetIn("assess_sequence")

    @dg.asset(
        key=asset_key,
        ins=ins_dict,
        required_resource_keys={'database', 'process_pool'}
    )
    def create_tar(
        context: dg.AssetExecutionContext,
        assess_seqs: Dict[str, List[str]],
    )  -> dg.Output:
        '''
        Receive dictionary of folder paths, selects those suitable for TAR wrap.
        Builds MD5 manifest of sequence contents, TAR wraps file then compares
        manifest with internal TAR checksums. Updates CID record with Python
        tar file statement, updates database and passes list of TAR filepaths
        to the validation asset.
        '''
        log_prefix = f"[{key_prefix}] " if key_prefix else ""
        context.log.info(f"{log_prefix}Received new encoding data: {assess_seqs}")

        if not assess_seqs['TAR']:
            context.log.info(f"{log_prefix}No TAR sequences to process at this time.")
            return []

        tar_tasks = [(folder,) for folder in assess_seqs['TAR']]

        results = context.resources.process_pool.map(tar_wrap, tar_tasks)
        completed_files = [r['path'] for r in results if r['success'] is not False]
        context.log.info(f"{log_prefix}Successfully completed {len(completed_files)} TAR archives: \n{results}")

        success_list = []
        for data in results:
            context.log.info(data)
            seq = data['sequence']
            if data['success'] is True:
                success_list.append(data['path'])
            args = data['db_arguments']
            entry = context.resources.database.append_to_database(context, seq, args)
            context.log.info(f"{log_prefix}Written to Database: {entry}")
            for log in data['logs']:
                if 'WARNING' in log:
                    context.log.warning(f"{log_prefix} {log}")
                else:
                    context.log.info(f"{log_prefix} {log}")

        # Validate in function
        if success_list is None:
            return dg.Output(
                value={},
                metadata={
                    "successfully_complete": '0'
                }
            )
        validation_tasks = [(folder,) for folder in success_list]
        results = context.resources.process_pool.map(tar_validate, validation_tasks)
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
                    context.log.warning(f"{log_prefix}{log}")
                else:
                    context.log.info(f"{log_prefix}{log}")

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
    return create_tar


def tar_wrap(fullpath: str) -> Dict[str, Any]:
    '''
    TAR wrap under parallelisation
    '''

    log_data = []
    root, tar_source = os.path.split(fullpath[0])
    local_log = os.path.join(str(Path(root).parents[0]), f'tar_wrapping/{tar_source}_tar_wrap.log')
    if not os.path.exists(fullpath[0]):
        log_data.append(f"WARNING: Failed to find path {fullpath[0]}. Exiting.")
        arguments = (
            ['status', 'TAR failure'],
            ['error_message', f'Path failed: {fullpath[0]}']
        )
        return {
            "sequence": tar_source,
            "success": False,
            "path": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    log_data.append(f"==== New path for TAR wrap: {fullpath[0]} ====")

    # Calculate checksum manifest for supplied fullpath
    local_md5 = {}
    for root, _, files in os.walk(fullpath[0]):
        for file in files:
            if file.endswith(('.ini', '.md5', '.DS_Store', '.')):
                continue
            if 'tar_wrap.log' in file:
                continue
            rel_path = os.path.relpath(root, os.path.dirname(fullpath[0]))
            if rel_path == tar_source or rel_path.startswith(tar_source + os.sep):
                dct = utils.get_checksum(os.path.join(root, file))
                local_md5.update(dct)
    log_data.append(f"Local MD5 manifest created: {local_md5}")

    # Tar folder creation
    utils.append_to_tar_log(local_log, f"Beginning TAR wrap now... {fullpath[0]}")
    log_data.append("Beginning TAR wrap now")
    tar_path = utils.tar_item(fullpath[0])
    log_data.append("TAR wrap completed")
    if tar_path is None:
        log_data.append("TAR wrap FAILED! See local logs for details.")
        utils.append_to_tar_log(local_log, f"==== Failure exit: {fullpath[0]} ====")
        arguments = (
            ['status', 'TAR failure'],
            ['error_message', 'TAR wrap failed to archive sequence']
        )
        return {
            "sequence": tar_source,
            "success": False,
            "path": tar_path,
            "db_arguments": arguments,
            "logs": log_data
        }

    # Print checksums for local / create and print for TAR log
    utils.append_to_tar_log(local_log, "Checksums for local files (excluding images):")
    for key, val in local_md5.items():
        if not key.lower().endswith(('.dpx', '.tif', '.tiff', '.jp2', '.j2k', '.jpf', '.jpm', '.jpg2', '.j2c', '.jpc', '.jpx', '.mj2')):
            data = f"{val} -- {key}"
            utils.append_to_tar_log(local_log, f"\t{data}")
    tar_content_md5 = utils.get_checksums(tar_path, tar_source)
    utils.append_to_tar_log(local_log, "Checksums for TAR wrapped contents (excluding images):")
    log_data.append("TAR MD5 manifest created (MD5s excluding images):")
    for key, val in tar_content_md5.items():
        if not key.lower().endswith(('.dpx', '.tif', '.tiff', '.jp2', '.j2k', '.jpf', '.jpm', '.jpg2', '.j2c', '.jpc', '.jpx', '.mj2')):
            data = f"{val} -- {key}"
            utils.append_to_tar_log(local_log, f"\t{data}")
            log_data.append(data)

    # Compare manifests
    tar_fail = False
    if local_md5 == tar_content_md5:
        log_data.append("MD5 Manifests match, adding manifest to TAR file and moving to autoingest.")
        md5_manifest = utils.make_manifest(tar_path, tar_content_md5)
        if not md5_manifest:
            log_data.append("WARNING: Failed to create checksum manifest.")
            log_data.append(utils.move_to_failures(fullpath[0]))
            log_data.append(utils.move_to_failures(tar_path))
            arguments = (
                ['status', 'TAR failure'],
                ['error_message', 'Failed to create checksum manifest']
            )
            tar_fail = True
        else:
            log_data.append(f"TAR checksum manifest created. Adding to TAR file {tar_path}")
            try:
                arc_path = os.path.split(md5_manifest)
                tar = tarfile.open(tar_path, 'a:')
                tar.add(md5_manifest, arcname=f"{arc_path[1]}")
                tar.close()
                os.remove(md5_manifest)
            except Exception as exc:
                log_data.append(f"WARNING: Unable to add MD5 manifest to TAR file. Moving TAR file to failures folder.\n{exc}")
                log_data.append(utils.move_to_failures(tar_path))
                arguments = (
                    ['status', 'TAR failure'],
                    ['error_message', 'Failed to embed MD5 manifest into TAR file']
                )
                tar_fail = True

    else:
        log_data.append("MD5 checksum manifests did not match. Moving to failures")
        utils.append_to_tar_log(local_log, "Checksum mismatch between TAR and source sequence. Moving to failures/")
        log_data.append(utils.move_to_failures(fullpath[0]))
        log_data.append(utils.move_to_failures(tar_path))
        arguments = (
            ['status', 'TAR failure'],
            ['error_message', 'MD5 checksum mismatch between TAR and source']
        )
        tar_fail = True

    if tar_fail is True:
        utils.append_to_tar_log(local_log, f"==== Failure exit: {fullpath[0]} ====")
        return {
            "sequence": tar_source,
            "success": False,
            "path": tar_path,
            "db_arguments": arguments,
            "logs": log_data
        }
    else:
        ## This section needs test / import of adlib ##
        priref, _, _ = utils.get_file_type(tar_source)
        log_data.append("TAR wrap completed successfully. Updating CID item record with TAR wrap method")
        if not priref:
            utils.append_to_tar_log(local_log, f"Cannot find Priref associated with file: {tar_file}. Please add manually.")
        if len(priref) > 0:
            utils.append_to_tar_log(local_log, f"Updating CID Item record with TAR wrap data: {priref}")
            tar_file = os.path.basename(tar_path)
            # success = utils.write_to_cid(priref, tar_file)
            #if not success:
            #    utils.append_to_tar_log(local_log, f"Failed to write Python tarfile message to CID item record: {priref} {tar_file}. Please add manually.")

        # Get complete size of file following TAR wrap
        file_stats = os.stat(tar_path)
        file_size = file_stats.st_size
        data = utils.md5_hash(tar_path)
        arguments = (
            ['status', 'TAR wrap completed'],
            ['encoding_complete', str(datetime.datetime.today())[:19]],
            ['derivative_path', tar_path],
            ['derivative_size', file_size],
            ['derivative_md5', data],
            ['encoding_log', local_log],
            ['encoding_retry', '0']
        )

        utils.append_to_tar_log(local_log, f"TAR wrap completed successfully. Updating database:\n{arguments}\n")
        log_data.append(f"==== Log actions complete: {fullpath[0]} ====")

        return {
            "sequence": tar_source,
            "success": True,
            "path": tar_path,
            "db_arguments": arguments,
            "logs": log_data
        }


def tar_validate(fullpath):
    '''
    Run validation checks against TAR
    '''
    log_data = []
    errors = []

    log_data.append(f"Received: {fullpath}")
    spath = fullpath[0]
    fname = os.path.basename(spath)
    seq = fname.split('.')[0]
    dpath = os.path.join(str(Path(spath).parents[1]), 'processing/', seq)
    log_data.append(f"Paths to work with:\n{dpath}\n{spath}")
    folder_size = utils.get_folder_size(dpath)
    file_size = utils.get_folder_size(spath)
    log_data.append(f"Found sizes:\n{folder_size} {dpath}\n{file_size} {spath}")

    if fname.endswith('.tar'):
        log = os.path.join(str(Path(spath).parents[1]), f'tar_wrapping/{seq}_tar_wrap.log')
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
            log_data.append("Error: TAR file smaller than original folder size...")
            for line in log_data:
                utils.append_to_tar_log(log, line)
            utils.move_log_to_dest(log, 'failures')

            arguments = (
                ['status', 'TAR validation failure'],
                ['validation_success', 'False'],
                ['validation_complete', str(datetime.datetime.today())[:19]],
                ['error_message', ', '.join(errors)]
            )

            return {
                "sequence": seq,
                "success": validation,
                "db_arguments": arguments,
                "logs": log_data
            }
        else:
            '''
            # Delete source sequence
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
            '''
            seq_del = "Turned off for test"
            auto_move = "Turned off for test"
            log_data.append("TAR wrap validation completed successfully.")
            for line in log_data:
                utils.append_to_tar_log(log, line)
            utils.move_log_to_dest(log, 'tar_logs')

            arguments = (
                ['status', 'TAR validation complete'],
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
