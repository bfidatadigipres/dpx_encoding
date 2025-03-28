import os
import datetime
import subprocess
import dagster as dg
from pathlib import Path
from typing import Dict, List, Any, Optional
from . import utils


def build_transcode_ffv1_asset(key_prefix: Optional[str] = None):
    '''
    New factory function that returns the asset with optional key prefix.
    '''
    
    # Build the asset key with optional prefix
    asset_key = [f"{key_prefix}", "transcode_ffv1"] if key_prefix else "transcode_ffv1"
    
    # Build input keys with prefix if needed
    ins_dict = {}
    if key_prefix:
        ins_dict["assessment"] = dg.AssetIn([f"{key_prefix}", "assess_sequence"])
    else:
        ins_dict["assessment"] = dg.AssetIn("assess_sequence")

    @dg.asset(
        #ins={"assessment": dg.AssetIn("assess_sequence")},
        key=asset_key,
        ins=ins_dict,
        required_resource_keys={"database", "process_pool"}
    )
    def transcode_ffv1(
        context: dg.AssetExecutionContext,
        assessment: Dict[str, List[str]],
    ) -> List[str]:
        '''
        Receive AssetIn data from the assessment asset. Select RAWcook
        items, retrieve row information for file from database. Using
        ParallelExecution, launch maximum of four parallel encodings.
        Update database if successfully encoded or failed.
        '''
        context.log.info("Received new encoding data: %s", assessment)
        if not assessment['RAWcook']:
            context.log.info("No RAWcook sequences to process at this time.")
            return []

        # Create/execute parallel transcodes
        transcode_tasks = [(folder,) for folder in assessment['RAWcook']]
        results = context.resources.process_pool.map(transcode, transcode_tasks)

        # Filter out None vals
        completed_files = [r['path'] for r in results if r['success'] is not None]
        context.log.info(f"Completed {len(completed_files)} RAWcooked transcodes.")

        for data in results:
            seq = data['sequence']
            arg = data["db_arguments"]
            entry = context.resources.database.append_to_database(context, seq, arg)
            context.log.info(f"Written to Database: {entry}")
            for log in data['logs']:
                if 'WARNING' in log:
                    context.log.warning(log)
                else:
                    context.log.info(log)

        return completed_files
    return transcode_ffv1


def transcode(fullpath: tuple[str]) -> Dict[str, Any]:
    ''' Complete transcodes in parallel '''
    log_data = []

    seq = os.path.basename(fullpath[0])
    transcodes_path = os.path.join(str(Path(fullpath[0]).parents[1]), 'ffv1_transcoding/')
    log_data.append("Encoding choice is RAWcooked")
    if not os.path.exists(fullpath[0]):
        log_data.append(f"WARNING: Failed to find path {fullpath[0]}. Exiting.")
        arguments = (
            ['status', 'RAWcook failed'],
            ['encoding_complete', str(datetime.datetime.today())[:19]]
        )
        return {
            "sequence": seq,
            "success": False,
            "path": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    log_data.append(f"File path identified: {fullpath[0]}")
    ffv1_path = os.path.join(transcodes_path, f"{seq}.mkv")
    log_data.append(f"Path for Matroska: {ffv1_path}")
    log_path = os.path.join(transcodes_path, f"{seq}.mkv.txt")
    log_data.append(f"Outputting log filet to {log_path}")
    log_data.append("Calling Encoder function")

    # Encode
    output_v2 = utils.check_for_version_two(log_path)
    if output_v2 is True:
        cmd = [
            "rawcooked", "-y", "--all",
            "--no-accept-gaps",
            "--output-version", "2",
            "-s", "5281680", fullpath[0],
            "--output-name", ffv1_path,
            "&>", log_path
        ]
    else:
        cmd = [
            "rawcooked", "-y", "--all",
            "--no-accept-gaps",
            "-s", "5281680", fullpath[0],
            "--output-name", ffv1_path,
            "&>", log_path
        ]

    try:
        subprocess.run(" ".join(cmd), shell=True, check=True, timeout=300)
    except subprocess.CalledProcessError as err:
        print(err)
        raise err

    if not os.path.isfile(ffv1_path):
        log_data.append("WARNING: RAWcooked encoding failed. Moving to failures folder.")
        if not os.path.isfile(ffv1_path):
            log_data.append("WARNING: Cannot find file, moving to failures folder")
            utils.move_to_failures(ffv1_path)
        utils.move_to_failures(fullpath[0])
        utils.move_log_to_dest(log_path, 'failures')
        arguments = (
            ['status', 'RAWcook failed'],
            ['encoding_complete', str(datetime.datetime.today())[:19]]
        )

        return {
            "sequence": seq,
            "success": False,
            "path": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    log_data.append("RAWcooked encoding completed. Ready for validation checks")
    checksum_data = utils.md5_hash(ffv1_path)
    log_data.append(f"Checksum: {checksum_data}")
    arguments = (
        ['status', 'RAWcook completed'],
        ['encoding_complete', str(datetime.datetime.today())[:19]],
        ['derivative_path', ffv1_path],
        ['derivative_size', utils.get_folder_size(ffv1_path)],
        ['derivative_md5', checksum_data]
    )
    log_data.append(f"RAWcook completed successfully. Updating database:\n{arguments}")

    return {
        "sequence": seq,
        "success": True,
        "path": ffv1_path,
        "db_arguments": arguments,
        "logs": log_data
    }


# Default asset without prefix for backward compatibility
transcode_ffv1 = build_transcode_ffv1_asset()
