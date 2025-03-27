import os
import datetime
import dagster as dg
from typing import List
from . import utils


@dg.asset(required_resource_keys={'database'})
def reencode_failed_asset(
    context: dg.AssetExecutionContext,
    config: dg.Config,
) -> List[str]:
    '''
    Receive context op_config containting folder path for failed transcode
    attempt, retrieves database row data and begins re-encode attempt.
    List containing filepath is passed to validation asset.
    '''
    if not context.op_config.get('sequence'):
        return []

    fullpath = context.op_config.get('sequence')
    proc_path, seq = os.path.split(fullpath)
    context.log.info("Received new encoding data: %s", config)

    search = f"SELECT * FROM encoding_status WHERE seq_id=?"
    data = context.resources.database.retrieve_seq_id_row(context, search, 'fetchone', (seq,))
    context.log.info(f"Row retrieved: {data}")
    status = data[2]
    choice = data[15]
    context.log.info(fullpath, f"==== Retry RAWcook encoding: %s ====", fullpath)
    if status != "Pending retry":
        context.log.error("Sequence not suitable for retry. Exiting.")
        return []
    context.log.info("Status indicates selected for retry successful")
    if choice != "RAWcook":
        context.log.error("Sequence not suitable for RAWcooked re-encoding. Exiting.")
        return []
    context.log.info("Encoding choice is RAWcooked")
    if not os.path.exists(fullpath):
        context.log.error(f"Failed to find path {fullpath}. Exiting.")
        return []
    context.log.info("File path identified: %s", fullpath)

    ffv1_path = os.path.join(os.path.split(proc_path)[0], f"ffv1_transcoding/{seq}.mkv")
    if os.path.isfile(ffv1_path):
        context.log.info("Delete existing transcode attempt.")
        os.remove(ffv1_path)
    context.log.info("Path for Matroska: %s", ffv1_path)
    log_path = f"{ffv1_path}.txt")
    context.log.info("Outputting log file to %s", log_path)
    context.log.info("Calling Encoder function")
    output_path = utils.encoder(fullpath, ffv1_path, log_path)

    if output_path is None:
        context.log.warning("RAWcooked encoding failed. Moving to failures folder.")
        if not os.path.isfile(ffv1_path):
            context.log.warning("Cannot find file, moving to failures folder")
            utils.move_to_failures(ffv1_path)
        utils.move_to_failures(fullpath)
        utils.move_log_to_dest(log_path, 'failures')
        arguments = (
            ['status', 'RAWcook failed'],
            ['encoding_complete', str(datetime.datetime.today())[:19]]
        )
        context.log.info(f"RAWcooked encoding failed. Updating database:\n%s", arguments)
        entry = context.resources.database.append_to_database(context, seq, arguments)
        context.log.info(entry)
        return []
    context.log.info("RAWcooked encoding completed. Ready for validation checks")
    checksum_data = utils.get_checksum(ffv1_path)
    context.log.info("Checksum: %s", data[f"{seq}.mkv"])
    arguments = (
        ['status', 'RAWcook completed'],
        ['encoding_complete', str(datetime.datetime.today())[:19]],
        ['derivative_path', ffv1_path],
        ['derivative_size', utils.get_folder_size(ffv1_path)],
        ['derivative_md5', checksum_data[f"{seq}.mkv"]]
    )
    context.log.info("RAWcook completed successfully. Updating database:\n%s", arguments)
    entry = context.resources.database.append_to_database(context, seq, arguments)
    context.log.info(f"Row data written: {entry}")
    return [ffv1_path]


defs = dg.Definitions(assets=[reencode_failed_asset])
