import os
from typing import List
import dagster as dg
from .. import resources


@dg.asset(required_resource_keys={'database'})
def target_sequences(
    context: dg.AssetExecutionContext,
) -> List[str]:
    '''
    Look for new sequences in watch folder and update to database,
    and hand list of folderpaths to assessment asset.
    '''
    target_automation = context.resources.source_path
    target = os.path.join(target_automation, 'image_sequence_processing/')
    if not os.path.exists(target):
        context.log.info("Unable to access target_path: %s", target)
        return None
    seq_supply = os.path.join(target, "processing")

    context.resources.database.initialise_db(context)
    directories = [x for x in os.listdir(seq_supply) if os.path.isdir(os.path.join(seq_supply, x))]
    directories.sort()
    context.log.info("Directories located:\n%s", directories)

    current_files = []
    for dr in directories:
        dpath = os.path.join(seq_supply, dr)
        context.log.info("Directory path: %s", dpath)

        search = f"SELECT status FROM encoding_status WHERE seq_id=?"
        result = context.resources.database.retrieve_seq_id_row(context, search, 'fetchall', (dr,))
        context.log.info(result)

        # Review database entries
        if len(result) == 0:
            current_files.append(dpath)
            entry = context.resources.database.start_process(context, dr, dpath, 'Triggered assessment')
            context.log.info("New entry made in database: %s - %s", entry, dpath)
        elif len(result) > 0 and 'Triggered assessment' not in str(result):
            context.log.info("Skipping: Sequence already listed in process/processed: %s", result)
            continue
        elif len(result) > 0 and 'Triggered assessment' in str(result):
            context.log.info("Picking up sequence a second time. Passing for processing: %s", result)
            current_files.append(dpath)
        else:
            current_files.append(dpath)
            entry = context.resources.database.start_process(context, dr, dpath, 'Triggered assessment')
            context.log.info("New entry made in database: %s - %s", entry, dpath)

    context.log.info("Files being handed to assessment:\n%s", current_files)
    return current_files

