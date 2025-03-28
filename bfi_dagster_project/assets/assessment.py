import os
import datetime
import dagster as dg
from typing import List, Dict, Any, Optional
from . import utils


def build_assess_sequence_asset(key_prefix: Optional[str] = None):
    '''
    Factory function that returns the asset with optional key prefix.
    '''
    
    # Build the asset key with optional prefix
    asset_key = [f"{key_prefix}", "assess_sequence"]
    # Handle the input dependency with prefixing
    ins_dict = {}
    if key_prefix:
        # If prefixed, reference the prefixed input asset
        ins_dict["folder_list"] = dg.AssetIn([key_prefix, "target_sequences"])
    else:
        # Default case without prefix
        ins_dict["folder_list"] = dg.AssetIn("target_sequences")

    @dg.asset(
        key=asset_key,
        ins=ins_dict,
        required_resource_keys={'database', 'process_pool'}
    )
    def assess_sequence(
        context: dg.AssetExecutionContext,
        folder_list: List[str],
    ) -> Dict[str, List[str]]:
        '''
        Receives list of sequences and runs series of assessments against the
        image sequences to decide on encoding path. Updates database with results
        and passes RAWcook and TAR list to encoding assets.
        '''
        # Get folder_name from AssetIn
        if folder_list == []:
            context.log.info("Run input is empty. Closing.")
            return {'ffmpeg': [], 'tar': [], 'invalid': []}
        context.log.info("Folder list received: %s", folder_list)

        # Change assessment status
        arg = (
            ['status', 'Assessment started'],
        )
        for folder in folder_list:
            seq = os.path.basename(folder)
            context.log.info("Updating assessment started:\n%s", arg)
            entry = context.resources.database.append_to_database(context, seq, arg)
            context.log.info("Updated database status: Assessment started %s.", entry)

        results = context.resources.process_pool.map(run_assessment, folder_list)
        print(f"Pool map returned results: {results}")

        assess_sequences = {
            "RAWcook": [],
            "TAR": [],
            "invalid": []
        }
        for image_dict in results:
            if not image_dict['encoding_choice']:
                assess_sequences['invalid'].append(image_dict['sequence'])
            else:
                assess_sequences[image_dict['encoding_choice']].append(image_dict['sequence'])

        # Update log data
        context.log.info(f"Results: RAWcook={len(assess_sequences['RAWcook'])}, "
                        f"TAR={len(assess_sequences['TAR'])}, "
                        f"Invalid={len(assess_sequences['invalid'])}")

        for item in results:
            seq_id = os.path.basename(item['sequence'])
            args = item['db_arguments']
            entry = context.resources.database.append_to_database(context, seq_id, args)
            context.log.info("Updated database status: Assessment started %s.", entry)
            for log in item['logs']:
                if 'WARNING' in str(log):
                    context.log.warning(log)
                else:
                    context.log.info(log)

        return assess_sequences
    return assess_sequence


def run_assessment(image_sequence: str) -> Dict[str, Any]:
    ''' Run assessment function '''
    log_data = []
    arguments = []

    seq = os.path.basename(image_sequence)
    log_data.append(f"Processing image sequence: {seq}")
    success = utils.check_fname(seq)
    if success is False:
        arguments = (
                ['status', 'Assessment failed'],
                ['error_message', 'File name formatted incorrectly']
            )
        log_data.append(f"Writing to database:\n{arguments}")
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    part, whole = utils.get_partwhole(seq)
    log_data.append(f"Reel number {str(part).zfill(2)} of {str(whole).zfill(2)}")
    if not part:
        arguments = (
                ['status', 'Assessment failed'],
                ['error_message', 'Part whole extraction failure']
            )
        log_data.append(f"Writing to database:\n {arguments}")
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    _, ftype, repro_ref = utils.get_file_type(seq)
    if len(repro_ref) > 0:
        if seq in str(repro_ref):
            log_data.append(f"WARNING: Digital file with same sequence name exists in DPI: {repro_ref}")
            arguments = (
                ['status', 'Assessment failed'],
                ['error_message', f'Digital file ingested for this CID item already: {repro_ref}']
            )
            return {
                "sequence": image_sequence,
                "success": False,
                "encoding_choice": None,
                "db_arguments": arguments,
                "logs": log_data
            }

    if ftype.lower() not in ['tif', 'tiff', 'dpx', 'dcp', 'dcdm', 'wav', 'tar']:
        log_data.append(f"File type incorrect for sequence: {seq}")
        arguments = (
            ['status', 'Assessment failed'],
            ['error_message', f'File type incorrect for sequence: {seq}']
        )
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    folder_depth, file_path = utils.count_folder_depth(image_sequence)
    log_data.append(f"Folder depth is {folder_depth} folder to images")
    if folder_depth is None:
        arguments = (
                ['status', 'Assessment failed'],
                ['error_message', 'Folder path depth error']
            )
        log_data.append("Folder path is non-standard depth. Assessment failed.")
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    if int(folder_depth) < 3 or int(folder_depth) > 4:
        arguments = (
                ['status', 'Assessment failed'],
                ['error_message', f'Folder path depth error: {folder_depth}']
        )
        log_data.append("Folder path is non-standard depth. Assessment failed.")
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    first_image, last_image, missing = utils.gaps(image_sequence)
    log_data.append(f"Image data - first {first_image} - last {last_image} - missing: {len(missing)}")
    if len(missing) > 0:
        log_data.append(f"Gaps found in sequence: {missing}")
        arguments = (
                ['status', 'Assessment failed'],
                ['error_message', f'Gaps found in sequence: {missing}']
            )
        log_data.append(f"Folder has gaps in sequence. {missing}")
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    folder_size = utils.get_folder_size(image_sequence)
    cspace = utils.get_metadata('Video', 'ColorSpace', first_image)
    log_data.append(f"Colourspace: {cspace}")
    if not cspace:
        cspace = utils.get_metadata('Image', 'ColorSpace', first_image)
    bdepth = utils.get_metadata('Video', 'BitDepth', first_image)
    log_data.append(f"Bit depth: {bdepth}")
    if not bdepth:
        bdepth = utils.get_metadata('Image', 'BitDepth', first_image)
    width = utils.get_metadata('Video', 'Width', first_image)
    log_data.append(f"DPX width: {width}")
    if not width:
        width = utils.get_metadata('Image', 'Width', first_image)
    height = utils.get_metadata('Video', 'Height', first_image)
    log_data.append(f"DPX width: {width}")
    if not width:
        height = utils.get_metadata('Image', 'Height', first_image)
    if not folder_size or not cspace or not bdepth or not width:
        log_data.append("Folder metadata could not be extracted. Assessment failed.")
        arguments = (
                ['status', 'Assessment failed'],
                ['error_message', 'Failed with metadata error']
            )
        log_data.append("Missing metadata from DPX sequence.")
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    if first_image.lower().endswith(('.tif', '.tiff')):
        arg = 'TIF'
    elif first_image.lower().endswith('.dpx'):
        arg = 'DPX'
    else:
        arg = 'TAR'
    policy_pass, response = utils.mediaconch(first_image, arg)

    if policy_pass == 'Fail':
        encoding_choice = 'TAR'
        log_data.append(f"DPX sequence {seq} failed DPX policy:\n {response}")
    else:
        encoding_choice = 'RAWcook'
        log_data.append(f"DPX sequence passed DPX policy: {seq}")

    # Write tree directory to folder
    pth = utils.write_dir_tree(image_sequence)
    if not pth:
        log_data.append(f"Write of directory tree to folder failed: {seq}")
    else:
        log_data.append(f"Directory tree written into sequence path: {pth}")

    # Use file_path to create metadata in folder
    pth = utils.metadata_dump(image_sequence, file_path, '')
    if pth:
        log_data.append(f"Metadata written into sequence path: {pth}")
    else:
        log_data.append(f"WARNING: Metadata not written into sequence path: {seq}")

    # Use file_path to create metadata for CID media record
    if arg == 'TAR':
        pth1, pth2 = utils.metadata_dump(image_sequence, file_path, 'tar')
    else:
        pth1, pth2 = utils.metadata_dump(image_sequence, file_path, 'mkv')
    if pth1:
        log_data.append(f"Metadata written to Admin/Logs/cid_mediainfo path: {pth1} / {pth2}")
    else:
        log_data.append(f"WARNING: Metadata not written into Admin/Logs/cid_mediainfo path: {seq}")

    status = "Assessment successful"
    arguments = (
        ['status', status],
        ['gaps_in_sequence', 'No'],
        ['assessment_pass', 'Yes'],
        ['assessment_complete', str(datetime.datetime.today())[:19]],
        ['colourspace', cspace],
        ['image_width', width],
        ['image_height', height],
        ['bitdepth', bdepth],
        ['seq_size', folder_size],
        ['encoding_choice', encoding_choice],
        ['first_image', os.path.basename(first_image)],
        ['last_image', os.path.basename(last_image)]
    )
    log_data.append(f"Database arguments: {arguments}")
    return {
        "sequence": image_sequence,
        "success": True,
        "encoding_choice": encoding_choice,
        "db_arguments": arguments,
        "logs": log_data
    }


# Default asset without prefix for backward compatibility
assess_sequence = build_assess_sequence_asset()
