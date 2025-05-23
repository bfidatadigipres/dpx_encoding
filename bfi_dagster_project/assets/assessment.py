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
        log_prefix = f"[{key_prefix}] " if key_prefix else ""
        # Get folder_name from AssetIn
        if folder_list == []:
            context.log.info(f"{log_prefix}Run input is empty. Closing.")
            return {'RAWcook': [], 'TAR': [], 'invalid': []}
        context.log.info(f"{log_prefix}Folder list received: {folder_list}")

        # Change assessment status
        arg = (
            ['status', 'Assessment started'],
        )
        for folder in folder_list:
            if folder.startswith('GAPS_'):
                fd = folder.split('_', 1)[-1]
                seq = os.path.basename(fd)
            else:           
                seq = os.path.basename(folder)
            context.log.info(f"{log_prefix}Updating assessment started: {arg}")
            entry = context.resources.database.append_to_database(context, seq, arg)
            context.log.info(f"{log_prefix}Updated database status: Assessment started {entry}")

        context.log.info(f"{log_prefix}Launching run assessment {seq}, mediaconch checks and metadata generation...")
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
            context.log.info(f"{log_prefix}Updated database status: Assessment started {entry}")
            for log in item['logs']:
                if 'WARNING' in str(log):
                    context.log.warning(f"{log_prefix}{log}")
                else:
                    context.log.info(f"{log_prefix}{log}")

        return assess_sequences
    return assess_sequence


def run_assessment(image_sequence: str) -> Dict[str, Any]:
    ''' Run assessment function '''
    log_data = []
    arguments = []

    # Recursively set permissions
    accept_gaps = False
    if image_sequence.startswith('GAPS_'):
        accept_gaps = True
        image_sequence = image_sequence.split('_', 1)[-1]

    seq = os.path.basename(image_sequence)
    try:
        utils.recursive_chmod(image_sequence, 0o777)
    except PermissionError as err:
        print(err)

    log_data.append(f"Processing image sequence: {seq}")
    success = utils.check_fname(seq)
    if success is False:
        arguments = (
                ['status', 'Assessment failed'],
                ['folder_path', image_sequence],
                ['error_message', 'Folder name formatted incorrectly']
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
                ['folder_path', image_sequence],
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

    _priref, ftype, rec = utils.get_file_type(seq)
    if _priref is None or ftype is None:
        log_data.append(f"WARNING: Unable to match sequence to CID Item record {seq}")
        arguments = (
            ['status', 'Assessment failed'],
            ['folder_path', image_sequence],
            ['error_message', f'Could not match sequence to CID item record: {seq}']
        )
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }
    elif seq in str(rec):
        log_data.append(f"WARNING: Digital file with same sequence name exists in DPI: {rec}")
        arguments = (
            ['status', 'Assessment failed'],
            ['folder_path', image_sequence],
            ['error_message', f'Digital file exists in DPI with same name: {seq}.']
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
            ['folder_path', image_sequence],
            ['error_message', f'File type incorrect for sequence: {seq}']
        )
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }
    
    first_image, last_image, missing = utils.gaps(image_sequence)
    if not first_image or not last_image:
        log_data.append(f"WARNING: No DPX or TIFF files found for sequence.")
        arguments = (
                ['status', 'Assessment failed'],
                ['folder_path', image_sequence],
                ['error_message', 'No DPX or TIFF files found in sequence.']
            )
        log_data.append("No first or last DPX/TIFF found. Assessment failed.")
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    if accept_gaps is False:
        log_data.append(f"Image data - first {first_image} - last {last_image} - missing: {len(missing)}")
        if len(missing) > 0:
            log_data.append(f"Gaps found in sequence: {missing}")
            arguments = (
                    ['status', 'Assessment failed'],
                    ['folder_path', image_sequence],
                    ['gaps_in_sequence', 'Yes'],
                    ['error_message', f'{len(missing)} gaps found in sequence']
                )
            log_data.append(f"Folder has gaps in sequence. {missing}")
            return {
                "sequence": image_sequence,
                "success": False,
                "encoding_choice": None,
                "db_arguments": arguments,
                "logs": log_data
            }
    else:
        log_data.append(f"GAPS ACCEPTED for long-term preservation:\n{missing}")

    if first_image.endswith(('.dpx', '.DPX')):
        # Only assess DPX folder depths
        folder_depth = utils.count_folder_depth(image_sequence)
        log_data.append(f"Folder depth is {folder_depth} folder to images")
        if folder_depth is None:
            arguments = (
                    ['status', 'Assessment failed'],
                    ['folder_path', image_sequence],
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
                    ['folder_path', image_sequence],
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
    else:
        log_data.append("Sequence is not DPX and will not have folder structure assessed.")

    framerate = utils.get_fps(first_image)
    if framerate is None:
        pass
    elif framerate < 12:
        log_data.append(f"WARNING: Frame rate is too low: {framerate}")
        arguments = (
                ['status', 'Assessment failed'],
                ['folder_path', image_sequence],
                ['error_message', f'Sequence FPS is too low: {framerate}']
            )
        log_data.append("Frame rate is low. Assessment failed.")
        return {
            "sequence": image_sequence,
            "success": False,
            "encoding_choice": None,
            "db_arguments": arguments,
            "logs": log_data
        }

    folder_size = utils.get_folder_size(image_sequence)
    cspace = utils.get_metadata('pix_fmt', first_image)
    log_data.append(f"Image colourspace: {cspace}")

    bdepth = utils.get_metadata('bits_per_raw_sample', first_image)
    log_data.append(f"Image bit depth: {bdepth}")

    width = utils.get_metadata('width', first_image)
    log_data.append(f"Image width: {width}")

    height = utils.get_metadata('height', first_image)
    log_data.append(f"Image width: {height}")

    if first_image.lower().endswith(('.tif', '.tiff')):
        arg = 'TIF'
    elif first_image.lower().endswith('.dpx'):
        arg = 'DPX'
    else:
        arg = 'TAR'

    if arg == 'DPX':
        if not folder_size or not cspace or not bdepth or not width:
            log_data.append("Folder metadata could not be extracted. Assessment failed.")
            arguments = (
                    ['status', 'Assessment failed'],
                    ['folder_path', image_sequence],
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

    # Use first_image to create metadata in folder
    pth = utils.metadata_dump(image_sequence, first_image, '')
    if pth:
        log_data.append(f"Metadata written into sequence path: {pth}")
    else:
        log_data.append(f"WARNING: Metadata not written into sequence path: {seq}")

    # Use first_image to create metadata for CID media record
    if arg == 'TAR':
        pth1, pth2 = utils.metadata_dump(image_sequence, first_image, 'tar')
    else:
        pth1, pth2 = utils.metadata_dump(image_sequence, first_image, 'mkv')
    if pth1:
        log_data.append(f"Metadata written to Admin/Logs/cid_mediainfo path: {pth1} / {pth2}")
    else:
        log_data.append(f"WARNING: Metadata not written into Admin/Logs/cid_mediainfo path: {seq}")

    status = f"Assessment success, starting {encoding_choice} encoding"
    arguments = (
        ['status', status],
        ['folder_path', image_sequence],
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
