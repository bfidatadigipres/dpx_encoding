'''
dagster_rawcooked/assess.py
Links together all python code
for RAWcooked assessment calling
in external module from dpx_assess
as needed. Move for splitting.

NOTE: Need splitting path so assess.py
doesn't pick up for repeat assessment

Joanna White
2024
'''

# Imports
import os
import shutil
from dagster import asset, DynamicOutput
from .dpx_assess import get_partwhole, count_folder_depth, get_metadata, get_mediaconch, get_folder_size
from .sqlite_funcs import create_first_entry, update_table
from .dpx_seq_gap_check import gaps
from .dpx_splitting import launch_split
from .config import QNAP_FILM, ASSESS, DPX_COOK, DPOLICY, DPX_REVIEW, PART_RAWCOOK, PART_TAR, TAR_WRAP, DATABASE


@asset
def get_assess_folders():
    '''
    Retrieve list of DPX subfolders
    extract items partially processed
    '''
    dpx_folder = os.path.join(QNAP_FILM, ASSESS)
    print(dpx_folder)
    dpx_folders = [x for x in os.listdir(dpx_folder) if os.path.isdir(os.path.join(dpx_folder, x))]

    return dpx_folders


@asset
def dynamic_process_assess_folders(get_assess_folders):
    ''' Push get_dpx_folder list to multiple assets'''
    if len(get_asset_folders) == 0:
        return False
    for dpx_path in get_assess_folders:
        dpath = os.path.join(QNAP_FILM, DPX_COOK, dpx_path)
        yield DynamicOutput(dpath, mapping_key=dpx_path)


@asset
def assessment(context, dynamic_process_assess_folders):
    ''' Calling dpx_assess modules run assessment '''
    dpx_path = dynamic_process_assess_folders
    if not dpx_path:
        return {"status": "no folders to process", "dpx_seq": dpx_path}
    dpx_seq = os.path.split(dpx_path)[1]
    context.log.info(f"Processing DPX sequence: {dpx_path}")

    part, whole = get_partwhole(dpx_seq)
    if not part:
        row_id = update_table('status', dpx_seq, f'Fail! DPX sequence named incorrectly.', DATABASE)
        if not row_id:
            context.log.warning("Failed to update status with 'DPX sequence named incorrectly'")
            return {"status": "database failure", "dpx_seq": dpx_path}
        return {"status": "partWhole failure", "dpx_seq": dpx_path}        
    context.log.info(f"* Reel number {str(part).zfill(2)} of {str(whole).zfill(2)}")

    folder_depth, first_dpx = count_folder_depth(dpx_path)
    if folder_depth is None:
        # Incorrect formatting of folders
        row_id = update_table('status', dpx_seq, f'Fail! Folders formatted incorrectly.', DATABASE)
        if not row_id:
            context.log.warning("Failed to update status with 'Folders formatted incorrectly'")
            return {"status": "database failure", "dpx_seq": dpx_path}
        return {"status": "folder failure", "dpx_seq": dpx_path}
    context.log.info(f"Folder depth is {folder_depth}")

    gap_check, missing, first_dpx = gaps(dpx_path)
    if gap_check is True:
        context.log.info(f"Gaps found in sequence,moving to dpx_review folder: {missing}")
        review_path = os.path.join(QNAP_FILM, DPX_REVIEW, dpx_seq)
        shutil.move(dpx_path, review_path)
        row_id = update_table('status', dpx_seq, f'Fail! Gaps found in sequence: {missing}.', DATABASE)
        if not row_id:
            context.log.warning(f"Failed to update status with 'Gaps found in sequence'\n{missing}")
        return {"status": "gap failure", "dpx_seq": dpx_path}

    size = get_folder_size(dpx_path)
    cspace = get_metadata('Video', 'ColorSpace', first_dpx)
    bdepth = get_metadata('Video', 'BitDepth', first_dpx)
    width = get_metadata('Video', 'Width', first_dpx)
    if not cspace:
        cspace = get_metadata('Image', 'ColorSpace', first_dpx)
    if not bdepth:
        bdepth = get_metadata('Image', 'BitDepth', first_dpx)
    if not width:
        width = get_metadata('Image', 'Width', first_dpx)

    tar = fourk = luma = False
    context.log.info(f"* Size: {size} | Colourspace {cspace} | Bit-depth {bdepth} | Pixel width {width}")
    policy_pass, response = get_mediaconch(dpx_path, DPOLICY)

    if not policy_pass:
        tar = True
        context.log.info(f"DPX sequence {dpx_seq} failed DPX policy:\n{response}")
    if int(width) > 3999:
        fourk = True
        context.log.info(f"DPX sequence {dpx_seq} is 4K")
    if 'Y' == cspace:
        luma = True

    if tar is True:
        row_id = create_first_entry(dpx_seq, cspace, size, bdepth, 'Ready for split assessment', 'tar', dpx_path, DATABASE)
        if not row_id:
            context.log.warning(f"Failed to update status new reord data")
            return {"status": "database failure", "dpx_seq": dpx_path}
        return {"status": "tar", "dpx_seq": dpx_path, "size": size, "encoding": 'tar', "part": part, "whole": whole}
    if luma is True and fourk is True:
        row_id = create_first_entry(dpx_seq, cspace, size, bdepth, 'Ready for split assessment', 'rawcook', dpx_path, DATABASE)
        if not row_id:
            context.log.warning(f"Failed to update status new reord data")
            return {"status": "database failure", "dpx_seq": dpx_path}
        return {"status": "rawcook", "dpx_seq": dpx_path, "size": size, "encoding": 'luma_4k', "part": part, "whole": whole}
    else:
        row_id = create_first_entry(dpx_seq, cspace, size, bdepth, 'Ready for split assessment', 'rawcook', dpx_path, DATABASE)
        if not row_id:
            context.log.warning(f"Failed to update status new reord data")
            return {"status": "database failure", "dpx_seq": dpx_path}
        return {"status": "rawcook", "dpx_seq": dpx_path, "size": size, "encoding": 'rawcook', "part": part, "whole": whole}    


@asset
def move_for_split_or_encoding(context, assessment):
    '''
    Move to splitting or to encoding
    by updating sqlite data and trigger
    dpx_splitting.py func launch_split()
    '''
    if 'no folders to process' in assessment['status']:
        pass
    elif 'failure' not in assessment['status']:
        if assessment['size'] > 1395864370:
            reels = launch_split(assessment['dpx_seq'], assessment['kb_size'], assessment['encoding'])
            if 'failure' in reels['status']:
                raise Exception("Reels were not split correctly. Exiting")
            if assessment['status'] == 'rawcook':
                for reel in reels['paths']:
                    context.log.info(f"Moving reel {reel} to {PART_RAWCOOK}")
                    shutil.move(reel, os.path.join(QNAP_FILM, PART_RAWCOOK))
            elif assessment['status'] == 'tar':
                for reel in reels:
                    context.log.info(f"Moving reel {reel} to {PART_TAR}")
                    shutil.move(reel, os.path.join(QNAP_FILM, PART_TAR))

        if int(assessment['whole']) == 1:
            context.log.info(f"Moving single reel under 1TB to encoding path")
            if assessment['status'] == 'rawcook':
                shutil.move(assessment['dpx_seq'], os.path.join(QNAP_FILM, DPX_COOK))         
            elif assessment['status'] == 'tar':
                shutil.move(assessment['dpx_seq'], os.path.join(QNAP_FILM, TAR_WRAP))
        else:
            if assessment['status'] == 'rawcook':
                shutil.move(reel, os.path.join(QNAP_FILM, PART_RAWCOOK))
            elif assessment['status'] == 'tar':
                shutil.move(reel, os.path.join(QNAP_FILM, PART_TAR))
