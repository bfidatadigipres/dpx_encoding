'''
dagster_rawcooked/assessment.py
Links together all python code
for RAWcooked assessment calling
in external modules from dpx_assess
as needed. Write data to dB.

'''

# Imports
import os
import sys
import requests
from dagster import asset, AssetIn, DynamicPartitionsDefinition

sys.path.append(os.environ.get('utils'))
from dpx_assess import get_partwhole, count_folder_depth, get_metadata, get_mediaconch, get_folder_size
from sqlite_funcs import retrieve_fnames, create_first_entry, make_filename_entry, update_table
from dpx_seq_gap_check import gaps
from config import DPX_PATH, DPOLICY, DPX_REVIEW, PART_RAWCOOK, PART_TAR, TAR_WRAP, DATABASE
import adlib_v3


@asset(
    partitions_def=dpx_partitions,
    ins={
        "sequence_info": AssetIn("process_dpx_sequences"),
        "db": AssetIn("encoding_database")
    }
)
def assess_dpx_sequence(context, sequence_info, db):
    '''
    Assess a single DPX sequence. Can run concurrently for different sequences.
    '''
    dpx_id = sequence_info["dpx_id"]
    path = sequence_info["path"]
    
    cursor = db.cursor()
    try:
        # Update status to processing
        cursor.execute("""
            UPDATE encoding_status 
            SET status = 'processing',
                current_stage = 'assessment',
                processing_start = CURRENT_TIMESTAMP
            WHERE dpx_id = ?
        """, (dpx_id,))
        db.commit()
        
        # Perform assessment
        result = perform_assessment(path)
        
        # Update successful assessment
        cursor.execute("""
            UPDATE encoding_status 
            SET assessment_complete = 1,
                current_stage = 'assessment_complete',
                status = 'ready_for_encoding'
            WHERE dpx_id = ?
        """, (dpx_id,))
        db.commit()
        
        return {
            "dpx_id": dpx_id,
            "path": path,
            "metadata": result
        }
        
    except Exception as e:
        cursor.execute("""
            UPDATE encoding_status 
            SET status = 'failed',
                error_message = ?
            WHERE dpx_id = ?
        """, (str(e), dpx_id))
        db.commit()
        raise


"""
@asset(
    ins={
        "db": AssetIn("encoding_database")
    }
)
def process_assess_folder(context, db):
    '''
    Push get_dpx_folder list to multiple assets
    Validate DPX sequence metadata and check against internal API
    '''
    dpx_path = context.op_config["dpx_path"]
    
    # Check internal API for Item record - use adlib to check file_type is DPX
    '''
    api_response = requests.get(f"internal-api/items/{Path(dpx_path).name}")
    if not api_response.ok:
        raise Exception("No matching Item record found")
    '''
    # Run assessment script
    result = subprocess.run(["assessment_script.py", dpx_path], capture_output=True)
    if result.returncode != 0:
        raise Exception("Assessment failed")
        
    # Update database
    cursor = db.cursor()
    cursor.execute(
        "UPDATE encoding_status SET assessment_complete = ? WHERE fname = ?",
        (str(datetime.date.now()), Path(dpx_path).name)
    )
    db.commit()
    
    return {"dpx_path": dpx_path, "metadata": result.stdout}


    print(DPX_PATH)
    dpx_folders = [x for x in os.listdir(DPX_PATH) if os.path.isdir(os.path.join(DPX_PATH, x))]
    context.log.info(f"Sequences found in dpx_to_assess/ folder path:\n{dpx_folders}")

    if len(dpx_folders) == 0:
        return False
    for dpx_path in dpx_folders:
        # Trying to keep sequences in one path now, rethink this!
        dpath = os.path.join(QNAP_FILM, ASSESS, dpx_path)
        make_filename_entry(dpx_path, dpath, DATABASE)


@asset(deps=['process_assess_folder'])
def assessment(context):
    ''' Calling dpx_assess modules run assessment '''

    dpx_paths = retrieve_fnames(DATABASE)
    if not dpx_paths:
        return {"status": "no folders to process", "dpx_seq": dpx_paths}
    
    for dpx_path in dpx_paths:
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


@asset(deps=['assessment'])
def move_for_split_or_encoding(context):
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
"""