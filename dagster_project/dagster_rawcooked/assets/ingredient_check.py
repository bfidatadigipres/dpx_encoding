'''
dagster_rawcooked/assessment
Links together all python code
for RAWcooked assessment calling
in external modules from dpx_assess
as needed. Write data to dB.
'''

# Imports
import os
import re
import subprocess
from datetime import datetime
from dagster import asset, AssetIn, AssetExecutionContext


@asset(
    ins={
        "sequence_info": AssetIn("process_dpx_sequences"),
        "db": AssetIn("encoding_database")
    }
)
def assess_dpx_sequence(context: AssetExecutionContext, sequence_info, db):
    '''
    Assess a single DPX sequence. Can run concurrently for different sequences.
    '''
    print(context)
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
        db_dict = perform_assessment(context, path)

        if db_dict['encoding_pass'] is False:
            context.log.warning(f"Assessment failed!")
            # Update unsuccessful assessment
            cursor.execute(f"""
                UPDATE encoding_status 
                SET assessment_complete = 0,
                    current_stage = 'assessment',
                    status = 'assessment_failed',
                    error_message = {db_dict['error_message']}
                WHERE dpx_id = ?
            """, (dpx_id,))
            db.commit()

        else:
            # Update successful assessment
            context.log.info(f"Assessment passed!")
            cursor.execute(f"""
                UPDATE encoding_status 
                SET assessment_complete = 1,
                    current_stage = 'rawcooked_encoding_ready',
                    assessment_pass = 'True'
                    colorspace {db_dict['colorspace']},
                    dir_size {db_dict['dir_size']},
                    bitdepth {db_dict['bitdepth']},
                    encoding_choice {db_dict['encoding_choice']},
                    first_dpx {db_dict['first_dpx']},
                    last_dpx {db_dict['last_dpx']},
                    status = 'Assessment complete',
                    assessment_complete = {str(datetime.today())[:19]},
                WHERE dpx_id = ?
            """, (dpx_id,))
            db.commit()
        
    except Exception as e:
        cursor.execute("""
            UPDATE encoding_status 
            SET status = 'failed',
                error_message = ?
            WHERE dpx_id = ?
        """, (str(e), dpx_id))
        db.commit()
        raise


@op # type: ignore
def perform_assessment(context, dpx_path) -> dict:
    '''
    Calling dpx_assess modules run assessment
    Updating database with results
    '''

    db_dict = {}
    dpx_seq = os.path.basename(dpx_path)
    db_dict['dpx_id'] = dpx_seq
    db_dict['status'] = 'Assessment'

    context.log.info(f"Processing DPX sequence: {dpx_path}")

    part, whole = get_partwhole(dpx_seq)
    context.log.info(f"Reel number {str(part).zfill(2)} of {str(whole).zfill(2)}")
    if not part:
        db_dict['error_message'] = 'Part whole error'
        db_dict['assessment_pass'] = False
        context.log.warning("Part Whole extraction failure. Assessment failed.")
        return db_dict

    folder_depth, first_dpx = count_folder_depth(dpx_path)
    context.log.info(f"Folder depth is {folder_depth} folder to DPX")
    if folder_depth is None:
        # Incorrect formatting of folders
        db_dict['error_message'] = 'Folder depth error'
        db_dict['assessment_pass'] = False
        context.log.warning("Folder path is non-standard. Assessment failed.")
        return db_dict 

    first_dpx, last_dpx, missing = gaps(dpx_path)
    context.log.info(f"DPX data - first {first_dpx} - last {last_dpx} - missing: {len(missing)}")
    if len(missing) > 0:
        context.log.info(f"Gaps found in sequence,moving to dpx_review folder: {missing}")
        db_dict['error_message'] = 'Gaps found in sequence'
        db_dict['assessment_pass'] = False
        context.log.warning("DPX files contain gaps. Assessment failed.")
        return db_dict

    folder_size = get_folder_size(dpx_path)
    cspace = get_metadata('Video', 'ColorSpace', first_dpx)
    context.log.info(f"Colourspace: {cspace}")
    if not cspace:
        cspace = get_metadata('Image', 'ColorSpace', first_dpx)
    bdepth = get_metadata('Video', 'BitDepth', first_dpx)
    context.log.info(f"Bit depth: {bdepth}")
    if not bdepth:
        bdepth = get_metadata('Image', 'BitDepth', first_dpx)
    width = get_metadata('Video', 'Width', first_dpx)
    context.log.info(f"DPX width: {width}")
    if not width:
        width = get_metadata('Image', 'Width', first_dpx)
    if not folder_size or not cspace or not bdepth or not width:
        db_dict['error_message'] = 'Metadata of directory/dpx not accessible'
        db_dict['assessment_pass'] = False
        context.log.warning("Folder metadata could not be extracted. Assessment failed.")
        return db_dict

    context.log.info(f"* Size: {folder_size} | Colourspace {cspace} | Bit-depth {bdepth} | Pixel width {width}")
    policy_pass, response = get_mediaconch(dpx_path)

    if policy_pass is False:
        db_dict['encoding_choice'] = 'TAR'
        context.log.info(f"DPX sequence {dpx_seq} failed DPX policy:\n{response}")
    if policy_pass is True:
        db_dict['encoding_choice'] = 'RAWcook'
        context.log.info("DPX sequence {dpx_seq} passed DPX policy:")
    if int(width) > 3999:
        db_dict['resolution'] = '4K'
        context.log.info(f"DPX sequence {dpx_seq} is 4K")
    else:
        db_dict['resolution'] = '2K'
        context.log.info(f"DPX sequence {dpx_seq} is 2K")

    db_dict['colorspace'] = cspace
    db_dict['width'] = width
    db_dict['bitdepth'] = bdepth
    db_dict['dir_size'] = folder_size
    db_dict['first_dpx'] = first_dpx
    db_dict['last_dpx'] = last_dpx
    db_dict['assessment_pass'] = True
    return db_dict


@op # type: ignore
def get_partwhole(fname) -> tuple[int, int]:
    '''
    Check part whole well formed
    '''
    match = re.search(r'(?:_)(\d{2,4}of\d{2,4})(?:\.)', fname)
    if not match:
        print('* Part-whole has illegal charcters...')
        return None, None
    part, whole = [int(i) for i in match.group(1).split('of')]
    len_check = fname.split('_')
    len_check = len_check[-1].split('.')[0]
    str_part, str_whole = len_check.split('of')
    if len(str_part) != len(str_whole):
        return None, None
    if part > whole:
        print('* Part is larger than whole...')
        return None, None
    return (part, whole)


@op # type: ignore
def count_folder_depth(fpath) -> str:
    '''
    Check if folder is three depth of four depth
    across total scan folder contents and folders
    ordered correctly
    '''
    folder_contents = []
    for root, dirs, _ in os.walk(fpath):
        for directory in dirs:
            folder_contents.append(os.path.join(root, directory))

    if len(folder_contents) < 2:
        return False
    if len(folder_contents) == 2:
        if 'scan' in folder_contents[0].split('/')[-1].lower() and 'x' in folder_contents[1].split('/')[-1].lower():
            return '3'
    if len(folder_contents) == 3:
        if 'x' in folder_contents[0].split('/')[-1].lower() and 'scan' in folder_contents[1].split('/')[-1].lower() and 'R' in folder_contents[2].split('/')[-1].upper():
            return '4'
    if len(folder_contents) > 3:
        total_scans = []
        for num in range(0, len(folder_contents)):
            if 'scan' in folder_contents[num].split('/')[-1].lower():
                total_scans.append(folder_contents[num].split('/')[-1])

        scan_num = len(total_scans)
        if len(folder_contents) / scan_num == 2:
            # Ensure folder naming order is correct
            if 'scan' not in folder_contents[0].split('/')[-1].lower():
                return None
            sorted(folder_contents, key=len)
            return '3'
        if (len(folder_contents) - 1) / scan_num == 2:
            # Ensure folder naming order is correct
            if 'scan' in folder_contents[0].split('/')[-1].lower() and 'R' not in folder_contents[len(folder_contents) - 1].split('/')[-1].upper():
                return None
            sorted(folder_contents, key=len)
            return '4'


@op # type: ignore
def gaps(dpath) -> tuple[str, str, list]:
    '''
    Return True/False depending on if gaps
    are sensed in the DPX sequence
    '''

    missing_dpx = []
    file_nums, filenames = iterate_folders(dpath)
    # Calculate range from first/last
    file_range = [ x for x in range(min(file_nums), max(file_nums) + 1) ]

    # Retrieve equivalent DPX names for logs
    first_dpx = filenames[file_nums.index(min(file_nums))]
    last_dpx = filenames[file_nums.index(max(file_nums))]

    # Check for absent numbers in sequence
    missing = list(set(file_nums) ^ set(file_range))
    print(missing)
    if len(missing) > 0:
        for missed in missing:
            missing_dpx.append(missed)
 
    return (first_dpx, last_dpx, missing_dpx)


def iterate_folders(fpath) -> tuple[list, list]:
    '''
    Iterate suppied path and return list
    of filenames
    '''
    file_nums = []
    filenames = []
    for root, _,files in os.walk(fpath):
        for file in files:
            if file.endswith(('.dpx', '.DPX')):
                file_nums.append(int(re.search(r'\d+', file).group()))
                filenames.append(os.path.join(root, file))
    return (file_nums, filenames)


@op # type: ignore
def get_folder_size(fpath) -> int:
    '''
    Check the size of given folder path
    return size in kb
    '''
    if os.path.isfile(fpath):
        return os.path.getsize(fpath)

    try:
        byte_size = sum(
            os.path.getsize(os.path.join(fpath, f)) for f in os.listdir(fpath) \
            if os.path.isfile(os.path.join(fpath, f))
            )
        return byte_size
    except OSError as err:
        print(f"get_size(): Cannot reach folderpath for size check: {fpath}\n{err}")
        return 0


@op # type: ignore
def get_metadata(stream, arg, dpath) -> str:
    '''
    Retrieve metadata with subprocess
    for supplied stream/field arg
    '''

    cmd = [
        'mediainfo', '--Full',
        '--Language=raw',
        f'--Output={stream};%{arg}%',
        dpath
    ]

    meta = subprocess.check_output(cmd)
    return meta.decode('utf-8').rstrip('\n')


@asset
def get_mediaconch(dpath) -> tuple[bool, str]:
    '''
    Check for 'pass! {path}' in mediaconch reponse
    for supplied file path and policy
    '''
    policy = os.getenv("DPX_POLICY")

    cmd = [
        'mediaconch', '--force',
        '-p', policy,
        dpath
    ]

    meta = subprocess.check_output(cmd)
    meta = meta.decode('utf-8')
    if meta.startswith(f'pass! {dpath}'):
        return (True, meta)
    return (False, meta)
