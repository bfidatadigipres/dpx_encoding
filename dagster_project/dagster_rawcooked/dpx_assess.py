'''
Modules for assess.py
'''
import os
import subprocess


def get_partwhole(folder):
    ''' Extract part wholes as int '''
    pw = folder.split('_')[-1]
    part, whole = pw.split('of')
    return int(part), int(whole)


def count_folder_depth(fpath):
    '''
    Work out the depth of folders to the DPX sequence
    and ensure folders follow file naming conventions
    - This should only fail if more than one R01o01 present,
      other folders present that shouldn't be or order wrong.
    '''
    folder_contents = []
    for root, dirs, _ in os.walk(fpath):
        for directory in dirs:
            folder_contents.append(os.path.join(root, directory))

    if len(folder_contents) < 2:
        return None
    if len(folder_contents) == 2:
        if 'scan' in folder_contents[0].split('/')[-1].lower() and 'x' in folder_contents[1].split('/')[-1].lower():
            sorted(folder_contents, key=len)
            return '3'
    if len(folder_contents) == 3:
        if 'x' in folder_contents[0].split('/')[-1].lower() and 'scan' in folder_contents[1].split('/')[-1].lower() and 'R' in folder_contents[2].split('/')[-1].upper():
            sorted(folder_contents, key=len)
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

    return None


def get_metadata(stream, arg, dpath):
    ''' Retrieve metadata with subprocess '''

    cmd = [
        'mediainfo', '--Full',
        '--Language=raw',
        f'--Output={stream};%{arg}%',
        dpath
    ]
    
    meta = subprocess.check_output(cmd)
    return meta.decode('utf-8')


def get_mediaconch(dpath, policy):
    ''' Check for pass! {path} in mediaconch reponse '''

    cmd = [
        'mediaconch', '--force',
        '-p', policy,
        dpath
    ]
    
    meta = subprocess.check_output(cmd)
    meta.decode('utf-8')
    if meta.startswith(f'pass! {dpath}'):
        return True, meta
    
    return False, meta


def get_folder_size(dpath):
    ''' Use Linux du for consistancy '''

    cmd = [
        'du', '-s', '-b',
        dpath
    ]
    size = subprocess.check_output(cmd)
    size = size.decode('utf-8')
    return int(size)
