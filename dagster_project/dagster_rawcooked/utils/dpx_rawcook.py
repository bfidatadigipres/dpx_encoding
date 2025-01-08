import os
import subprocess


def _check_for_log(dpx_seq, logs):
    ''' Check if outout version2 needed '''

    warnings = [
        'Error: undecodable file is becoming too big',
        'Error: the reversibility file is becoming big'
    ]

    outputv2 = False
    log_name = f"fail_{dpx_seq}.mkv.txt"

    if log_name in os.listdir(logs):
        with open(os.path.join(logs, log_name), 'r') as log_file:
            for line in log_file:
                if warnings[0] in str(line) or warnings[1] in str(line):
                    outputv2 = True

    return outputv2


def encoder(dpx_path, mkv_cooked, log_path):
    '''
    Build RAWcooked command
    '''
    dpx_seq = os.path.basename(dpx_path)
    output_v2 = _check_for_log(dpx_seq, log_path)
    if output_v2:
        cmd = [
            'rawcooked', '-y',
            '--all', '--no-accept-gaps',
            '--output-version', '2', '-s',
            '5281680', '-o', f'{mkv_cooked}{dpx_seq}.mkv',
            '&>>', f'{mkv_cooked}{dpx_seq}.mkv.txt'
        ]
    else:
        cmd = [
            'rawcooked', '-y',
            '--all', '--no-accept-gaps',
            '-s', '5281680', '-o',
            f'{mkv_cooked}{dpx_seq}.mkv',
            '&>>', f'{mkv_cooked}{dpx_seq}.mkv.txt'
        ]

    exit_code = subprocess.call(cmd, shell=True)
    if not exit_code == 0:
        print("Exit code is not 0, encoding failed")
        return None
    return f'{mkv_cooked}{dpx_seq}.mkv'
    
