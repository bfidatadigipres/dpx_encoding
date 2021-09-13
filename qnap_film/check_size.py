#!/usr/bin/env LANG=en_UK.UTF-8 /usr/local/bin/python3

'''
Add DPX sizes to CSV where folder matches basename of filepath already in CSV
Allows for averaging of RAWcooked encodings across multiple MKV/DPX sequences

Joanna White 2021
'''

import os
import csv
import subprocess

# Global variables
STORE_PATH = os.environ['QNAP_FILM']
CSV_PATH = os.path.join(STORE_PATH, "size_list.csv")
NEW_CSV_PATH = os.path.join(STORE_PATH, "all_size_list.csv")
DPX_SOURCE = os.environ['FILM_OPS']


def read_csv(folder_num):
    '''
    Recover DPX file size and path data stored in CSV_PATH
    '''
    mkv_path = ''
    filename = ''
    with open(CSV_PATH, newline='') as fname:
        readme = csv.DictReader(fname, delimiter='\t')
        for row in readme:
            mkv_path = row['mvk_path']
            path_split = os.path.split(mkv_path)
            filename_split = os.path.splitext(path_split[1])
            filename = filename_split[0]
            if str(folder_num) == str(filename):
                print(f"Match found {folder_num} and {filename}")
                kb_size = row['kb_size']
                return (kb_size, mkv_path)
            else:
                continue


def get_size(folder_path):
    '''
    Call subprocess du -s and return in kb
    '''
    command = [
        "du",
        "-s",
        folder_path
    ]
    try:
        dpx_size = subprocess.check_output(command)
        print(dpx_size)
        dpx_size = str(dpx_size).strip("b'")
        dpx_size = dpx_size.split('\\t')
        kb_size = dpx_size[0]
        print(kb_size)
        return kb_size
    except Exception as e:
        print(f"Unable to raise size of {folder_path}: {e}")
        return None


def main():
    '''
    Iterate through DPX folders looking for match in read_csv
    Where found extract data from CSV_PATH, check size of DPX
    sequence and then write all data to NEW_CSV_PATH
    '''
    for directory in os.listdir(DPX_SOURCE):
        if directory.startswith('dpx_completed'):
            folder_path_all = os.path.join(DPX_SOURCE, directory)
            for dir in os.listdir(folder_path_all):
                folder_path = os.path.join(folder_path_all, dir)
                print(folder_path)
                if dir.startswith("N_"):
                    print(f"Checking for CSV entry with {dir} in path")
                    mkv_data = read_csv(dir)
                    print(mkv_data)
                    if mkv_data:
                        print("Directory {} found in CSV, extracting folder size".format(dir))
                        print(folder_path)
                        if os.path.exists(folder_path):
                            dpx_size = get_size(folder_path)
                            if dpx_size:
                                write_csv(mkv_data[0], mkv_data[1], dpx_size, folder_path)
                            else:
                                print(f"No dpx_size found for {folder_path}")
                                continue
                        else:
                            print(f"Folder path {folder_path} does not exist")
                            continue
                    else:
                        print(f"No MKV data extracted for this item {dir}")
                        continue


def write_csv(mkv_size, mkv_path, dpx_size, dpx_path):
    '''
    Collect up all variable from main() and append to CSV for use later
    '''
    with open(NEW_CSV_PATH, 'a', newline='') as csvfile:
        data = [mkv_size, mkv_path, dpx_size, dpx_path]
        print(data)
        datawriter = csv.writer(csvfile)
        datawriter.writerow(data)
        csvfile.close()


if __name__ == "__main__":
    main()