## BFI National Archive Image Sequence Processing

The BFI National Archive developed workflows using open source software RAWcooked to convert 2K and 4K DPX and TIFF film scans into FFV1 Matroska video files for preservation. This has involved working with Media Area’s Jérôme Martinez, developer of RAWcooked, to help test and refine features. This repository contains the RAWcooked encoding (and TAR preservation scripts) used for these automation workflows. The aim of these scripts is to turn large image sequences into RAWcooked FFV1 Matroska files for preservation within the BFI's Digital Preservation Infrastructure (DPI). Encoding DPX/TIFF sequences to FFV1 can reduce the overall file size by half (2K RGB files), and allow the DPX image sequence to be played in VLC or similar software for instant review.  
  
These scripts are available under the MIT licence. Though originally a mix of Bash and Python code, they have recently evolved to run within a Dagster orchestrator which provides a Webserver interface providing realtime feedback of processes to our Developer team.  If you wish to test these yourself please create a safe environment to use this code separate from preservation critical files. All comments and feedback welcome.  
   

## Overview of bfi_dagster_project scripts:

These Python scripts contained with bfi_dagster_project are designed to be run from within a Dagster orchestrator (dagster.io) within a Python virtual environment (venv) with Python requirements installed.  Please refer to this README and the script comments for information about script functionality.  
  
These scripts handle the complete encoding process from start to finish, including assessment of the image sequences suitability for RAWcooked encoding, encoding, failure assessment of the Matroska, and clean up of completed processes with deletion of image sequences. If an image sequence does not meet the basic DPX or TIFF Mediaconch policies (included in this repository) it therefor fails requirements for RAWcooked encoding. The sequence is failed and passed to a TAR wrap preservation path. These scripts also manage the demuxing of RAWcooked FFV1 Matroska files back to an image sequence, and the unpacking to Python tarfile wrapped TAR sequences.  
  
RAWcooked encoding is adaptable depending on requests fed into the code from a Flask web application broadcasting within the BFI DPI network. This allows colleagues to switch on/off the --no-accept-gaps feature of RAWcooked, to force the ‘-framerate’ flag for 16 or 24 where image sequence frame rates are inaccurate. The code also senses if a previous encoding pass has run and failed due to information stored in the padding data, triggering a RAWcooked ‘--output-version 2’ pass. The Flask web application also provides an overview of each sequences settings for the first image sequence, and reports at various stages of the transcoding allowing colleagues an overview of their sequence progress, whether RAWcooked encoded or TAR wrapped.  
  
To launch the Dagster project in the active venv navigate into dpx_encoding repository and call:   
```dagster dev -w workspace.yaml -h 0.0.0.0 -p 8000```  
   
For an overview of how Dagster projects can be configured we recommend the Dagster Documentation, and their Dagster University.    
  

## Additional code (detailed notes below):
   
```tar_wrapping_launch.sh``` / ```tar_wrapping_checksum.py```   
```unwrap_mkv_rawcook.sh```   
```unwrap_tar_checksum.py```   

## Dependencies

Linux system programmes for the shell launched scripts.

Python standard library imports requirements.txt

These scripts are run from Ubuntu 24.04LTS installed server and rely upon several open source softwares Media Area and FFmpeg.  
Please follow the links below to find out more:  
RAWcooked version 24.01 - https://mediaarea.net/rawcooked  
(--output-version2 is not compatible with < RAWcooked V21.01)  
FFmpeg Version 7 - https://ffmpeg.org/  
MediaConch V24 - https://mediaarea.net/mediaconch  
MediaInfo V24 - https://mediaarea.net/mediainfo  

## Environmental variable storage  

These scripts are being operated on each server using environmental variables that store all path and key data for the script operations. These environmental variables are persistent so can be called indefinitely. They are imported to Python scripts near the beginning using ```os.environ.get('VARIABLE')```, and are called in shell scripts like ```"${VARIABLE}"```. They are saved into the /etc/environment file.
See list at bottom for variable links to folder paths.


## Operational environment

The scripts operate within a defined folder structure. These automation_dpx folders are deposited at various storage locations, and the dpx_encoding repository scripts are broken into folder path names to reflect this, eg ‘film_operations’, ‘qnap_film’ etc. The automation_dpx folder contents is always formatted like this so the scripts work across locations:

```bash
automation  
├── image_sequence_processing  
│   ├── processing   
│   │   ├── N_3623230_04of04   
│   │   └── N_3623278_01of02  
│   ├── ffv1_transcoding  
│   │   ├── N_3623278_01of02.mkv.txt  
│   │   └── N_3623278_01of02.mkv  
│   ├── tar_wrapping  
│   │   ├── N_3623284_03of03.tar  
│   │   └── N_3623284_03of03_tar.log  
│   ├── failures  
│   │   ├── N_294553_01of03  
│   │   └── N_294553_01of03.mkv  
│   └── logs  
│          ├── transcode_logs  
│          ├── tar_logs  
│          ├── check_logs  
│          └── failures  
├── tar_preservation  
│   ├── for_tar_wrap  
│   │   ├── N_3623230_01of04  
│   │   └── N_3623278_02of02  
│   ├── failures  
│   ├── checksum_manifests  
│   └── tar_wrapping_checksum.log  
├── unwrap_rawcook_mkv  
│   ├── N_123589_01of04.mkv  
│   └── completed  
└── unwrap_tar  
       ├── completed  
       ├── failures  
       └── unwrapped_tar_checksum.log  
```

## Supporting crontab actions

The RAWcooked and TAR scripts are to be driven from a server /etc/crontab.  
To prevent the scripts from running multiple versions at once and overburdening the server RAM the crontab calls the scripts via Linux Flock lock files (called from /usr/bin/flock shown below). These are manually created in the /var/run folder, and the script flock_rebuild.sh regularly checks for their presence, and if absent, recreates them every hour. It is common for the lock files to disappear when a server is rebooted, etc.

The scripts for encoding and automation_dpx/ activities will run frequently throughout the day:     

DPX Encoding script crontab entries:  
    0     22    *    *    *    user   /usr/bin/flock -w 0 --verbose /var/run/dpx_tar_script.lock  ${DPX_SCRIPTS}tar_wrapping_launch.sh ${DG1_QNAP03}
    0     22    *    *    *    user   /usr/bin/flock -w 0 --verbose /var/run/unwrap_mkv_rawcook.lock  ${DPX_SCRIPTS}unwrap_rawcook.sh ${DG10_QNAP11}  
    0     22    *    *    *    user   /usr/bin/flock -w 0 --verbose /var/run/unwrap_tar.lock  ${PYENV3}  ${DPX_SCRIPTS}unwrap_tar_checksum.py ${DG6_FILM_LAB}
    */5   *     *    *    *    user   /mnt/path/dpx_encoding/flock_rebuild.sh  
    
 
### BFI server and storage configuration  

The FFV1 codec was developed with SliceCRCs which compliment the multithreading feature of FFmpeg, enabling fast encoding by splitting encoding processes across the slices of each frame. Our server has 64 CPUs allowing better multithreading capability. The SliceCRCs also embed 64 CRC checksum across each frame, so if your file suffers any damage over time you can exactly pinpoint where in the sequence it is by running the MKV through FFmpeg.  

BFI RAWcooked server (retrieved using Linux ```lscpu```):  
Architecture X86-64  
CPU op-mode 64-bit  
CPU(s) 64  
Model Intel(R) Xeon(R) Gold 5218 CPU @2.30GHz  
RAM 252GB  
Threads (per core) 2  
Cores (per socket) 16  
Sockets 2  
The 64 CPUs are calculated Threads x Cores x Sockets.  

In addition to plenty of CPUs it is good to have storage commuication speeds that match CPU speeds. The BFI have networked access storage connected at either 10Gb or 25Gb fibre optic. To understand this more we recommend reviewing GitHub issue 375 which discusses this more thorougly: https://github.com/MediaArea/RAWcooked/issues/375  


## NON-DAGSTER SCRIPTS

### dpx_unwrap_rawcook.sh

A shell script which manages the demuxing of FFV1 Matroska files using the latest version of RAWcooked installed to the server.

Script functions:
1. Compiles a list of all files found in the unwrap_rawcook_mkv/ folder, ignoring any that have been modified in the last 10 minutes.
2. If files are found the logs are written to with a list of files to be targeted.
3. The current server RAWcooked version used for demuxing the DPX sequence is written to the logs.
4. A check is made that a folder doesn't exist for this file already, such as a file that has been recently unwrapped - named RAWcooked_unwrap_N_123456_01of01/.
5. If no folder found the file name is written to a 'confirmed_unwrap_list.txt'.
6. Each item on the 'confirmed_unwrap_list.txt' is passed to GNU parallel where one at a time, RAWcooked is used to unwrap the FFV1 MKV to DPX, and placed in the 'RAWcooked_unwrap...' folder.
7. A log is created for each demux, and this is checked for the message 'Reversibility was checkd, no issue detected.'
   - If this message is found the file FFV1 MKV is moved to the completed/ folder.
   - If this message is not found the problem is written to the logs, but the MKV and folder is left in place for manual review.

### tar_wrapping_launch.sh / tar_wrapping_checksum.py

#### tar_wrapping_launch.sh

The shell launch script regularly checks for files/folders in the tar_preservation/for_tar_wrap folder, ensuring the files have not been modified in the last 10 minutes (aren't still copying to location). It passes each item found one job at a time to GNU Parallel which launches the following Python script.

#### tar_wrapping_checksum.py

The python script receives the supplied path from the launch script and generates local checksum manifests, TAR wraps the file, then creates a TAR manifest and checks that they match.

Script functions:
1. Receives the fullpath as a sys.argv[1]. Checks if path exists and if filename is formatted correctly with part whole. Then looks to retrieve a CID item record reference number by using the filename as the search query in the CID database.
2. Checks if the supplied path is a file or directory. Creates an MD5 checksum if a single file and outputs to MD5 manifest. If a folder, iterates through all contents of folder generating MD5 checksums for all items and adds to an MD5 checksum manifest.
3. Loads the MD5 manifest as a dictionary, and prints them all to the local log but not including any image sequence files such as DPX, TIF or JPEG files.
4. Commences TAR wrapping of the file/folder using the Python tarfile module. Then generates a new TAR MD5 manifest of all TAR contents, and compares the TAR manifest with the original manifest.
   - If manifests do not match, the TAR file is moved to failures folder, and warning output to logs. The source file/folder is left in place for a repeat TAR wrap attempt. Script exits.
   - If manifests match, the TAR manifest is added into the TAR archive, and a whole file MD5 checksum is created of the TAR file and added to the local logs.
5. The size is taken and checked to be under 1TB.
   - If over 1TB the file is moved to the oversize folder and errors written to the logs.
   - If under 1TB the TAR file is moved to DPI ingest paths, the source file/folder is moved to 'to_delete' folder and deleted. The MD5 manifest is moved to the checksum_manifests/ folder for storage.
6. A note is written to the CID item record for the file, noting that the file was TAR wrapped using Python tarfile and that a checksum manifest has been added to the TAR file.

### unwrap_tar_checksum.py

This python script manages the unwrapping of TAR files downloaded from DPI, and is able to unwrap Python tarfile TARs where 7zip and other software fail.

Script functions:
1. This script checks in the 'unwrap_tar' folder for any files ending '.tar'.
2. Where a file is found it launches the Linux TAR software using Python subprocess to unwrap the file, placing the items into a folder alongside the TAR file named the same as the TAR file, minus the '.tar' extension.
3. The subprocess error messages is checked for a zero exit code that equals 0 (completed successfully).
   - If fails to return exit code 0, the script attempt to unwrap the TAR file using Python tarfile module.
   - If succeeds, the script looks for the presence of TAR checksum manifest.
4. Where checksum manifest found, this is loaded into the scripts memory as dictionary.
5. The script creates a new local unpacked TAR file checksum manifest.
6. The two manifest are compared to ensure that no data has been lost/corrupted in the TAR file retrieved from DPI.
   - If matched the result is written to the logs.
   - If not matched, or the TAR manifest is not present, then the logs are updated that the MD5 checks cannot be completed.
7. TAR files are moved to completed/ folder for manual deletion and untarred items are left in place in their folder.

### flock_rebuild.sh

This short script is called by crontab each day to check that the Flock locks are still available in /var/run.
Loads with a list of flock lock paths. A for loop checks if each path in the list exists, if not it uses touch to recreate it.


### Environmental variable mapping

#### Paths to different storage devices used at launch of Dagster projec:

DG1_QNAP03  
DG2_FILM_OPS  
DG3_FILM_PRES  
DG4_FILM_SCAN  
DG5_FILM_QC  
DG6_FILM_LAB  
DG7_FILM_MICRL  
DG8_DIGIOPS  
DG9_QNAP10  
DG10_QNAP11  
DG11_QNAP06  
DG12_EDIT_DIR
  
#### automation folder variables:  
Dagster home allows logs to persist across launches of Dagster software:  
DAGSTER_HOME=“/home/user/code/git/dagster_home/” 
  
MySQL database called in Dagster to update Flask app for colleagues and pass information to the RAWcooked encoding scripts:  
DATABASE=“dpx_encoding/encoding_database.db”  
  
Environmental vars needed for contab script launches:  
DPX_SCRIPTS="/home/user/code/git/dpx_encoding/"  
PYENV3="/home/user/code/ENV3/bin/python3" 
  
Addition script variables:  
POLICY_RAWCOOK="dpx_encoding/rawcooked_mkv_policy.xml"  
POLICY_DPX=“dpx_encoding/rawcooked_dpx_policy.xml"  
POLICY_TIFF=“dpx_encoding/rawcooked_tiff_policy.xml”  
UNWRAP_RAWCOOK="automation/unwrap_rawcook_mkv/"
TAR_PRES="automation/tar_preservation/"  
DPX_WRAP="automation/tar_preservation/for_tar_wrap/"  
  
