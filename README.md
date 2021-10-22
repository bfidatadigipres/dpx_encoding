## DPX preservation scripts

The BFI National Archive recently developed workflows using open source software RAWcooked to convert 2K and 4K DPX film scans into FFv1 Matroska video files for preservation. This has involved working with Media Area’s Jérôme Martinez, developer of RAWcooked, to help test and refine features. This repository contains the RAWcooked encoding (and TAR preservation scripts) used for these DPX automation workflows.  The aim of these scripts is to turn large DPX image sequences into RAWcooked FFV1 Matroska files for preservation within the BFI's Digital Preservation Infrastructure (DPI). Encoding DPX sequences to FFV1 can reduce the overall file size by half (2K RGB files), and allow the DPX image sequence to be played in VLC or similar software for instant review.

These scripts are available under the MIT licence. They have been recently redeveloped and may contain some untested features within the code. If you wish to test these yourself please create a safe environment to use this code separate from preservation critical files. All comments and feedback welcome.


## Overview

These bash shell scripts and Python scripts are not designed to be run from the command line, but via cron scheduling. As a result there is no built in help command, so please refer to this README and the script comments for information about script functionality.

These scripts handle the complete encoding process from start to finish, including assessment of the DPX sequences suitability for RAWcooked encoding, splitting of sequences too large for ingest into our DPI, encoding, failure assessment of the Matroska, and clean up of completed processes with deletion of DPX sequences. If a DPX sequence does not meet the basic DPX Mediaconch policy requirements for RAWcooked encoding then the sequence is failed and passed to a TAR wrap preservation path.

RAWcooked encoding functions with two scripts, the first encodes all items found in the RAWcooked encoding path and the second assesses the results of these encoding attempts. If an encoding fails, dpx_post_rawcook.sh will assess the error type moving failed files to a seprate folder, and create a new list which allows the RAWcooked first encoding script to try again with a different encoding formula, using '--output-version 2'. If it fails again an error is issued to an current errors log, flagging the folder in need of human intervention.

The TAR script wraps the files, verifies the wrap using 7zip and then generates an MD5 sum of the whole file. Both encoding scripts move successful encodings to the BFI's Digital Preservation Infrastructure (DPI) ingest path, and associated DPX sequence folders into a dpx_completed/ folder.  Here the final script assesses the DPX sequences in dpx_completed/ folder by checking the DPI ingest logs for evidence of successful MKV/TAR ingest before deleting the DPX sequence.


## Dependencies

These scripts are run from Ubuntu 20.04LTS installed server and rely upon various Linux command line programmes. These include: flock, md5sum, tree, grep, cat, echo, ls, head, rm, touch, basename, dirname, find, du, rev, cut, mv, cp, date, sort and uniq. You can find out more about these by running the manual (man md5sum) or by calling the help page (md5sum --help).  

Several open source softwares are used from Media Area. Please follow the links below to find out more:  
RAWcooked version 21.09 - https://mediaarea.net/rawcooked (dpx_rawcook.sh is not compatible with any earlier versions of RAWcooked)  
FFmpeg Version 4+ - https://ffmpeg.org/  
MediaConch V18.03.2+ - https://mediaarea.net/mediaconch  
MediaInfo V19.09 - https://mediaarea.net/mediainfo (dpx_assessment.sh is not currently compatible with later versions if 4K scans in use)  

To run the concurrent processes the scripts use GNU Parallel which will require installation (with dependencies of it's own that may include the following):  

    GNU parallel may also require: sysstat 12.2.0, libsensors 5-6.0, libsensors-config 3.6.0
    available here http://archive.ubuntu.com/ubuntu/pool/main/l/lm-sensors/libsensors-config_3.6.0-2ubuntu1_all.deb
    available here http://archive.ubuntu.com/ubuntu/pool/main/l/lm-sensors/libsensors5_3.6.0-2ubuntu1_amd64.deb
    available here http://archive.ubuntu.com/ubuntu/pool/main/s/sysstat/sysstat_12.2.0-2_amd64.deb
    available here http://archive.ubuntu.com/ubuntu/pool/universe/p/parallel/parallel_20161222-1.1_all.deb

The TAR wrapping script uses p7zip-full programme available for download (Ubuntu 18.04+) using:  
`sudo apt install p7zip-full`  


## Environmental variable storage  

These scripts are being operated on each server using environmental variables that store all path and key data for the script operations. These environmental variables are persistent so can be called indefinitely.  


## Operational environment

The scripts operate within a defined folder structure. These automation_dpx folders are deposited at various storage locations, and the dpx_encoding repository scripts are broken into folder path names to reflect this, eg ‘film_operations’, ‘qnap_film’. The automation_dpx folder contents is always formatted like this so the scripts work across locations:

automation_dpx  
├── current_errors  
├── encoding  
│   ├── dpx_completed  
│   │   ├── N_3623230_04of04  
│   │   ├── N_3623278_01of02  
│   ├── dpx_to_assess  
│   │   ├── N_3623284_03of03  
│   │   └── N_489875_2_08of08  
│   ├── part_whole_split  
│   │   ├── rawcook  
│   │   └── tar  
│   ├── rawcooked  
│   │   ├── dpx_to_cook  
│   │   │   └── N_473236_01of02  
│   │   ├── encoded  
│   │   │   ├── killed  
│   │   │   ├── logs  
│   │   │   └── mkv_cooked  
│   │   │       └── N_473236_01of02.mkv  
│   │   │       └── N_473236_01of02.mkv.txt  
│   │   └── failures  
│   ├── script_logs  
│   ├── tar_preservation  
│   │   ├── dpx_to_wrap  
│   │   │   └── N_489855_2_01of01  
│   │   ├── failures  
│   │   ├── logs  
│   │   └── tarred_files  
│   └── to_delete  
└── QC_files


## Supporting crontab actions

The RAWcooked and TAR scripts are to be driven from a server /etc/crontab.  
To prevent the scripts from running multiple versions at once and overburdening the server RAM the crontab calls the scripts via Linux Flock lock files (called from /usr/bin/flock shown below). These are manually created in the /var/run folder, and the script flock_rebuild.sh regularly checks for their presence, and if absent, recreates them every hour. It is common for the lock files to disappear when a server is rebooted, etc.

The scripts for encoding and automation_dpx/ activities will run frequently throughout the day:  
dpx_assessment.sh - Every eight hours on the half hour (launches dpx_splitting_script.py processing one job at a time).    
dpx_rawcooked.sh - Runs continually, with crontab attempts made (but blocked by Flock when active) every 15 minutes to ensure continual encoding activity  
dpx_post_rawcook.sh - Runs three times a day every 8 hours, at 8am, 4pm, and 12am  
dpx_tar_script.sh - Runs once a day at 10pm  
dpx_clean_up.sh - Runs once a day at 5am  

DPX Encoding script crontab entries:  

    `30    */8   *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_assess.lock         /mnt/path/dpx_encoding/film_operations/dpx_assessment.sh`
    `*/15  *     *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_rawcook.lock        /mnt/path/dpx_encoding/film_operations/dpx_rawcook.sh`
    `0    */8   *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_post_rawcook.lock   /mnt/path/dpx_encoding/film_operations/dpx_post_rawcook.sh`
    `0     22    *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_tar_script.lock     /mnt/path/dpx_encoding/film_operations/dpx_tar_script.sh`
    `0     5     *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_clean_up.lock       /mnt/path/dpx_encoding/film_operations/dpx_clean_up.sh`  
    `*/55  *     *    *    *       username      /mnt/path/dpx_encoding/flock_rebuild.sh`  
    
## global.log

Global.log is created by DPI ingest scripts to map processing of files as they are successfully ingested. When an ingest process completes the final message reads "successfully deleted file". This message is necessary for clean up of the DPX sequences, and so global.log must be accessed daily by dpx_clean_up.sh. The global.log is copied every day at 3AM to the automation_dpx/script_logs folder, just before dpx_clean_up.sh accesses it.


## THE SCRIPTS

### dpx_assessment.sh [Launches Python splitting script]

This script assesses a DPX sequence's suitability to be RAWcooked encoded, based on criteria met within the metadata of the fifth DPX file. The metadata is checked against a Mediaconch policy, if it fails, the folder is passed to the tar_preservation/ folder path.
This script need the DPX sequences to be formatted identically:  N_123456_01of01/scan01/2048x1556/<dpx_files>

Script functions:
- Refreshes the DPX success and failure lists, tar, rawcooked, luma and python lists so clean for each run of the script, avoiding path failures.
- Looks within the dpx_to_assess/ folder for DPX sequence directories at pixel ratio folder level. Eg, 2048x1556/ (found at mindepth 3 / maxdepth 3)
- Takes the fifth DPX within this folder and stores in a 'dpx' variable, creates 'filename' and 'scan' variables using the basename and dirname of path
- Greps for the 'filename' within script_logs/rawcooked_dpx_success.log and script_logs/tar_dpx_failures.log. If appears in either then the file is skipped. If not:
  - Compares 'dpx' to Mediaconch policy rawcooked_dpx_policy.xml (policy specifically written to pass/fail RAWcooked encodings)
  - If pass, looks for metadata indicating if the DPX are 4K, have RGB or Luma (Y) colourspace, before writing 'filename' to rawcooked_dpx_list.txt or luma_4k_dpx_list.txt
  - If fail writes 'filename' to tar_dpx_list.txt and outputs reason for failure to script_logs/dpx_assessment.log
- Each list created in previous stage is sorted by its part whole, and passed into a loop that calculates the total folder size in KB, then writes this data to new list python_list.txt
- The contents of python_list.txt are passed one at a time to the Python splitting script for size assessment and potential splitting. From here they are moved to their encoding paths if 01of01, or into the part_whole_split folder if multiple parts exist.
- Appends the luma, 4K, rawcooked and tar failure lists to rawcooked_dpx_success.log and tar_dpx_failures.log

Requires use of rawcooked_dpx_policy.xml.


### dpx_splitting_script2.py

This script is not listed in the crontab as it is launched at the end dpx_assessment.sh to arrange movement of the image sequence, and where necessary splits the folders into smaller folders to allow for RAWcooked / TAR wrapping of a finished file no larger that 1TB.

Script function:
- Receives three arguments within SYS.ARGV[1], splitting them into:
  - Total KB size of the DPX sequence
  - Path to DPX sequence
  - Encoding type - rawcooked, luma, 4k or tar
- Checks if the DPX sequence name is listed in splitting_document.csv first column 'original' name
  - If yes, renumbers the folder and updates the dpx_sequence/dpx_path variables
  - If no, skips onto next stage
- Divisions are calculated based upon the DPX sequence total KB size. The options returned include:
  - No division needed because the folder is under the minimum encoding size for Imagen. The DPX folders are moved to their encoding paths if the have the part whole 01of01. If not they are moved straight to part_whole_split folder to await their remaining parts.  
     Script exits, completed.
  - Oversized folder. The folder is too large to be divided by this script and needs human intervention (over 5.2TB). The folder is moved to current_errors/oversized_sequences/ folder and the error log is appended.
     Script exits, completed.
  - Divisions are required by either 2, 3, 4 or 5 splits depending on the size of the DPX sequence and whether the encoding is for RAWcooked RGB, RAWcooked luma, RAWcooked 4K or TAR wrapping (see table 1)
    Script continues to next step
- Splittings functions begin:
  - DPX_splitting.log is updated with details of the DPX sequence that required splitting
  - New folder names are generated for the split folder additions and old numbers have new folder part wholes calculated for them. The current DPX sequence folder has it’s number updated, and the dpx_sequence and dpx_path variables are updated.
  - All original folder names, new folder names, and today's date are updated respectively to splitting_document.csv
  - DPX sequence path is iterated over finding all folders within it that contain files ending in ‘.dpx’ or ‘.DPX’. This will find all instances of scan01, scan02 folders within the DPX path and splits/moves each equally.
  - New folders have new paths created, incorporating all sub folders down to the level of the DPX files.
  - All DPX files within the DPX path are counted per scan folder, divided as per division (2, 3, 4 or 5 - see table 1).
  - The new folder list and first DPX of each block of data is written to the DPX_splitting.log
  - Each block is moved to it’s corresponding new folder, one DPX at a time using Python’s shutil function.
  - Each block is checked that the files all moved correctly, where any are missing a second move attempt is made.  
- Human readable data of the splits added to DPI database field for retrieval, and added to each folder in a text file for long-term embedding in MKV container.  

Requires supporting documentation: splitting_document.csv, DPX_splitting.log

Table 1  
| RAWcooked RGB  | RAWcooked Luma  | RAWcooked 4K    | TAR wrapping    | Total divisions |  
| -------------- | --------------- | --------------- | --------------- | --------------- |  
| 1.3TB to 2.6TB | 1.0TB to 2.0TB  | 1.0TB to 2.0TB  | 1.0TB to 2.0TB  | 2 Divisions     |  
| 2.6TB to 3.9TB | 2.0TB to 3.0TB  | 2.0TB to 3.0TB  | 2.0TB to 3.0TB  | 3 Divisions     |  
| 3.9TB to 5.2TB | 3.0TB to 4.0TB  | 3.0TB to 4.0TB  | 3.0TB to 4.0TB  | 4 Divisions     |  
|                | 4.0TB to 5.0TB  | 4.0TB to 5.0TB  | 4.0TB to 5.0TB  | 5 Divisions     |  

NOTES: We've only recently started RAWcooked encoding Y (Luma) and 4K DPX sequences, and on average the first Matroska files have approximate size reductions of 27% (Luma Y) and 10% (RGB 4K). This has shown to be very variable depending on the DPX image sequence content, with the occassional file only 4-5% smaller than the DPX sequence. Because of this we're currently assuming that any of these files could have reduced size reductions and are therefore setting the divisions sizes the same as TAR wrapping which has no compression.


### dpx_part_whole_move.py

This Python3 script has been written to check for complete part whole sequences and where present to move the files together as a unit onto the encoding paths. The sequences are only moved into the part_whole_split folder when they are within their safe encoding size, and when they are part of a series of reels.

Script functions:
- Looks in part_whole_split/ subfolders rawcook/ and tar/ for sequences. Checks every sequence's folder name in the splitting_document.csv and if found in the 'original' name column then the folder name is updated to reflect changes to the situation of the whole reel collection (another part must have required splitting actions).
- Next it sorts through files looking for 01of01, where present (by human accident only) moves straight to transcode path determined by whether the file is found in the tar/ or rawcook/ subfolder.
- Looks for any remaining sequences with the part whole 01of* only, and expands the part whole in to the name range for the folder and stores as a Python list, for example:  ['N_123456_01of03', 'N_123456_02of03', 'N_123456_03of03']  
- Checks in both tar/ and rawcook/ folders for these folder names, if any are absent the then script exits with a message to log saying that there sequence parts missing. Where all sequences are found, then each found folder is moved onto it's respective encoding path.
- Update the script actions to the log.

Requires supporting documentation: splitting_document.csv  


### dpx_rawcook.sh

This script runs two passes of the DPX sequences in dpx_to_cook/, first pass running --all --no-accept-gaps --output-version 2 command against reversibility_list, second with just --all --no-accept-gaps command. It is run from /etc/crontab every 15 minutes which is protected by Flock lock to ensure the script cannot run more than one instance at a time.

Script functions:
- Refreshes the temporary_rawcooked_list.txt and temp_queued_list.txt  
  
PASS ONE:  
- Feeds list of DPX sequence folders from reversibility_list.txt into loop and compares against rawcooked_success.log and temp_queued_list.txt
  - If DPX sequence not on either lists the folder name is written to temporary_retry_list.txt
- Takes the temporary_retry_list.txt and performs filter of all names by part whole extension
- Sorts into a unique list and passes to retry_list.txt and outputs this list to the log, with details about encoding using --output-version 2
- Passes retry_list.txt DPX sequences to GNU parallel to start RAWcooked encoding multiple jobs at a time
  Script generates FFV1 Matroska file and log file of RAWcooked console outputted logging data, used in dpx_post_rawcook.sh

- Refreshes temp_queued_list.txt again

PASS TWO:
- Feeds list of DPX sequences in dpx_to_cook/ into loop and compares against rawcooked_success.log and temp_queued_list.txt
  - If a DPX sequence is not on either lists the folder name is written to temporary_rawcook_list.txt
- Takes the temporary_rawcook_list.txt and performs filter of all names by part whole extension
- Trims first twenty from this filtered list and passes to rawcook_list.txt and outputs this list to the log, allowing for analysis later
- Passes these DPX sequence names to GNU parallel to start RAWcooked encoding multiple jobs at a time
  Script generates FFV1 Matroska file and log file of RAWcooked console outputted logging data, used in dpx_post_rawcook.sh


### dpx_post_rawcook.sh

A script assesses Matroska files, and logs, before deciding if a file can be moved to autoingest or to failures folder. Moves successful DPX sequences to dpx_completed/ folder ready for clean up scripts.

Script functions:  
- Refresh all temporary lists .txt files generated in the scripts

MKV file size check:
- Obtains total size of encoded Matroska. If it's larger that 1TB (1048560MB) moves to killed/ folder and appends current_errors files with failure
- Outputs loud warning to logs that this file needs all part wholes removing for splitting scripts to enact file size reductions.
- If undersize, skips but reports filename is under 1TB to log.

Mediaconch check:
- Looks for Matroska files in mkv_cooked/ path not modified in the last ten minutes, checks each one against the basic RAWcooked mkv Mediaconch Policy
- If pass: script continues and information passed to post_rawcook.log
- If fail: script writes filename to temp_mediaconch_policy_fails.txt, and writes failure to post_rawcook.log. Matroska items listed on temp_mediaconch_policy_fails.txt are moved to Killed/ folder and logs are prepended fail_ and move to logs fodler

Grep logs for pass statement of Mediaconch passed files:
- Script looks through all logs for 'Reversablity was checked, no issue detected' where found:
  - Outputs successful cooked filenames to rawcooked_success.log
  - Prints list to post_rawcook.log
  - Moves Matroska files using GNU parallel to DPI ingest path
  - Moves successfully cooked DPX sequences to dpx_completed/ path
  - Moves log file to logs/ path

Grep remaining logs for signs of oversized reversibility files:
- Searches through log files for use of term 'Error: undecodable file is becoming too big.'
- Checks any found filenames against the reversibility_list.txt to see if they have already been logged
   If no, the filename is added to the reversibility list for first pass RAWcooked encoding (in dpx_rawcook.sh)
   If yes, a repeat encoding problem is logged in the dpx_encoding_errors.log
   - Deletes Matroska files that have same name as the log files
   - Moves failed logs to logs folder prepended 'fail_'

Greps remaining logs for any generic Error messages:
- Searches through log files for use of term 'Error:', 'Reversability was checked, issues detected, see below' etc.
   If not found then the script skips on
   If found, an unknown encoding error is registered
   - The filename is output to error_list.txt and the dpx_encoding_errors.log
   - Logs are prepended 'fail_' and moved to Logs/ folder
   - The associated Matroska is deleted where present

Search for log files that have not been modified in over 24 hours (1440 minutes):
- For all mkv.txt files older than 1440 minutes since last modified (ie stale encodings):
  - Generates list and outputs to post_rawcook.log
  - Deletes logs files
  - While loop checks if Matroska exists too, if yes deletes.
- Sorts temp_rawcooked_succes.log and updates rawcook_success.log with these new additions.
- Deletes all temporary lists generated for each while loop.

Requires use of rawcooked_mkv_policy.xml.


### dpx_tar_script.sh

This script handles TAR encoding of any DPX sequences that cannot be RAWcooked encoded due to licence limitations or unsupported features within the DPX. It uses 7zip to wrap the files in 'store' mode without compression, and runs a validation check against the file using 7zip's test feature. The script begins by refreshing four list files, allowing for new lists to be formed that will drive the movement or removal of files.

Script operations:

First find / while loop:
- Checks against current tar_check list (for most recent tar operations) to see if files found in DPX_PATH are already being processed
- If not in the current list being worked on:
  - Generates MD5 checksum manifest for the contents of each DPX sequence found and places in scan folder alongside DPX sequence folder
  - Adds the DPX sequence to the tar_list.txt for TAR processing
- If in the current list: Skips.
Takes grepped tar_list.txt contents and passes to GNU parallel which runs tar wrap jobs concurrently using 7Zip (7z a -mx=0) with no compressions and archive settings. The file it output to TAR_DEST path using filename.tar, and the console output is written to a filename.tar.log and place alongside the tar file.

Second find/while loop:
- Searches in TAR_DEST for filename.tar files. Checks if the file is greater than 1048560 MB (just under 1TB):
  - If larger than 1TB the file is moved to the failures folder, and log is appended "failed_oversize" and placed in logs folder, and the filename is output to current_errors log files.
  - If smaller than 1TB the filename is written to tar_list_complete.txt file for further processing.
Takes grepped tar_list_complete.txt and passes to GNU parallel for test verification checks using 7zip (7z t) and the console results are appended to the same filename log that the earlier data was written to.

Third find/while loop:
- Searches in TAR_DEST for filename.tar.log files, opens each using cat and greps (using -i to ignore case) for error or warning messages in both the wrapping and verification messages written to each log.
  - If error messages found filename is output to a failed_tar_list.txt, and current_errors encoding log is updated with failure
  - If no error messages found the filename is output to passed_tar_list.txt.
The passed_tar_list.txt is grepped and results are sent to GNU parallel which makes md5sum for each whole TAR file, outputs to the MD5_PATH Isilon folder.
The contents of passed_tar_list.txt is copied to tar_checks.txt for assessment at start of next script, in case of script movement failures.

Following clean up actions for all files and logs using grep passes to GNU parallel:
- Successfully wrapped tar files have MD5 checksum generating
- Short while loop cleans up this output md5 sum log, formatting for Python DPI ingest scripts
- Successfully wrapped tar files are moved from TAR_DEST to DPI ingest
- Successfully associated log files are moved to logs/ folder
- Successfully associated dpx_to_wrap DPX sequence folders are moved to DPX_WRAPPED (dpx_completed folder)
- Failed tar wrapped files are moved to failures/ folder
- Failed associated log files are moved to logs/ folder prepended "failed_filename.tar.log"
- Failed associated DPX sequences are left in place for a repeat attempt


### dpx_clean_up.sh

This script's function is to check completed encodings against a recent copy of DPI ingest's global.log.

Script functions:
- Find all directories in dpx_completed/ to a max/min depth of one (ie, the N_123456_01of01 folder) and sort them into ascending order using the part whole, 01of*, 02of*, 03of* etc. Output to new temp_dpx_list.txt overwriting earlier version.
- Refresh the files_for_deletion_list.txt

Check if a directory has same object number to those listed in part_whole_search.log
- If yes, leave file in place and skip to next directory
- If no, proceed with following stages

Cat temp_dpx_list.txt while loop:
- Grep in global_copy.log for instances of the filenames found in DPX_PATH with the string 'Successfully deleted file' from THIS_MONTH and LAST_MONTH only
- If present: The files is added to the files_for_deletion_list.txt
- If not found: There is a second grep which looks for filename and string 'Skip object' THIS_MONTH. If found returns a message of 'Still being ingested' to log, but item not added to deletion list / If not found returns a message of 'NOT PASSED INTO AUTOINGEST!' and no filenames are written to deletion list. In all these cases the files are left in place for a comparison at a later date.  The date range THIS_MONTH/LAST_MONTH has been applied to handle instances where a re-encoded file is replacing an older version in DPI after a fault is found with the original file. To avoid the logs giving a false flag of 'deleted' for a given filename, a maximum two month date range is given, on the assumption the clean up scripts will complete the work within this time frame.

Grep files_for_deletion_list.txt for all filenames and stores to file_list variable.
If loop checks if file_list variable is True (ie, has filenames in list):
- Moves all DPX directory filenames in list from DPX_PATH to FOR_DELETION folder
- From within the FOR_DELETION folder, all DPX sequences are deleted. 
Else it just outputs to log 'no items for deletion at this time'.


### flock_rebuild.sh

This short script is called by crontab each day to check that the Flock locks are still available in /var/run.
Loads with a list of flock lock paths. A for loop checks if each path in the list exists, if not it uses touch to recreate it.

