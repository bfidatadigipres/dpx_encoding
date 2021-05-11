## DPX Automation preservation encoding scripts

The BFI National Archive recently developed workflows using open source software RAWcooked to convert DPX film scans into FFv1 Matroska video files for preservation. This has involved working with Media Area’s Jérôme Martinez, developer of RAWcooked, to help test and refine features. This repository contains the RAWcooked encoding (and TAR preservation scripts) used for these DPX automation workflows.  The aim of these scripts is to turn large DPX image sequences into RAWcooked FFV1 Matroska files for preservation within the BFI's Digital Preservation Infrastructure (DPI). Encoding DPX sequences to FFV1 can reduce the overall file size by half, and allow the DPX image sequence to be played in VLC or similar software for instant review.

These scripts are available under the MIT licence. They have been recently redeveloped and as such have a few untested features within the code, which will be updated as testing continues in coming weeks. If you wish to test these yourself please create a safe environment to use this code separate from preservation critical files. All comments and feedback welcome.


### Overview
These bash shell scripts are not designed to be run from the command line, but via cron scheduling. As a result there is no built in help command, so please refer to this README and the script comments for information about script functionality.

These handle the complete encoding process from start to finsih, including assessment of the DPX sequences suitability for RAWcooked encoding, encoding, failure assessment of the Matroska, and clean up of completed processes with deletion of DPX sequences. If a DPX sequence does not meet the basic DPX Mediaconch policy requirements for RAWcooked encoding then the sequence is failed and passed to a TAR wrap preservation path.

RAWcooked encoding functions with two scripts, the first encodes all items found in the RAWcooked encoding path and the second assesses the results of these encoding attempts. If an encoding fails, dpx_post_rawcook.sh will assess the error type moving failed files to a seprate folder, and create a new list which allows the RAWcooked first encoding script to try again with a different encoding formula, using --check-padding. If it fails again an error is issued to an current errors log, flagging the folder in need of human intervention.

The TAR script wraps the files, verifies the wrap using 7zip and then generates an MD5 sum of the whole file. Both encoding scripts move successful encodings to the BFI's Digital Preservation Infrastructure (DPI) ingest path, and associated DPX sequence folders into a dpx_completed/ folder.  Here the final script assesses the DPX sequences in dpx_completed/ folder by checking the DPI ingest logs for evidence of successful MKV/TAR ingest before deleting the DPX sequence.


### Dependencies

These scripts are run from Ubuntu 20.04LTS installed server and rely upon various Linux command line programmes. These include:  
flock, md5sum, tree, grep, cat, echo, ls, head, rm, touch, basename, dirname, find, du, rev, cut, mv, cp, date, sort and uniq.  
You can find out more about these by running the manual (man md5sum) or by calling the help page (md5sum --help).  

Several open source softwares are used from Media Area. Please follow the links below to find out more:  
RAWcooked (with dependency upon FFmpeg version 4+) - https://mediaarea.net/rawcooked  
MediaConch - https://mediaarea.net/mediaconch  
MediaInfo - https://mediaarea.net/mediainfo  

To run the concurrent processes the scripts use GNU Parallel which will require installation (with dependencies of it's own thay may include the following):  
- GNU parallel may also require: sysstat 12.2.0, libsensors 5-6.0, libsensors-config 3.6.0  
- available here http://archive.ubuntu.com/ubuntu/pool/main/l/lm-sensors/libsensors-config_3.6.0-2ubuntu1_all.deb  
- available here http://archive.ubuntu.com/ubuntu/pool/main/l/lm-sensors/libsensors5_3.6.0-2ubuntu1_amd64.deb  
- available here http://archive.ubuntu.com/ubuntu/pool/main/s/sysstat/sysstat_12.2.0-2_amd64.deb  
- available here http://archive.ubuntu.com/ubuntu/pool/universe/p/parallel/parallel_20161222-1.1_all.deb  

The TAR wrapping script uses p7zip-full programme available for download (Ubuntu 18.04+) using:  
`sudo apt install p7zip-full`


## Environmental variable storage
These scripts are being operated on each server under a specific user, who has environmental variables storing path for these operations. These environmental variables are persistent so can be called indefinitely. When being called from crontab it's critical that the crontab user is set to the correct user with associated environmental variables.


## Operational environment
These scripts operate within a defined folder structure, listed here with example files.

automation_dpx  
├── current_errors  
├── encoding  
│   ├── dpx_completed  
│   │   ├── N_3623230_04of04  
│   │   ├── N_3623278_01of02  
│   ├── dpx_to_assess  
│   │   ├── N_3623284_03of03  
│   │   └── N_489875_2_08of08  
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
These RAWcooked and TAR scripts are to be driven from a server /etc/crontab.  
They use Linux Flock locks which are reside in /var/run and are included in the crontab FLOCK LOCK DETAILS. These flock locks are checked for, and if absent, recreated every hour using the flock_rebuild.sh script.

The scripts for encoding and automation_dpx/ activities will run frequently throughout the day:  
dpx_assessment.sh - Twice a day at 12:35 am and 12:35pm  
dpx_rawcooked.sh - Runs continually, with crontab attempts made (but blocked by Flock when active) every 15 minutes to ensure continual encoding activity  
dpx_post_rawcook.sh - Runs three times a day every 8 hours, at 8:15am, 4:15pm, and 12:15am  
dpx_tar_script.sh - Runs once a day at 5pm  
dpx_clean_up.sh - Runs once a day at 4am  

DPX Encoding script crontab entries:  
`35    */12  *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_assess.lock         /mnt/path/dpx_encoding/dpx_assessment.sh`  
`*/15  *     *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_rawcook.lock        /mnt/path/dpx_encoding/dpx_rawcook.sh`  
`15    */8   *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_post_rawcook.lock   /mnt/path/dpx_encoding/dpx_post_rawcook.sh`  
`0     17    *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_tar_script.lock     /mnt/path/dpx_encoding/dpx_tar_script.sh`  
`0     4     *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_clean_up.lock       /mnt/path/dpx_encoding/dpx_clean_up.sh`  
 
`*/55  *     *    *    *       username      /mnt/path/dpx_encoding/flock_rebuild.sh`  


### global.log
Global.log is created by autoingest scripts to map processing of files as they are ingested into DPI. When completed the final message reads "successfully deleted file". This message is necessary to clean up of the DPX sequences, and so global.log must be accessed daily by dpx_clean_up.sh. The global.log is copied every day at 3AM to the automation_dpx/script_logs folder, just before dpx_clean_up.sh accesses it.


----------------------------------  

## SCRIPTS

### dpx_assessment.sh
This script assesses a DPX sequence's suitability to be RAWcooked encoded, based on criteria met within the metadata of the first DPX file. The metadata is checked against a Mediaconch policy, if it fails the folder is passed to the tar_preservation/ folder path.
This script need the DPX sequences to be formatted identically:  N_123456_01of01/scan01/2048x1556/<dpx_files>

Script functions:
- Refreshes the DPX success and failure lists so clean for each run
- Looks within the dpx_to_assess/ folder for DPX sequence directories at pixel ratio folder level. Eg, 2048x1556/ (found at mindepth 3 / maxdepth3)
- Takes the fifth DPX within this folder and stores in a 'dpx' variable, creates 'filename' and 'scan' variables using the basename and dirname of path
- Greps for the 'filename' within script_logs/rawcooked_dpx_success.log and script_logs/tar_dpx_failures.log. If appears in either then the file is skipped. If not:
  - Compares 'dpx' to Mediaconch policy rawcooked_dpx_policy.xml (policy specifically written to pass/fail RAWcooked encodings)
  - If pass writes 'filename' to dpx_success_list.txt
  - If fail writes 'filename' to dpx_failures_list.txt and outputs reason for failure to script_logs/dpx_assessment.log
- Moves dpx_failures_list.txt entries to tar_preservation/dpx_to_wrap/ folder
- Moves dpx_success_list enties to rawcooked/dpx_to_cook/ folder
- Appends the pass and failures from this pass to rawcooked_dpx_success.log and tar_dpx_failures.log


### dpx_rawcook.sh
This script runs two passes of the DPX sequences in dpx_to_cook/, first pass running --check-padding command against check_padding_list, second with --check command. It is run from /etc/crontab every 15 minutes which is protected by Flock lock to ensure the script cannot run more than one instance at a time.

Script functions:
- Refreshes the temporary_rawcooked_list.txt and temp_queued_list.txt  
  
PASS ONE:  
- Feeds list of DPX sequences from check_padding_list.txt into loop and compares against rawcooked_success.log and temp_queued_list.txt
  - If 'folders' not on either lists the folder name is written to temporary_retry_list.txt
- Takes the temporary_retry_list.txt and performs filter of all names by last 5 digits (part whole) passing 01* first
- Trims first twenty from this filtered list and passes to retry_list.txt and outputs this list to the log, with details about encoding using --check-padding
- Passes retry_list.txt DPX sequences to GNU parallel to start RAWcooked encoding multiple jobs at a time
  Script generates log file of encoding data, used in dpx_post_rawcook.sh  
  
PASS TWO:
- Feeds list of DPX sequences in dpx_to_cook/ 'folders' into loop and compares against rawcooked_success.log and temp_queued_list.txt
  - If 'folders' not on either lists the folder name is written to temporary_rawcook_list.txt
- Takes the temporary_rawcook_list.txt and performs filter of all names by last 5 digits (part whole) passing 01* first
- Trims first twenty from this filtered list and passes to rawcook_list.txt and outputs this list to the log, allowing for analysis later
- Passes these DPX sequence names to GNU parallel to start RAWcooked encoding multiple jobs at a time
  Script generates log file of encoding data, used in dpx_post_rawcook.sh


### dpx_post_rawcook.sh
A script assesses Matroska files, and logs, before deciding if a file can be moved to autoingest or dumped to failures folder. Moves successful DPX sequences to dpx_completed/ folder ready for clean up scripts.

Script functions:  
- Refresh all temporary lists generated at each while loop section

MKV file size check:
- Obtains total size of encoded Matroska. If it's larger that 1TB (1048560MB) moves to killed/ folder and appends current_errors files with failure
- Outputs loud warning to logs that this file needs all part wholes removing for splitting scripts to enact file size reductions.
- If undersize, skips but reports filename is under 1TB to log.

Mediaconch check:
- Looks for Matroska files in mkv_cooked/ path not modified in the last ten minutes, checks each one against the RAWcooked mkv Mediaconch Policy
  - If pass: script continues and pass information passed to post_rawcook.log
  - If fail: script writes filename to temp_mediaconch_policy_fails.txt, and writes failure to post_rawcook.log
- Matroska items listed on temp_mediaconch_policy_fails.txt are moved to Killed/ folder and logs are prepended fail_ and move to logs fodler

Grep logs for pass statements:
- Script looks through all logs for 'Reversablity was checked, no issue detected' where found:
  - Outputs successful cooked filenames to rawcooked_success.log
  - Prints list to post_rawcook.log
  - Moves Matroska files using GNU parallel to autoingest path
  - Moves successfully cooked DPX sequences to dpx_completed/ path
  - Moves log file to logs/ path

Grep logs for error of warning statements:
- Searches through remaining log files for instances that uses words like 'Error', 'Conversion failed!', 'Warning', 'WARNING' etc.
- Checks if any of these erroring files have already been added to check_padding_list.
   If no, the file is added to list, DPX sequence left in place, logs appended 'retry_' and the file will be reencoded using --check-padding
   If yes, outputs list of erroring files to log and to current_errors logs
   - Deletes Matroska files that have same name as the log files
   - Moves failed logs to logs folder prepended 'fail_'

Search for log files that have not been modified in over 24 hours (1440 minutes):
- For all mkv.txt files older than 1440 minutes since last modified:
  - Generates list and outputs to post_rawcook.log
  - Deletes logs files
  - While loop checks if Matroska exists too, if yes deletes.
- Sorts temp_rawcooked_succes.log and updates rawcook_success.log with these new additions.
- Deletes all temporary lists generated for each while loop.


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
- Find all directories in DPX_PATH to a max/min depth of one (ie, the N_123456_01of01 folder) and sort them into ascending order using the part whole, 01of*, 02of*, 03of* etc. Output to new temp_dpx_list.txt overwriting earlier version.
- Refresh the files_for_deletion_list.txt

Check if a directory has same object number to those listed in part_whole_search.log
- If yes, leave file in place and skip to next directory
- If no, proceed with following stages

Cat temp_dpx_list.txt while loop:
- Grep in global_copy.log for instances of the filenames found in DPX_PATH with the string 'Successfully deleted file' from THIS_MONTH and LAST_MONTH only
- If present: The files is added to the files_for_deletion_list.txt
- If not found: There is a second grep which looks for filename and string 'Skip object' THIS_MONTH. If found returns a message of 'Still being ingested' to log, but item not added to deletion list / If not found returns a message of 'NOT PASSED INTO AUTOINGEST!' and no filenames are written to deletion list. In all these cases the files are left in place for a comparison at a later date.  The date range THIS_MONTH/LAST_MONTH has been applied to handle instances where a re-encoded file is replacing an older version in Imagen after a fault is found with the original file. To avoid the logs giving a false flag of 'deleted' for a given filename, a maximum two month date range is given, on the assumption the clean up scripts will complete the work within this time.

Grep files_for_deletion_list.txt for all filenames and stores to file_list variable.
If loop checks if file_list variable is True (ie, has filenames in list):
- Moves all DPX directory filenames in list from DPX_PATH to FOR_DELETION folder
- From within the FOR_DELETION folder, all DPX sequences are deleted. 
Else it just outputs to log 'no items for deletion at this time'.

