## DPX preservation scripts

The BFI National Archive recently developed workflows using open source software RAWcooked to convert 2K and 4K DPX film scans into FFV1 Matroska video files for preservation. This has involved working with Media Area’s Jérôme Martinez, developer of RAWcooked, to help test and refine features. This repository contains the RAWcooked encoding (and TAR preservation scripts) used for these DPX automation workflows.  The aim of these scripts is to turn large DPX image sequences into RAWcooked FFV1 Matroska files for preservation within the BFI's Digital Preservation Infrastructure (DPI). Encoding DPX sequences to FFV1 can reduce the overall file size by half (2K RGB files), and allow the DPX image sequence to be played in VLC or similar software for instant review.

These scripts are available under the MIT licence. They have been recently redeveloped and may contain some untested features within the code. If you wish to test these yourself please create a safe environment to use this code separate from preservation critical files. All comments and feedback welcome.


## Overview

These bash shell scripts and Python scripts are not designed to be run from the command line, but via cron scheduling. As a result there is no built in help command, so please refer to this README and the script comments for information about script functionality.

These scripts handle the complete encoding process from start to finish, including assessment of the DPX sequences suitability for RAWcooked encoding, splitting of sequences too large for ingest into our DPI, encoding, failure assessment of the Matroska, and clean up of completed processes with deletion of DPX sequences. If a DPX sequence does not meet the basic DPX Mediaconch policy requirements for RAWcooked encoding then the sequence is failed and passed to a TAR wrap preservation path. These scripts also manage the demuxing of RAWcooked FFV1 Matroska files back to a DPX sequence, and the unpacking to Python tarfile wrapped TAR DPX sequences.

RAWcooked encoding functions with two scripts, the first encodes all items found in the RAWcooked encoding path and the second assesses the results of these encoding attempts. If an encoding fails, dpx_post_rawcook.sh will assess the error type moving failed files to a seprate folder, and create a new list which allows the RAWcooked first encoding script to try again with a different encoding formula, using '--output-version 2'. If it fails again an error is issued to an current errors log, flagging the folder in need of human intervention.

The Python TAR scripts generates checksums for all items in the DPX folder, TAR wraps the folder using Python tarfile then creates checksums of all the tarfile contents and compares the original with the new MD5 checksum manifest. This MD5 manifest is then added into the TAR file, and all processes are output to logs as well as being written to the Collections and Information Database (CID). Both encoding scripts move successful encodings to the BFI's Digital Preservation Infrastructure (DPI) ingest path, and associated DPX sequence folders into a dpx_completed/ folder, the TAR wrapped files have their DPX sequences deleted immediately following checksum verification. The final script assesses the FFV1 Matroska in the check/ folder by checking the FFV1 Matroska with the RAWcooked '--check' function using the hashes stored in the reversibility data. The DPX sequence is then deleted the FFV1 Matroska is moved to DPI ingest workflows.


## Dependencies

These scripts are run from Ubuntu 20.04LTS installed server and rely upon various Linux command line programmes. These include: flock, md5sum, tree, grep, cat, echo, ls, head, rm, touch, basename, dirname, find, du, rev, cut, mv, cp, date, sort and uniq. You can find out more about these by running the manual (man md5sum) or by calling the help page (md5sum --help).  

Several open source softwares are used from Media Area. Please follow the links below to find out more:  
RAWcooked version 21.01+ - https://mediaarea.net/rawcooked (dpx_rawcook.sh is not compatible with any earlier versions of RAWcooked)  
FFmpeg Version 4+ - https://ffmpeg.org/  
MediaConch V18.03.2+ - https://mediaarea.net/mediaconch  
MediaInfo V19.09 - https://mediaarea.net/mediainfo (dpx_assessment.sh is not currently compatible with some later version if 4K scans in use)  

To run the concurrent processes the scripts use GNU Parallel which will require installation (with dependencies of it's own that may include the following):  

    GNU parallel may also require: sysstat 12.2.0, libsensors 5-6.0, libsensors-config 3.6.0
    available here http://archive.ubuntu.com/ubuntu/pool/main/l/lm-sensors/libsensors-config_3.6.0-2ubuntu1_all.deb
    available here http://archive.ubuntu.com/ubuntu/pool/main/l/lm-sensors/libsensors5_3.6.0-2ubuntu1_amd64.deb
    available here http://archive.ubuntu.com/ubuntu/pool/main/s/sysstat/sysstat_12.2.0-2_amd64.deb
    available here http://archive.ubuntu.com/ubuntu/pool/universe/p/parallel/parallel_20161222-1.1_all.deb

The TAR wrapping script uses Python standard library 'tarfile'.  


## Environmental variable storage  

These scripts are being operated on each server using environmental variables that store all path and key data for the script operations. These environmental variables are persistent so can be called indefinitely. They are imported to Python scripts near the beginning using ```os.environ['VARIABLE']```, and are called in shell scripts like ```"${VARIABLE}"```. They are saved into the /etc/environment file.
See list at bottom for variable links to folder paths.


## Operational environment

The scripts operate within a defined folder structure. These automation_dpx folders are deposited at various storage locations, and the dpx_encoding repository scripts are broken into folder path names to reflect this, eg ‘film_operations’, ‘qnap_film’ etc. The automation_dpx folder contents is always formatted like this so the scripts work across locations:

```bash
automation_dpx  
├── current_errors  
│   ├── oversized_sequences
│   ├── completed
│   │   └── N_3623230_04of04_errors.log
│   ├── N_489447_01of01_errors.log
│   └── N_294553_01of03_errors.log
├── encoding  
│   ├── dpx_completed  
│   │   ├── N_3623230_04of04  
│   │   ├── N_3623278_01of02
│   ├── dpx_for_review
│   │   └── N_489447_01of01  
│   ├── dpx_to_assess  
│   │   ├── N_3623284_03of03  
│   │   └── N_489875_2_08of08
│   ├── dpx_to_assess_fourdepth
│   │   ├── N_294553_01of03
│   │   └── N_294553_02of03  
│   ├── part_whole_split  
│   │   ├── rawcook  
│   │   └── tar  
│   ├── rawcooked  
│   │   ├── dpx_to_cook  
│   │   │   └── N_473236_01of02  
│   │   ├── encoded  
│   │   │   ├── check  
│   │   │   ├── killed  
│   │   │   ├── logs  
│   │   │   └── mkv_cooked  
│   │   │       └── N_473236_01of02.mkv  
│   │   │       └── N_473236_01of02.mkv.txt  
│   │   └── failures  
│   ├── script_logs  
│   ├── tar_preservation  
│   │   ├── for_tar_wrap  
│   │   │   └── N_489855_2_01of01  
│   │   ├── failures  
│   │   ├── logs  
│   │   └── checksum_manifests  
│   └── to_delete  
├── unwrap_tar
│   ├── completed
│   ├── failed
│   └── N_4702557_01of01.tar
└── unwrap_rawcook_mkv
    ├── completed       
    └── N_4723317_01of01.mkv

```

## Supporting crontab actions

The RAWcooked and TAR scripts are to be driven from a server /etc/crontab.  
To prevent the scripts from running multiple versions at once and overburdening the server RAM the crontab calls the scripts via Linux Flock lock files (called from /usr/bin/flock shown below). These are manually created in the /var/run folder, and the script flock_rebuild.sh regularly checks for their presence, and if absent, recreates them every hour. It is common for the lock files to disappear when a server is rebooted, etc.

The scripts for encoding and automation_dpx/ activities will run frequently throughout the day:  
dpx_assessment.sh / dpx_assessment_fourdepth.sh - Every four hours one or other of the scripts using shared lock (launches dpx_splitting_script.py processing one job at a time).    
dpx_rawcook.sh - Runs continually, with crontab attempts made (but blocked by Flock when active) every 15 minutes to ensure continual encoding activity  
dpx_post_rawcook.sh - Runs three times a day every 8 hours, at 8am, 4pm, and 12am  
dpx_tar_script.sh - Runs once a day at 10pm  
dpx_check_script.sh - Runs once a day at 5am  

DPX Encoding script crontab entries:  

    30    */4   *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_assess.lock         /mnt/path/dpx_encoding/film_operations/dpx_assessment.sh  
    05    */4   *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_assess.lock         /mnt/path/dpx_encoding/film_operations/dpx_assessment_fourdepth.sh  
    */15  *     *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_rawcook.lock        /mnt/path/dpx_encoding/film_operations/dpx_rawcook.sh  
    45    */4   *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_post_rawcook.lock   /mnt/path/dpx_encoding/film_operations/dpx_post_rawcook.sh  
    0     22    *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_tar_script.lock     /mnt/path/dpx_encoding/film_operations/tar_wrapping_launch.sh  
    0     5     *    *    *       username      /usr/bin/flock -w 0 --verbose /var/run/dpx_check_script.lock   /mnt/path/dpx_encoding/film_operations/dpx_check_script.sh  
    15    */4   *    *    *       username      /usr/bin/python3 /mnt/path/dpx_encoding/film_operations/dpx_part_whole_move.py > /tmp/python_cron.log  
    */55  *     *    *    *       username      /mnt/path/dpx_encoding/flock_rebuild.sh  
    

## THE SCRIPTS

### dpx_assessment.sh / dpx_assessment_fourdepth.sh
(Launches Python splitting script)

These scripts assess a DPX sequence's suitability to be RAWcooked encoded, based on criteria met within the metadata of the fourth DPX file. The metadata is checked against a Mediaconch policy, if it fails, the folder is passed to the tar_preservation/ folder path.
dpx_assessment.sh script need the DPX sequences to be formatted identically:  N_123456_01of01/scan01/2048x1556/<dpx_files>
dpx_assessment_fourdepth.sh script needs the DPX sequence formatted identically: N_123456_01of01/2048x1556/Scan01/R01of01/<dpx_files>
They both run from the same Flock lock file to avoid running splits simultaneously which would endanger the ordering process. If the first assessment script is busy using the lock then the second will skip until the next pass.

Script functions:
- Checks if contents of dpx_to_assess/dpx_to_assess_fourdepth folder has any contents. Exits if not to avoid unecessary log entries
- Checks downtime_control.json to see if script runs inhibited
- Refreshes the DPX success and failure lists, tar, rawcooked, luma and python lists so clean for each run of the script, avoiding path failures.
- dpx_assessment.sh looks within the dpx_to_assess/ folder for DPX sequence directories at pixel ratio folder level. Eg, 2048x1556/ (found at mindepth 3 / maxdepth 3)
- dpx_assessment_fourdepth.sh looks within the dpx_to_assess_fourdepth/ folder for DPX sequence directories at reel folder level. Eg, R01of01/ (found at mindepth 4 / maxdepth 4)
- Takes the first DPX within this folder and stores in a 'dpx' variable, creates further variables using the basename and dirname of path
- Creates documents for preservation in sequence. These include first dpx metadata, directory tree, whole byte size of directory.
- Greps for the 'filename' within script_logs/rawcooked_dpx_success.log and script_logs/tar_dpx_failures.log. If appears in either then the file is skipped. If not:
  - Checks 'dpx' to Mediaconch policy rawcooked_dpx_orientation.xml to check for DPX files that require vertical flipping of image. Creates RAWcooked_notes.txt to warn of potential '--check' failures.
  - Compares 'dpx' to Mediaconch policy rawcooked_dpx_policy.xml (policy specifically written to pass/fail RAWcooked encodings)
  - If pass, looks for metadata indicating if the DPX are 4K, have RGB or Luma (Y) colourspace, before writing 'filename' to rawcooked_dpx_list.txt or luma_4k_dpx_list.txt
  - If fail writes 'filename' to tar_dpx_list.txt and outputs reason for failure to script_logs/dpx_assessment.log
- Each list created in previous stage is sorted by its part whole, and passed into a loop that calculates the total folder size in KB, then writes this data to new list python_list.txt
- The contents of python_list.txt are passed one at a time to the Python splitting script for size assessment and potential splitting. From here they are moved to their encoding paths if 01of01, or into the part_whole_split folder if multiple parts exist.
- Appends the luma, 4K, rawcooked and tar failure lists to rawcooked_dpx_success.log and tar_dpx_failures.log

Requires use of rawcooked_dpx_policy.xml and rawcooked_dpx_orientation.xml.


### dpx_splitting_script.py

This script is not listed in the crontab as it is launched at the end dpx_assessment.sh to arrange movement of the image sequence, and where necessary splits the folders into smaller folders to allow for RAWcooked / TAR wrapping of a finished file no larger that 1TB.

Script function:
- Receives three arguments within SYS.ARGV[1], splitting them into:
  - Total KB size of the DPX sequence
  - Path to DPX sequence
  - Encoding type - rawcooked, luma, 4k or tar
- Checks if DPX filename/part whole is properly formatted and if 'DPX' string is the file_type in associated DPX Item record
  - If yes, continue with splitting
  - If no, exit script with warning in logs and DPX sequence moved to 'dpx_to_review' folder
- Checks if folder depth is correct for one of two accepted formats, no other folders present that shouldn't be
  - If yes, continue with splitting
  - If now, exit script with warning in logs and DPX sequence moved to 'dpx_to_review' folder
- Checks if the DPX sequence name is listed in splitting_document.csv first column 'original' name
  - If yes, renumbers the folder and updates the dpx_sequence/dpx_path variables
  - If no, skips onto next stage
- Divisions are calculated based upon the DPX sequence total KB size. The options returned include:
  - No division needed because the folder is under the minimum encoding size for Imagen. The DPX folders are moved to their encoding paths if the have the part whole 01of01. If not they are moved straight to part_whole_split folder to await their remaining parts.  
     Script exits, completed.
  - Oversized folder. The folder is too large to be divided by this script and needs human intervention (over 6TB/6.5TB). The folder is moved to current_errors/oversized_sequences/ folder and the error log is appended.
     Script exits, completed.
  - Divisions are required by between 2 and 6 splits depending on the size of the DPX sequence and whether the encoding is for RAWcooked RGB, RAWcooked luma, RAWcooked 4K or TAR wrapping (see table 1)
    Script continues to next step
- Splittings functions begin:
  - DPX_splitting.log is updated with details of the DPX sequence that required splitting
  - New folder names are generated for the split folder additions and old numbers have new folder part wholes calculated for them. The current DPX sequence folder has it’s number updated, and the dpx_sequence and dpx_path variables are updated.
  - All original folder names, new folder names, and today's date are updated respectively to splitting_document.csv
  - DPX sequence path is iterated over finding all folders within it that contain files ending in ‘.dpx’ or ‘.DPX’. This will find all instances of scan01, scan02 folders within the DPX path and splits/moves each equally.
  - New folders have new paths created, incorporating all sub folders down to the level of the DPX files.
  - All DPX files within the DPX path are counted per scan folder, divided as per division (2 to 6 - see table 1).
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
| 5.2TB to 6.5TB | 4.0TB to 5.0TB  | 4.0TB to 5.0TB  | 4.0TB to 5.0TB  | 5 Divisions     |  
|                | 5.0TB to 6.0TB  | 5.0TB to 6.0TB  | 5.0TB to 6.0TB  | 6 Divisions     |

NOTES: We've only recently started RAWcooked encoding Y (Luma) and 4K DPX sequences, and on average the first Matroska files have approximate size reductions of 27% (Luma Y) and 30% (RGB 4K). This has shown to be very variable depending on the DPX image sequence content, with the occassional file only 4-5% smaller than the DPX sequence. Because of this we're currently assuming that any of these files could have reduced size reductions and are therefore setting the divisions sizes the same as TAR wrapping which has no compression.


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
- Checks if any items have RAWcooked_notes.txt file in folder, indicating vertically flipped images and adds filename to separate list to tell RAWcooked to include a framemd5 manifest in the MKV file to allow for manual reversibility testing where the '--check' function may fail.
- Passes text lists of DPX sequences to GNU parallel to start RAWcooked encoding multiple jobs at a time
  Script generates FFV1 Matroska file and log file of RAWcooked console outputted logging data, used in dpx_post_rawcook.sh

- Refreshes temp_queued_list.txt again

PASS TWO:
- Feeds list of DPX sequences in dpx_to_cook/ into loop and compares against rawcooked_success.log and temp_queued_list.txt
  - If a DPX sequence is not on either lists the folder name is written to temporary_rawcook_list.txt
- Checks if any items have RAWcooked_notes.txt file in folder, indicating vertically flipped images and adds filename to separate list to tell RAWcooked to include a framemd5 manifest in the MKV file to allow for manual reversibility testing where the '--check' function may fail.
- Takes the text lists and performs filter of all names by part whole extension
- Trims first twenty from this filtered list and passes to rawcook_list.txt and outputs this list to the log, allowing for analysis later
- Passes these DPX sequence names to GNU parallel to start RAWcooked encoding multiple jobs at a time
  Script generates FFV1 Matroska file and log file of RAWcooked console outputted logging data, used in dpx_post_rawcook.sh


### dpx_post_rawcook.sh

A script assesses Matroska files, and logs, before deciding if a file can be moved to autoingest or to failures folder. Moves successful DPX sequences to dpx_completed/ folder ready for clean up scripts.

Script functions:  
- Checks if any items in mkv_cooked folder, if not exits without any Log entries
- Refresh all temporary lists .txt files generated in the scripts

MKV file size check:
- Obtains total size of encoded Matroska. If it's larger that 1TB (1048560MB) moves to killed/ folder and appends current_errors files with failure
- Outputs warning to logs that this file needs all part wholes removing for splitting scripts to enact file size reductions.
- If undersize, skips but reports filename is under 1TB to log.

Mediaconch check:
- Looks for Matroska files in mkv_cooked/ path not modified in the last ten minutes, checks each one against the basic RAWcooked mkv Mediaconch Policy
- If pass: script continues and information passed to post_rawcook.log
- If fail: script writes filename to temp_mediaconch_policy_fails.txt, and writes failure to post_rawcook.log. Matroska items listed on temp_mediaconch_policy_fails.txt are moved to Killed/ folder and logs are prepended fail_ and move to logs folder

Grep logs for pass statement of Mediaconch passed files:
- Script looks through all logs for 'Reversablity was checked, no issue detected' where found:
  - Outputs successful cooked filenames to rawcooked_success.log
  - Prints list to post_rawcook.log
  - Moves Matroska files using GNU parallel to check/ folder
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

Search for log files that have not been modified in over 48 hours (2880 minutes):
- For all mkv.txt files older than 2880 minutes since last modified (ie stale encodings):
  - Generates list and outputs to post_rawcook.log
  - Deletes logs files
  - While loop checks if Matroska exists too, if yes deletes.
- Sorts temp_rawcooked_succes.log and updates rawcook_success.log with these new additions.
- Deletes all temporary lists generated for each while loop.

Requires use of rawcooked_mkv_policy.xml.

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

### dpx_check_script.sh

This script replaces the clean up script. It runs a rawcooked --check call against each MKV and if it passes moves the MKV to DPI ingest, and moves the DPX sequence to the deletions folder and deletes it immediately.

Run checks against MKV files:
- First it looks for any MKV files in the check/ folder. If none it exits and avoids any needless log outputs.
- Create temporary log files that build through script searches:
  - mkv_list.txt, dpx_deletion_list.txt, successful_mkv_list.txt, failure_mkv_list.txt
- Find all MKV files not modified for 30 minutes in check/ folder and sort them. Output fullpath to new temp_mkv_list.txt overwriting previous entries.
- Open temp_mkv_list.txt and extract MKV file basename from each entry. Pass into new list one at a time called mkv_list.txt, which is located in check/ folder. Output this tidy name list to log.
- Launch GNU parallel to run 5 parallel --check runs against each folder, and output all console statements to log with same MKV file name appended .txt, also placed into check/ folder.

Review of check logs:
- Review check logs for success statement ‘Reversability was checked, no issue detected.'
  - Where no success statement is found (failed the --check) the name of the MKV file is written to failure_mkv_list.txt in the check/ folder.
  - Where a success statement is found (passes the --check) the name of the MKV file is written to successful_mkv_list.txt in check/ folder – and the DPX sequence name is written to dpx_deletion_list.txt placed in the dpx_completed/ folder.
- The failed_mkv_list.txt is next checked to see if there are any entries:
  - If no entries, the log is updated that no files failed --check
  - If MKV files failed the --check function (very unlikely) then a warning is added to the script log, and the contents of the fail list are written to the list (echoing failure statement in previous stage). The dpx_encoding_errors.log is updated with the list of failed --check files. The MKV files are moved into killed/ folder and the MKV associated logs are moved into logs/ folder prepended ‘check_fail_{}'. Finally the associated DPX sequence is moved from dpx_completed/ folder to dpx_to_cook/ folder
- The success_mkv_list.txt is checked to see if there are any successful MKV checks:
  - If yes, the MKV files are moved from check/ folder into DPI ingest path for MKV RAWcooked video files. MKV associated logs are moved to logs/ folder prepended ‘check_pass_{}’.
- The last stage is to move the successful DPX sequences (matched sequence numbers from the MKV file name) into a deletion path and delete. The dpx_deletion_list.txt is read and checked first:
  - If no contents are found then the log is updated that there are no DPX sequences for deletion
  - If the list has DPX folder names it proceeds with the following actions:
    - Moves all DPX directory filenames in list from dpx_completed/ to to_delete/ folder - using GNU parallel 10 jobs at a time.
    - The list is read again and all matching files within the to_delete/ folder, are deleted - using GNU parallel 3 jobs at a time (DPX deletions can be very CPU use heavy).
- Temporary txt files are deleted again:
  - mkv_list.txt, dpx_deletion_list.txt, successful_mkv_list.txt, failure_mkv_list.txt


### flock_rebuild.sh

This short script is called by crontab each day to check that the Flock locks are still available in /var/run.
Loads with a list of flock lock paths. A for loop checks if each path in the list exists, if not it uses touch to recreate it.


### Environmental variable mapping

#### Paths to different storage devices:
QNAP_FILM, QNAP_FILMOPS, QNAP_FILMOPS2  
QNAP_DIGIOPS, GRACK_FILM, FILM_OPS  
  
#### automation_dpx folder variables:  
DPX_ASSESS="automation_dpx/encoding/dpx_to_assess/"  
DPX_ASSESS_FOUR="automation_dpx/encoding/dpx_to_assess_fourdepth/"  
DPX_COOK="automation_dpx/encoding/rawcooked/dpx_to_cook/"  
DPX_REVIEW="automation_dpx/encoding/dpx_for_review/"  
DPX_COMPLETE="automation_dpx/encoding/dpx_completed/"  
DPX_SCRIPT_LOG="automation_dpx/encoding/script_logs/"  
RAWCOOKED_PATH="automation_dpx/encoding/rawcooked/"  
TAR_PRES="automation_dpx/encoding/tar_preservation/"  
TO_DELETE="automation_dpx/encoding/to_delete/"  
MKV_ENCODED="automation_dpx/encoding/rawcooked/encoded/"  
MKV_CHECK="automation_dpx/encoding/rawcooked/encoded/check/"  
MKV_ENCODED_FILM="automation_dpx/encoding/rawcooked/encoded_film/"  
MKV_CHECK_FILM="automation_dpx/encoding/rawcooked/encoded_film/check/"  
TAR_LOG="automation_dpx/encoding/tar_preservation/logs/"  
DPX_WRAP="automation_dpx/encoding/tar_preservation/dpx_to_wrap/"  
DPX_TARRED="automation_dpx/encoding/tar_preservation/tarred_files/"  
TAR_FAIL="automation_dpx/encoding/tar_preservation/failures/"  
CURRENT_ERRORS="automation_dpx/current_errors/"  
PART_TAR="automation_dpx/encoding/part_whole_split/tar/"  
PART_RAWCOOK="automation_dpx/encoding/part_whole_split/rawcook/"  
SEQ_RENUMBER="automation_dpx/sequence_renumbering/"

#### dpx_encoding script variables:
POLICY_RAWCOOK="dpx_encoding/rawcooked_mkv_policy.xml"  
POLICY_DPX="dpx_encoding/rawcooked_dpx_policy.xml"
SPLITTING_SCRIPT_QNAP_FILM="dpx_encoding/qnap_film/dpx_splitting_script.py"  
SPLITTING_SCRIPT_FILMOPS="dpx_encoding/film_operations/dpx_splitting_script.py"  
SPLITTING_SCRIPT_QNAP_FILMOPS="dpx_encoding/qnap_filmops/dpx_splitting_script.py"
SPLITTING_SCRIPT_QNAP_FILMOPS2="dpx_encoding/qnap_filmops2/dpx_splitting_script.py"  
SPLITTING_SCRIPT_QNAP_DIGIOPS="dpx_encoding/qnap_digiops/dpx_splitting_script.py"  
SPLITTING_SCRIPT_GRACK_FILM="dpx_encoding/grack_film/dpx_splitting_script.py"  
  
