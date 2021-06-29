#!/bin/bash -x

# ============================================
# === DPX sequence TAR preservation script ===
# ============================================

# Global paths exctracted from environmental vars
SCRIPT_LOG="${FILM_OPS}${DPX_SCRIPT_LOG}"
LOG_PATH="${FILM_OPS}${TAR_LOG}"
DPX_PATH="${FILM_OPS}${DPX_WRAP}"
DESTINATION="${FILM_OPS}${DPX_TARRED}"
TAR_FAILED="${FILM_OPS}${TAR_FAIL}"
AUTOINGEST="${FILM_OPS_FIN}${AUTOINGEST_DOC}"
MD5PATH="${MD5_PATH}"
ERRORS="${FILM_OPS}${CURRENT_ERRORS}"

# Refresh temporary succes/failure lists
rm "${DPX_PATH}tar_list.txt"
rm "${DESTINATION}tar_list_complete.txt"
rm "${DESTINATION}passed_tar_list.txt"
rm "${DESTINATION}failed_tar_list.txt"
touch "${DPX_PATH}tar_list.txt"
touch "${DESTINATION}passed_tar_list.txt"
touch "${DESTINATION}failed_tar_list.txt"
touch "${DESTINATION}tar_list_complete.txt"

# Function to write output to log, bypass echo calls, using just log + statement.
function log {
    timestamp=$(date "+%Y-%m-%d - %H.%M.%S")
    echo "$1 - $timestamp"
} >> "${SCRIPT_LOG}dpx_tar_script.log"

# Start TAR preparations and wrap of DPX sequences
log "===================== DPX TAR preservation workflow start ====================="

find "$DPX_PATH" -maxdepth 3 -mindepth 3 -type d | while IFS= read -r tar_files; do
    # Extract usable filenames from the path
    scans=$(basename $(dirname "$tar_files"))
    filename=$(basename $(dirname $(dirname "$tar_files")))
    file_scan_name="${filename}/${scans}/"
    # Check if the files are already being processed
    count_tarred=$(cat "${DESTINATION}tar_checks.txt" | grep -c "$filename")
    if [ "$count_tarred" -eq 0 ];
        then
            log "Filename being processed: ${filename}"
            # Start md5 manifest generation for each file in loop
            find "${tar_files}" -name "*.dpx" -exec md5sum {} \; >> "${DPX_PATH}${file_scan_name}${filename}_md5sum_manifest.md5"

            # Begin tar wrap using Linux TAR module before passing to parallel
            log "Putting $filename in list for TAR processing with GNU Parallel"
            echo "$filename" >> "${DPX_PATH}tar_list.txt"
        else
            log "Skipping ${filename}, as it has been successfully tarred already"
    fi
done

# Import tar_list/txt file to gnu parallel for TAR wrapping
log "------- TAR wrap beginning ---------------------- No compression -mx=0 store only -------"
grep ^N_ "${DPX_PATH}tar_list.txt" | parallel --jobs 5 7z a -mx=0 "${DESTINATION}{}.tar" "${DPX_PATH}{}" &>> "${DESTINATION}{}.tar.log"

# Check tar files size is under 1TB otherwise move out of verification checks and path to autoingest
find "$DESTINATION" -name "*.tar" | while IFS= read -r tar_file; do
    filename=$(basename "$tar_file")
    object=$(echo "$tar_file" | rev | cut -c 11- | rev )
    echo "$filename"
    size=$(du -m "$tar_file" | cut -f1 )
    if [ "${size}" -gt 1048560 ]
        then
            log "${filename} maybe larger than 1TB: ${size}mb. Moving to review folder for splitting"
            log "SPLITTING WARNING: All files within ${filename} part whole will need reviewing"
            echo "TARRED ${filename} is over 1TB. All DPX part wholes will need reviewing" >> "${ERRORS}oversize_files_error.log"
            echo "${object}" >> "${ERRORS}part_whole_search.log"
            mv "${tar_file}" "${TAR_FAILED}failed_oversize_${filename}"
            log "Moving failed ${filename}.log to ${LOG_PATH}"
            mv "${tar_file}.log" "${LOG_PATH}failed_oversize_${filename}.log"
        else
            log "Writing ${filename} into tar_list_complete.txt for verification checks"
            echo "$filename" >> "${DESTINATION}tar_list_complete.txt"
    fi
done

# When wrap finished start verification of successful batch, append to log
log "------- TAR wrap concluded ------------------------------- Beginning verification -------"
log "Starting verification checks of tar files, and ouputting verification test results to log"
grep ^N_ "${DESTINATION}tar_list_complete.txt" | parallel --jobs 2 7z t "${DESTINATION}{}" &>> "${DESTINATION}{}.log"

# Begin verification checks from the accompanying log files
find "${DESTINATION}" -name "*.tar.log" | while IFS= read -r checked_logs; do
    # Running analysis of tar file using verify setting (sourceforge question...)
    log "Looking for error signs in ${checked_logs} from verification and tarring activities"
    tar_pass=$(cat "${checked_logs}" | grep -i 'error\|fatal\|critical\|warning\|warnings')
    filename=$(basename "$checked_logs")
    if [ -z "$tar_pass" ]
        then
            # Create MD5 checksum for whole TAR file
            log "${filename} NO ERROR WORDS FOUND - Writing filename to passed_tar_list for MD5 sum generation"
            echo "$filename" >> "${DESTINATION}passed_tar_list.txt"
        else
            log "***** FAILED: TAR file ${filename} has FAILED a verification check *****"
            log "$tar_pass"
            echo "${filename} TAR ERROR in log ${checked_logs}" >> "${ERRORS}dpx_encoding_errors.log"
            echo "$filename" >> "${DESTINATION}failed_tar_list.txt"
    fi
done

# Write md5sum for each TAR file using Parallel
log "Generating md5 checksum for passed_tar_list.txt"
grep ^N_ "${DESTINATION}passed_tar_list.txt" | parallel --jobs 5 md5sum "${DESTINATION}{}" > "${MD5PATH}{}.md5"

# TESTING NEEDED: Add date to end of TAR MD5 record for persistence.py usage
log "Reformatting log for Python script access, adding date and replacing '  ' spaces with ' - '"
grep ^N_ "${DESTINATION}passed_tar_list.txt" | while IFS= read -r md5_logs; do
    # Add date to end of log
    sed -i '1s/.*/&'"  $(date +%Y-%m-%d)"'/' "${MD5PATH}${md5_logs}.md5"
    # Reformat log with hyphen between data, for processing by persistence.py
    md5_content=$(cat "${MD5PATH}${md5_logs}.md5")
    echo "${md5_content}" | sed 's/[ ][ ]*/ - /g' > "${MD5PATH}${md5_logs}.md5"
done

# Move successfully TAR and verified files from passed_tar_list.txt
log "Moving successful .tar wrapped files to QNAP-03 autoingest/ingest/store/document and log files"
grep ^N_ "${DESTINATION}passed_tar_list.txt" | parallel --jobs 5 mv "${DESTINATION}{}" "${AUTOINGEST}{}"
grep ^N_ "${DESTINATION}passed_tar_list.txt" | parallel --jobs 5 mv "${DESTINATION}{}.log" "${LOG_PATH}{}.log"
log "Moving succesfully tar wrapped folders to QNAP-03 dpx_wrapped folder"
grep ^N_ "${DESTINATION}passed_tar_list.txt" | rev | cut -c 5- | rev | parallel --jobs 5 mv "${DPX_PATH}{}" "${DPX_WRAP}{}"

# Move unsuccessful TAR or verified files from failed_tar_list.txt
log "Moving unsuccessful .tar wrapped files to QNAP-03 failed_verification and log files"
grep ^N_ "${DESTINATION}failed_tar_list.txt" | parallel --jobs 5 mv "${DESTINATION}{}" "${TAR_FAILED}{}"
grep ^N_ "${DESTINATION}failed_tar_list.txt" | parallel --jobs 5 mv "${DESTINATION}{}.log" "${LOG_PATH}failed_{}.log"
log "Leaving unsuccessfully TAR wrapped folders where they are for retry"

# Copy passed_tar_list.txt contents to tar_checks.txt to prevent re-wrapping if script overlaps or fails prematurely
log "Copying passed tar list to tar_checks.txt to prevent successful items being re-wrapped"
cp "${DESTINATION}passed_tar_list.txt" "${DESTINATION}tar_checks.txt"

# Writing script close statement
log "===================== DPX TAR preservation workflow start ====================="

