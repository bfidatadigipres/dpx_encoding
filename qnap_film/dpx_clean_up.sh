#!/bin/bash -x

# ===================================================================
# === DPX clean up, compare to global.log and delete if completed ===
# ===================================================================

# Global variables extracted from environmental variables
DPX_PATH="${QNAP_FILM}${DPX_COMPLETE}"
DPX_LOG="${QNAP_FILM}${DPX_SCRIPT_LOG}"
GLOB_LOG="${QNAP_FILM}${GLOBAL_LOG}"
FOR_DELETION="${QNAP_FILM}${TO_DELETE}"
ERRORS="${QNAP_FILM}${CURRENT_ERRORS}"

# Global date variables
THIS_MONTH=$(date --date="$(date +%Y-%m-%d)" "+%Y-%m")
LAST_MONTH=$(date --date="$(date +%Y-%m-%d) -1 month" "+%Y-%m")

# Function to write output to log, call 'log' + 'statement' that populates $1.
function log {
    timestamp=$(date "+%Y-%m-%d - %H.%M.%S")
    echo "$1 - $timestamp"
} >> "${DPX_LOG}dpx_clean_up.log"

log "============= DPX clean up script START ============="

# Create list for folders in dpx_completed/ folder ordered by second digit of the extensions 01of**
find "$DPX_PATH" -maxdepth 1 -mindepth 1 -type d | sort -n -k10.12 > "${DPX_PATH}temp_dpx_list.txt"

# Refresh files_for_deletion_list.txt
rm "${DPX_PATH}files_for_deletion_list.txt"
touch "${DPX_PATH}files_for_deletion_list.txt"

# Begin with writing to log
log "Comparing dpx_completed/ folder to global.log"

# Run grep search for named items and pass for line reading to while loop which sorts
cat "${DPX_PATH}temp_dpx_list.txt" | while IFS= read -r files; do
    trim_file=$(basename "$files")
    object=$(echo "$trim_file" | rev | cut -c 8- | rev)
    # Check if file present on global log this month/last month
    on_global=$(grep "$trim_file" "$GLOB_LOG" | grep 'Successfully deleted file' | grep "$THIS_MONTH")
    if [ -z "$on_global" ]
      then
        retry=$(grep "$trim_file" "$GLOB_LOG" | grep 'Successfully deleted file' | grep "$LAST_MONTH")
        if [ -z "$retry" ]
          then
            skipped=$(grep "$trim_file" "$GLOB_LOG" | grep 'Skip object' | grep "$THIS_MONTH")
            if [ -z "$skipped" ]
              then
                log "****** ${files} MKV/TAR HAS NOT PASSED INTO AUTOINGEST! ******"
              else
                log "${files} MKV/TAR is still being ingested"
            fi
          else
            log "DELETE: ${files} MKV/TAR has been RAWcooked or TAR wrapped and is in Imagen"
            echo "$trim_file" >> "${DPX_PATH}files_for_deletion_list.txt"
        fi
      else
        log "DELETE: ${files} MKV/TAR has been RAWcooked or TAR wrapped and is in Imagen"
        echo "$trim_file" >> "${DPX_PATH}files_for_deletion_list.txt"
    fi
done

# Checks for items that were written to files_for_deletion_list.txt
file_list=$(grep ^N_ "${DPX_PATH}files_for_deletion_list.txt")
log "Checking for files that made it through to deletion..."
if [ -z "$file_list" ]
    then
        log "There are no items for deletion at this time"
    else
        log "Moving any documented files to deletion path:"
        log "$file_list"
        # Moves successfully ingested items in deleted list to delete folder
        grep ^N_ "${DPX_PATH}files_for_deletion_list.txt" | parallel --jobs 10 "mv ${DPX_PATH}{} ${FOR_DELETION}{}"
        # Deletes the files that have been successfully ingested to Imagen AND deleted
        log "Deleting files listed as 'Successfully deleted' and moved to ${FOR_DELETION}"
#        grep ^N_ "${DPX_PATH}files_for_deletion_list.txt" | parallel --jobs 3 "rm -r ${FOR_DELETION}{}"
fi

log "============= DPX clean up script END ============="

