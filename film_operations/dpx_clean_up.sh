#!/bin/bash -x

# ===================================================================
# === DPX clean up, compare to global.log and delete if completed ===
# ===================================================================

# Global variables extracted from environmental variables
DPX_PATH="${FILM_OPS}${DPX_COMPLETE}"
DPX_LOG="${FILM_OPS}${DPX_SCRIPT_LOG}"
GLOB_LOG="${FILM_OPS}${GLOBAL_LOG}"
FOR_DELETION="${FILM_OPS}${TO_DELETE}"
ERRORS="${FILM_OPS}${CURRENT_ERRORS}"

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
find "$DPX_PATH" -type d -maxdepth 1 -mindepth 1 | rev | sort -n -k1.5 | rev > "${DPX_PATH}temp_dpx_list.txt"

# Refresh files_for_deletion_list.txt
rm "${DPX_PATH}files_for_deletion_list.txt"
touch "${DPX_PATH}files_for_deletion_list.txt"

# Begin with writing to log
log "Comparing dpx_completed/ folder to global.log"

# Run grep search for named items and pass for line reading to while loop which sorts
cat "${DPX_PATH}temp_dpx_list.txt" | while IFS= read -r files; do
    trim_file=$(basename "$files")
    object=$(echo "$trim_file" | rev | cut -c 8- | rev)
    # Check if file present in part_whole_search, skip if so
    count=$(grep -c "$object" "${ERRORS}part_whole_search.log")
    if [ "$count" -eq 0 ];
      then
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
      else
        log "$trim_file is in part_whole_search.log and should be left for splitting scripts"
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
        # Moves successfully ingested items in deleted list to 'to_delete/' folder
        grep ^N_ "${DPX_PATH}files_for_deletion_list.txt" | parallel --jobs 10 "mv ${DPX_PATH}{} ${FOR_DELETION}{}"
        # Deletes the files that have been successfully ingested to Imagen AND deleted
        log "Deleting files listed as 'Successfully deleted' and moved to ${FOR_DELETION}"
        grep ^N_ "${DPX_PATH}files_for_deletion_list.txt" | parallel --jobs 3 "rm -r ${FOR_DELETION}{}"
fi

log "============= DPX clean up script END ============="
