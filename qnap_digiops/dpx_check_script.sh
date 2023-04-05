#!/bin/bash -x

# ===================================================
# === DPX check_script / deletion of DPX sequence ===
# ===================================================

# Global variables extracted from environmental variables
MKV_PATH="${QNAP_DIGIOPS}${MKV_CHECK}"
MKV_LOG="${QNAP_DIGIOPS}${MKV_ENCODED}logs/"
MKV_KILLED="${QNAP_DIGIOPS}${MKV_ENCODED}killed/"
MKV_AUTOINGEST="${QNAP_DIGIOPS}${AUTOINGEST_VID}"
DPX_PATH="${QNAP_DIGIOPS}${DPX_COMPLETE}"
DPX_TO_COOK="${QNAP_DIGIOPS}${DPX_COOK}"
FOR_DELETION="${QNAP_DIGIOPS}${TO_DELETE}"
DPX_LOG="${QNAP_DIGIOPS}${DPX_SCRIPT_LOG}"
ERRORS="${QNAP_DIGIOPS}${CURRENT_ERRORS}"

# Function to write output to log
function log {
    timestamp=$(date "+%Y-%m-%d - %H.%M.%S")
    echo "$1 - $timestamp"
} >> "${DPX_LOG}dpx_check_script.log"

touch "${MKV_PATH}mkv_list.txt"
touch "${DPX_PATH}dpx_deletion_list.txt"
touch "${MKV_PATH}successful_mkv_list.txt"
touch "${MKV_PATH}failure_mkv_list.txt"

# ================================================================
# Matroska --check pass against all MKV files in check/ folder ===
# ================================================================

# Create list for Matroskas in check/ folder with basic sort against whole name
find "$MKV_PATH" -name '*.mkv' -mmin +30 | sort > "${MKV_PATH}temp_mkv_list.txt"
if [ -s "${MKV_PATH}temp_mkv_list.txt" ]
  then
    log "============= DPX check script START ============="
  else
    echo "No files found at this time, script exiting"
    exit 1
fi

# Extract basename to new list for tidy logs/--check path creation
grep '/mnt/' "${MKV_PATH}temp_mkv_list.txt" | while IFS= read -r mkv_list; do
    mkv_file=$(basename "$mkv_list")
    echo "$mkv_file" >> "${MKV_PATH}mkv_list.txt"
done

# Begin with writing to log
log "Compiled list of Matroskas for --check tests:"
list1=$(cat "${MKV_PATH}mkv_list.txt")
log "$list1"

# Begin parallel check passes and output results to log in same path
grep ^N "${MKV_PATH}mkv_list.txt" | parallel --jobs 5 "rawcooked --check ${MKV_PATH}{} &>> ${MKV_PATH}{}.txt"

# ===========================================================
# Review check logs for pass / failures and sift to lists ===
# ===========================================================

# Search for .txt files for success message
grep '/mnt/' "${MKV_PATH}temp_mkv_list.txt" | while IFS= read -r log_list; do
  success_check=$(grep 'Reversibility was checked, no issue detected.' "${log_list}.txt")
  mkv_file=$(basename "$log_list")
  dpx_seq=$(echo "$mkv_file" | rev | cut -c 5- | rev )
  if [ -z "$success_check" ];
    then
      log "FAILED: Matroska $mkv_file has errors detected."
      echo "$mkv_file" >> "${MKV_PATH}failure_mkv_list.txt"
    else
      log "PASSED: RAWcooked MKV $mkv_file passed --check successfully and will be moved to DPI ingest"
      echo "$dpx_seq" >> "${DPX_PATH}dpx_deletion_list.txt"
      echo "$mkv_file" >> "${MKV_PATH}successful_mkv_list.txt"
  fi
done

# ==========================================================================
# Moves failed MKV files and logs and return DPX sequence to dpx_to_cook ===
# ==========================================================================

# Checks for folders (not paths) that were written to failure_mkv_list.txt
fail_list=$(grep ^N_ "${MKV_PATH}failure_mkv_list.txt")
log "Checking for MKV files that failed --check assessment"
if [ -z "$fail_list" ]
    then
        log "There are no MKV files that failed --check"
    else
        log "WARNING! Failed --check items have been found:"
        log "$fail_list"
        log "Writing failures to dpx_encoding_errors.log"
        log "MKV files moved to killed/ folder. Associated DPX sequences moved back to dpx_to_cook/ folder"
        {
           echo "MKV files failed to pass --check tests:"
           "$fail_list"
           echo "MKV files moved to killed/ folder. Associated DPX sequences moved back to dpx_to_cook/ folder"
        } >> "${ERRORS}dpx_encoding_errors.log"
fi

# Move failed --check MKV files to killed folder
grep ^N_ "${MKV_PATH}failure_mkv_list.txt" | parallel --jobs 3 mv "${MKV_PATH}{}" "${MKV_KILLED}{}"

# Move the failed --check txt files to logs folder
grep ^N_ "${MKV_PATH}failure_mkv_list.txt" | parallel --jobs 3 mv "${MKV_PATH}{}.txt" "${MKV_LOG}check_fail_{}.txt"

# Move the DPX sequences back into dpx_to_cook for re-encoding
grep ^N_ "${MKV_PATH}failure_mkv_list.txt" | rev | cut -c 5- | rev | parallel --jobs 1 mv "${DPX_PATH}{}" "${DPX_TO_COOK}{}"

# =====================================
# Moves successful MKV files to DPI ===
# =====================================

# Move successfully encoded MKV files to autoingest
grep ^N_ "${MKV_PATH}successful_mkv_list.txt" | parallel --jobs 10 mv "${MKV_PATH}{}" "${MKV_AUTOINGEST}{}"

# Move the successful txt files to logs folder
grep ^N_ "${MKV_PATH}successful_mkv_list.txt" | parallel --jobs 10 mv "${MKV_PATH}{}.txt" "${MKV_LOG}check_pass_{}.txt"

# =============================================================
# Deletion of DPX sequences whose MKV files passed --check  ===
# =============================================================

# Checks for folders (not paths) that were written to dpx_deletion_list.txt
dpx_list=$(grep ^N_ "${DPX_PATH}dpx_deletion_list.txt")
log "Checking for DPX sequences that made it through to deletion..."
if [ -z "$dpx_list" ]
    then
        log "There are no items for deletion at this time"
    else
        log "Moving any documented files to deletion path:"
        log "$dpx_list"
        # Moves successfully ingested items in deleted list to 'to_delete/' folder
        grep ^N_ "${DPX_PATH}dpx_deletion_list.txt" | parallel --jobs 10 "mv ${DPX_PATH}{} ${FOR_DELETION}{}"
        # Deletes the files that have been successfully ingested to Imagen AND deleted
        log "Deleting files listed as 'Successfully deleted' and moved to ${FOR_DELETION}"
#        grep ^N_ "${DPX_PATH}dpx_deletion_list.txt" | parallel --jobs 3 "rm -r ${FOR_DELETION}{}"
fi

rm "${MKV_PATH}mkv_list.txt"
rm "${DPX_PATH}dpx_deletion_list.txt"
rm "${MKV_PATH}successful_mkv_list.txt"
rm "${MKV_PATH}failure_mkv_list.txt"

log "============= DPX check script END ============="
