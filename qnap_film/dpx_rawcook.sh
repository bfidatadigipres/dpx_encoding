#!/bin/bash -x

# =========================================================================
# === RAWcook encoding script, two pass for --check and --check-padding ===
# =========================================================================

# Global variables extracted from environmental variables
SCRIPT_LOG="${QNAP_FILM}${DPX_SCRIPT_LOG}"
DPX_PATH="${QNAP_FILM}${DPX_COOK}"
MKV_DEST="${QNAP_FILM}${MKV_ENCODED}"

# Function to write output to log, call 'log' + 'statement' that populates $1.
function log {
    timestamp=$(date "+%Y-%m-%d - %H.%M.%S")
    echo "$1 - $timestamp"
} >> "${SCRIPT_LOG}dpx_rawcook.log"

# Remove or generate temporary files per script run
rm "${MKV_DEST}temporary_rawcook_list.txt"
rm "${MKV_DEST}temporary_retry_list.txt"
rm "${MKV_DEST}retry_list.txt"
rm "${MKV_DEST}rawcook_list.txt"
ls "${MKV_DEST}mkv_cooked" > "${MKV_DEST}temp_queued_list.txt"
touch "${MKV_DEST}temporary_rawcook_list.txt"
touch "${MKV_DEST}temporary_retry_list.txt"
touch "${MKV_DEST}retry_list.txt"
touch "${MKV_DEST}rawcook_list.txt"

# Write a START note to the logfile
log "===================== DPX RAWcook START ====================="

# ========================
# === RAWcook pass one ===
# ========================

# Run first pass where list generated for large reversibility cases by dpx_post_rawcook.sh
log "Checking for files that failed RAWcooked due to large reversibility files"
grep '/mnt/' "${MKV_DEST}reversibility_list.txt" | while IFS= read -r retry; do
  folder_retry=$(basename "$retry")
  count_cooked_2=$(grep -c "$folder_retry" "${MKV_DEST}rawcooked_success.log")
  count_queued_2=$(grep -c "$folder_retry" "${MKV_DEST}temp_queued_list.txt")
  # Those not already queued/active passed to list, else bypassed
  if [ "$count_cooked_2" -eq 0 ] && [ "$count_queued_2" -eq 0 ];
   then
    echo "$folder_retry" >> "${MKV_DEST}temporary_retry_list.txt"
  fi
done

# Sort the temporary_rawcook_list by part of extension, pass first 20 to rawcook_list.txt
grep ^N_ "${MKV_DEST}temporary_retry_list.txt" | sort -n -k10.12 | uniq > "${MKV_DEST}retry_list.txt"
cook_retry=$(grep ^N_ "${MKV_DEST}retry_list.txt")
log "DPX folder will be cooked using --output-version 2:"
log "${cook_retry}"

# Begin RAWcooked processing with GNU Parallel using --output-version 2
cat "${MKV_DEST}retry_list.txt" | parallel --jobs 5 "rawcooked -y --all --no-accept-gaps --output-version 2 ${DPX_PATH}{} -o ${MKV_DEST}mkv_cooked/{}.mkv &>> ${MKV_DEST}mkv_cooked/{}.mkv.txt"

rm "${MKV_DEST}reversibility_list.txt"

# ========================
# === RAWcook pass two ===
# ========================

# Refresh temporary queued list
ls "${MKV_DEST}mkv_cooked" > "${MKV_DEST}temp_queued_list.txt"

# When large reversibility cooks complete target all N_ folders, and pass any not already being processed to temporary_rawcook_list.txt
log "Outputting files from DPX_PATH to list, if not already queued"
find "${DPX_PATH}" -maxdepth 1 -mindepth 1 -type d -name "N_*" | while IFS= read -r folders; do
  folder_clean=$(basename "$folders")
  count_cooked=$(grep -c "$folder_clean" "${MKV_DEST}rawcooked_success.log")
  count_queued=$(grep -c "$folder_clean" "${MKV_DEST}temp_queued_list.txt")
  if [ "$count_cooked" -eq 0 ] && [ "$count_queued" -eq 0 ];
   then
    echo "$folder_clean" >> "${MKV_DEST}temporary_rawcook_list.txt"
  fi
done

# Sort the temporary_rawcook_list by part of extension, pass first 20 to rawcook_list.txt and write items to log
grep ^N_ "${MKV_DEST}temporary_rawcook_list.txt" | sort -n -k10.12 | uniq | head -20 > "${MKV_DEST}rawcook_list.txt"
cook_list=$(grep ^N_ "${MKV_DEST}rawcook_list.txt")
log "DPX folder will be cooked:"
log "${cook_list}"

# Begin RAWcooked processing with GNU Parallel
cat "${MKV_DEST}rawcook_list.txt" | parallel --jobs 5 "rawcooked -y --all --no-accept-gaps -s 5281680 ${DPX_PATH}{} -o ${MKV_DEST}mkv_cooked/{}.mkv &>> ${MKV_DEST}mkv_cooked/{}.mkv.txt"

log "===================== DPX RAWcook ENDED ====================="
