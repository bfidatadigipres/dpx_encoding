#!/bin/bash -x

# ===============================
# === RAWcook decoding script ===
# ===============================

# Global variables extracted from environmental variables
FPATH="$1"
echo "${FPATH}"
SCRIPT_LOG="${LOG_PATH}"
DPX_PATH="${FPATH}${UNWRAP_RAWCOOK}"
ERRORS="${DPX_PATH}failed/"
COMPLETED="${DPX_PATH}completed/"

# Function to write output to log, call 'log' + 'statement' that populates $1.
function log {
    timestamp=$(date "+%Y-%m-%d - %H.%M.%S")
    echo "$timestamp - $1"
} >> "${SCRIPT_LOG}dpx_unwrap_rawcook.log"

touch "${DPX_PATH}unwrap_list.txt"
touch "${DPX_PATH}confirmed_unwrap_list.txt"

# Get list of MKV files in UNWRAP_MKV path
find "${DPX_PATH}" -maxdepth 1 -mindepth 1 -name '*.mkv' -mmin +10 >> "${DPX_PATH}unwrap_list.txt"

# Write a START note to the logfile if files for decoding, else exit
if [ -s "${DPX_PATH}unwrap_list.txt" ]
  then
    log "============= DPX Unwrap RAWcook ${1} START ============="
    list=$(cat "${DPX_PATH}unwrap_list.txt")
    log "Files found to process:"
    log "$list"
  else
    echo "No files available for demuxing, script exiting."
    rm "${DPX_PATH}unwrap_list.txt"
    rm "${DPX_PATH}confirmed_unwrap_list.txt"
    exit 1
fi

# ========================
# === Unwrap RAWcooked ===
# ========================

# Output RAWcooked version to log
version=$(rawcooked --version)
log "RAWcooked version in use for demux: $version"

# Run first pass where list generated for large reversibility cases by dpx_post_rawcook.sh
log "Checking for files in the Unwrap MKV folder"
grep '/mnt/' "${DPX_PATH}unwrap_list.txt" | while IFS= read -r file; do
  foldername=$(basename "$file" | rev | cut -c 5- | rev)
  rawcook_fname=$("${DPX_PATH}RAWcooked_unwrap_${foldername}")
  log "File found: $file"
  if [ -d "$rawcooked_fname" ]
    then
      log "Folder already exists and has been unwrapped: $rawcooked_fname."
    else
      log "No RAWcooked folder found for file. MKV suited to demux: $file"
      echo "$foldername" >> "${DPX_PATH}confirmed_unwrap_list.txt"
  fi
done

# Begin RAWcooked processing with GNU Parallel / CHMOD RAWcooked folder
log "Launching RAWcooked demux of folder now..."
grep ^N_ "${DPX_PATH}confirmed_unwrap_list.txt" | parallel --jobs 1 "rawcooked -y --all ${DPX_PATH}{}.mkv -o ${DPX_PATH}RAWcooked_unwrap_{} &>> ${DPX_PATH}{}.mkv.txt"
grep ^N_ "${DPX_PATH}confirmed_unwrap_list.txt" | parallel --jobs 5 "sudo chmod 777 -R ${DPX_PATH}RAWcooked_unwrap_{}"

# Check for pass statement then mov MKV to 'completed' folder
log "Demux complete. Checking for log success statements"
find "$DPX_PATH" -maxdepth 1 -mindepth 1 -name "*.mkv.txt" | while IFS= read -r checks; do
  echo "$checks"
  success_check=$(grep 'Reversibility was checked, no issue detected.' "$checks")
  filename=$(basename "$checks")
  fname=$(echo "$filename" | rev | cut -c 5- | rev)
  fname_log=$(echo "$filename" | rev | cut -c 9- | rev)
  echo "$fname"

  if [ -z '$success_check' ]
    then
       log "PROBLEM: '$fname' failed string check for completion success"
       log "PROBLEM: '$fname' needs attention and reversibility tests"
       echo "unwrap_rawcook $(date "+%Y-%m-%d - %H.%M.%S"): Matroska ${fname} failed reversibility." >> "${ERRORS}${fname_log}_errors.log"
       echo "\tPlease contact the Knowledge and Collections Developer about this reversibility failure." >> "${ERRORS}${fname_log}_errors.log"
    else
       log "Demux completed successfully: $fname"
       log "Moving file to completed folder for manual deletion"
       mv "${DPX_PATH}${fname}" "${COMPLETED}${fname}"
  fi
done

log "===================== DPX Unwrap RAWcook ENDED ====================="

rm "${DPX_PATH}unwrap_list.txt"
rm "${DPX_PATH}confirmed_unwrap_list.txt"
