#!/bin/bash -x

# ==========================================================================
# === DPX sequence conformance pass to RAWcook/TAR DPX splitting scripts ===
# ==========================================================================

# Global variables call environmental variables
DPX_LOG="${IS_DIGITAL}${DPX_SCRIPT_LOG}"
DPX_PATH="${IS_DIGITAL}${DPX_ASSESS_FOUR}"
ERRORS="${IS_DIGITAL}${CURRENT_ERRORS}"
POLICY_PATH="${POLICY_DPX}"
POLICY_PATH2="${POLICY_IMAGE_ORIENTATION}"
PY3_LAUNCH="${PY3_ENV}"
SPLITTING="${SPLITTING_SCRIPT_ISILON_DIGIOPS}"

# Function to write output to log, using call 'log' + 'statement' to populate $1.
function log {
    timestamp=$(date "+%Y-%m-%d - %H.%M.%S")
    echo "$1 - $timestamp"
} >> "${DPX_LOG}dpx_assessment_fourdepth.log"

# Check for DPX sequences in path before script launch
if [ -z "$(ls -A ${DPX_PATH})" ]
  then
    echo "No files available for encoding, script exiting"
    exit 1
  else
    log "============= DPX Assessment workflow START ============="
fi

# Function to check for control json activity
function control {
    boole=$(cat "${LOG_PATH}downtime_control.json" | grep "rawcooked" | awk -F': ' '{print $2}')
    if [ "$boole" = false, ] ; then
      log "Control json requests script exit immediately"
      log "===================== DPX assessment workflow ENDED ====================="
      exit 0
    fi
}

# Refresh temporary success/failure lists
touch "${DPX_PATH}rawcooked_dpx_list.txt"
touch "${DPX_PATH}tar_dpx_list.txt"
touch "${DPX_PATH}luma_4k_dpx_list.txt"
touch "${DPX_PATH}python_list.txt"

# Control check
control

# Loop that retrieves single DPX file in each folder, runs Mediaconch check and generates metadata files
# Configured for four level folders: N_123456_01of01/dimensions/scan01/Reel/<dpx_seq>
find "${DPX_PATH}" -maxdepth 4 -mindepth 4 -type d -mmin +30 | while IFS= read -r files; do
    # Find first DPX of sequence
    dpx=$(ls "$files" | head -1)
    reel=$(basename "$files")
    scans=$(basename "$(dirname "$files")")
    dimensions=$(basename "$(dirname "$(dirname "$files")")")
    filename=$(basename "$(dirname "$(dirname "$(dirname "$files")")")")
    file_scan_name="$filename/$dimensions/$scans"
    count_queued_pass=$(grep -c "$file_scan_name" "${DPX_PATH}rawcook_dpx_success.log")
    count_queued_fail=$(grep -c "$file_scan_name" "${DPX_PATH}tar_dpx_failures.log")

    if [ "$count_queued_pass" -eq 0 ] && [ "$count_queued_fail" -eq 0 ];
        then
            # Output metadata to filepath into second level folder
            log "Metadata file creation has started for:"
            log "- ${dimensions}/${scans}/${reel}/${dpx}"
            timeout 3600 mediainfo -f "${files}/${dpx}" > "${DPX_PATH}${file_scan_name}/${filename}_${dpx}_metadata.txt"
            tree "${files}" > "${DPX_PATH}${file_scan_name}/${filename}_directory_contents.txt"
            byte_size=$(timeout 3600 du -s -b "${DPX_PATH}${filename}")
            echo "${filename} total folder size in bytes (du -s -b from BK-CI-DATA3): ${byte_size}" > "${DPX_PATH}${file_scan_name}/${filename}_directory_total_byte_size.txt"

            # Check for Image orientation 'Bottom to top' and manage with temporary file additions
            orientation_check=$(timeout 3600 mediaconch --force -p "${POLICY_PATH2}" "${files}/${dpx}" | grep "pass!")
            if [ -z "$orientation_check" ]
                then
                    log "Skipping: File does not have 'Bottom to top' orientation issues."
                else
                    log "File has 'Bottom to top' Image orientation! Adding RAWcooked warning note to folder"
                    touch "${DPX_PATH}${file_scan_name}/RAWcooked_notes.txt"
                    echo "This sequence has been exported with imagen orientation reading 'Bottom to top'." >> "${DPX_PATH}${file_scan_name}/RAWcooked_notes.txt"
                    echo "This RAWcooked transcode has flipped the image using FFmpeg '-vf vflip'." >> "${DPX_PATH}${file_scan_name}/RAWcooked_notes.txt"
                    echo "As a result it may fail future '--check' tests, so a framemd5 manifest" >> "${DPX_PATH}${file_scan_name}/RAWcooked_notes.txt"
                    echo "will be included to allow for manual reversibility tests after decoding." >> "${DPX_PATH}${file_scan_name}/RAWcooked_notes.txt"
                    echo "dpx_assessment_fourdepth $(date "+%Y-%m-%d - %H.%M.%S"): DPX in ${filename} have image orientation bottom to top." >> "${ERRORS}${filename}_errors.log"
                    echo "    As a result the RAWcooked MKV file may fail the '--check' tests, but may be a good FFV1 Matroska file." >> "${ERRORS}${filename}_errors.log"
                    echo "    Please contant the Knowledge and Collections Developer if the file fails 'check' tests." >> "${ERRORS}${filename}_errors.log"
            fi

            # Start comparison of first dpx file against mediaconch policy
            check=$(timeout 3600 mediaconch --force -p "${POLICY_PATH}" "${files}/$dpx" | grep "pass!")
            if [ -z "$check" ]
                then
                    log "FAIL: $file_scan_name DOES NOT CONFORM TO MEDIACONCH POLICY. Adding to tar_dpx_failures_list.txt"
                    log "$check"
                    echo "${DPX_PATH}$filename" >> "${DPX_PATH}tar_dpx_list.txt"
                    echo "dpx_assessment $(date "+%Y-%m-%d - %H.%M.%S"): DPX ${filename} failed DPX Mediaconch policy and will be TAR wrapped." >> "${ERRORS}${filename}_errors.log"
                else
                    width_find=$(timeout 3600 mediainfo --Details=1 "${files}/$dpx" | grep -i 'Pixels per line:')
                    read -a array <<< "$width_find"
                    if [ "${array[4]}" -gt 3999 ]
                        then
                            log "PASS: 4K scan $file_scan_name has passed the MediaConch policy and can progress to RAWcooked processing path"
                            echo "${DPX_PATH}$filename" >> "${DPX_PATH}luma_4k_dpx_list.txt"
                        else
                            descriptor=$(timeout 3600 mediainfo --Details=1 "${files}/$dpx" | grep -i "Descriptor" | grep -i "Luma (Y)")
                            if [ -z "$descriptor" ]
                                then
                                    log "PASS: RGB $file_scan_name has passed the MediaConch policy and can progress to RAWcooked processing path"
                                    echo "${DPX_PATH}$filename" >> "${DPX_PATH}rawcooked_dpx_list.txt"
                                else
                                    log "PASS: Luma (Y) $file_scan_name has passed the MediaConch policy and can progress to RAWcooked processing path"
                                    echo "${DPX_PATH}$filename" >> "${DPX_PATH}luma_4k_dpx_list.txt"
                            fi
                    fi
            fi
        else
            log "SKIPPING DPX folder, it has already been processed but has not moved to correct processing path:"
            log "$file_scan_name"
    fi
done

# Prepare luma_4k_dpx_list for DPX splitting script/move to RAWcooked preservation
if [ -s "${DPX_PATH}luma_4k_dpx_list.txt" ]; then
  list1=$(cat "${DPX_PATH}luma_4k_dpx_list.txt" | sort -n -k10.12 )
  log "RAWcooked Luma Y/4K path items for size check and Python splitting/moving script:"
  log "$list1"
  echo "$list1" > "${DPX_PATH}luma_4k_dpx_list.txt"
  cat "${DPX_PATH}luma_4k_dpx_list.txt" | while IFS= read -r line1; do
    kb_size=$(du -s "$line1" | cut -f1)
    log "Size of $line1 is $kb_size KB. Passing to Python script..."
    echo "$kb_size, $line1, luma_4k" >> "${DPX_PATH}python_list.txt";
  done
fi

# Prepare tar_dpx_failure_list for DPX splitting script/move to TAR preservation
if [ -s "${DPX_PATH}tar_dpx_list.txt" ]; then
  list2=$(cat "${DPX_PATH}tar_dpx_list.txt" | sort -n -k10.12 )
  log "TAR path items for size check and Python splitting/moving script:"
  log "$list2"
  echo "$list2" > "${DPX_PATH}tar_dpx_list.txt"
  cat "${DPX_PATH}tar_dpx_list.txt" | while IFS= read -r line2; do
    kb_size2=$(du -s "$line2" | cut -f1)
    log "Size of $line2 is $kb_size2 KB. Passing to Python script..."
    echo "$kb_size2, $line2, tar" >> "${DPX_PATH}python_list.txt";
  done
fi

# Prepare dpx_success_list for DPX splitting script/move to RAWcooked preservation
if [ -s "${DPX_PATH}rawcooked_dpx_list.txt" ]; then
  list3=$(cat "${DPX_PATH}rawcooked_dpx_list.txt" | sort -n -k10.12 )
  log "RAWcooked 2K RGB path items for size check and Python splitting/moving script:"
  log "$list3"
  echo "$list3" > "${DPX_PATH}rawcooked_dpx_list.txt"
  cat "${DPX_PATH}rawcooked_dpx_list.txt" | while IFS= read -r line3; do
    kb_size3=$(du -s "$line3" | cut -f1)
    log "Size of $line3 is $kb_size3 KB. Passing to Python script..."
    echo "$kb_size3, $line3, rawcooked" >> "${DPX_PATH}python_list.txt";
  done
fi

# Take python_list.txt and iterate through entries, passing to Python script one of each instance
if [ -s "${DPX_PATH}python_list.txt" ]; then
  log "Launching python script to process DPX sequences. Please see dpx_splitting_script.log for more details"
  grep '/mnt/' "${DPX_PATH}python_list.txt" | sort -u | parallel --jobs 1 "$PY3_LAUNCH $SPLITTING '{}'"
  log "===================== DPX assessment workflows ENDED ====================="
fi

# Append latest pass/failures to movement logs
cat "${DPX_PATH}rawcooked_dpx_list.txt" >> "${DPX_PATH}rawcook_dpx_success.log"
cat "${DPX_PATH}luma_4k_dpx_list.txt" >> "${DPX_PATH}rawcook_dpx_success.log"
cat "${DPX_PATH}tar_dpx_list.txt" >> "${DPX_PATH}tar_dpx_failures.log"

# Clean up temporary files
rm "${DPX_PATH}rawcooked_dpx_list.txt"
rm "${DPX_PATH}tar_dpx_list.txt"
rm "${DPX_PATH}luma_4k_dpx_list.txt"
rm "${DPX_PATH}python_list.txt"

