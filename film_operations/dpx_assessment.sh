#!/bin/bash -x

# =================================================================
# === DPX sequence conformance pass to RAWcook/TAR preservation ===
# =================================================================

# Global variables call environmental variables
LOG_PATH="${FILM_OPS}${DPX_SCRIPT_LOG}"
DPX_PATH="${FILM_OPS}${DPX_ASSESS}"
RAWCOOKED="${FILM_OPS}${RAWCOOKED_PATH}"
POLICY_PATH="$POLICY_DPX"
TAR_DEST="${FILM_OPS}${TAR_PRES}"

# Function to write output to log, using call 'log' + 'statement' to populate $1.
function log {
    timestamp=$(date "+%Y-%m-%d - %H.%M.%S")
    echo "$1 - $timestamp"
} >> "${LOG_PATH}dpx_assessment.log"

# Refresh temporary success/failure lists
rm "${DPX_PATH}dpx_success_list.txt"
rm "${DPX_PATH}dpx_failures_list.txt"
touch "${DPX_PATH}dpx_success_list.txt"
touch "${DPX_PATH}dpx_failures_list.txt"

# Write first log output
log "===================== DPX assessment workflows start ====================="

# Loop that retrieves single DPX file in each folder, runs Mediaconch check and generates metadata files
find "${DPX_PATH}" -maxdepth 4 -mindepth 4 -type d | while IFS= read -r files; do
    # Find fifth DPX of sequence (avoid non-DPX files already in folder or poor formed first/last DPX files)
    dpx=$(ls "$files" | head -5 | tail -1)
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
            log "- ${file_scan_name}/$reel/${dpx}"
            mediainfo -f "${files}/${dpx}" > "${DPX_PATH}${file_scan_name}/${filename}_${dpx}_metadata.txt"
            tree "${files}" > "${DPX_PATH}${file_scan_name}/${filename}_directory_contents.txt"

            # Start comparison of first dpx file against mediaconch policy
            check=$(mediaconch --force -p "${POLICY_PATH}" "${files}/$dpx" | grep "pass!")
            if [ -z "$check" ]
                then
                    log "FAIL: $file_scan_name DOES NOT CONFORM TO MEDIACONCH POLICY. Adding to tar_dpx_failures_list.txt"
                    log "$check"
                    echo "${DPX_PATH}$filename" >> "${DPX_PATH}dpx_failures_list.txt"
                else
                    log "PASS: $file_scan_name has passed the MediaConch policy and can progress to RAWcooked processing path"
                    echo "${DPX_PATH}$filename" >> "${DPX_PATH}dpx_success_list.txt"
            fi
        else
            log "SKIPPING DPX folder, it has already been processed but has not moved to correct processing path:"
            log "$file_scan_name"
    fi
done

# Move tar_dpx_failure_list to tar_preservation/ folder
log "Moving items that failed MediaConch policy to tar_preservation/ path:"
list1=$(cat "${DPX_PATH}dpx_failures_list.txt")
log "$list1"
cat "${DPX_PATH}dpx_failures_list.txt" | parallel --jobs 10 "mv {} ${TAR_DEST}dpx_to_wrap/"

# Move successful files to rawcooked/ folder path
log "Moving items that passed MediaConch policy to rawcooked/ path:"
list2=$(cat "${DPX_PATH}dpx_success_list.txt")
log "$list2"
cat "${DPX_PATH}dpx_success_list.txt" | parallel --jobs 10 "mv {} ${RAWCOOKED}dpx_to_cook/"

# Append latest pass/failures to movement logs
cat "${DPX_PATH}dpx_success_list.txt" >> "${DPX_PATH}rawcook_dpx_success.log"
cat "${DPX_PATH}dpx_failures_list.txt" >> "${DPX_PATH}tar_dpx_failures.log"

log "===================== DPX Assessment workflows ends ====================="
