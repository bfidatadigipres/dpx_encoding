#!/bin/bash -x

# ==========================================================================
# === DPX sequence conformance pass to RAWcook/TAR DPX splitting scripts ===
# ==========================================================================

# Global variables call environmental variables
LOG_PATH="${QNAP_FILM}${DPX_SCRIPT_LOG}"
# DPX_PATH="${QNAP_FILM}${DPX_ASSESS}"
DPX_PATH="/mnt/qnap_film/Public/test/"
POLICY_PATH="$POLICY_DPX"
PY3_LAUNCH="${PY3_ENV}"
SPLITTING="${SPLITTING_SCRIPT}"

# Function to write output to log, using call 'log' + 'statement' to populate $1.
function log {
    timestamp=$(date "+%Y-%m-%d - %H.%M.%S")
    echo "$1 - $timestamp"
} >> "${LOG_PATH}dpx_assessment.log"

# Refresh temporary success/failure lists
rm "${DPX_PATH}rawcooked_dpx_list.txt"
rm "${DPX_PATH}tar_dpx_list.txt"
rm "${DPX_PATH}luma_dpx_list.txt"
touch "${DPX_PATH}rawcooked_dpx_list.txt"
touch "${DPX_PATH}tar_dpx_list.txt"
touch "${DPX_PATH}luma_dpx_list.txt"

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
#            mediainfo -f "${files}/${dpx}" > "${DPX_PATH}${file_scan_name}/${filename}_${dpx}_metadata.txt"
#            tree "${files}" > "${DPX_PATH}${file_scan_name}/${filename}_directory_contents.txt"

            # Start comparison of first dpx file against mediaconch policy
            check=$(mediaconch --force -p "${POLICY_PATH}" "${files}/$dpx" | grep "pass!")
            if [ -z "$check" ]
                then
                    log "FAIL: $file_scan_name DOES NOT CONFORM TO MEDIACONCH POLICY. Adding to tar_dpx_failures_list.txt"
                    log "$check"
                    echo "${DPX_PATH}$filename" >> "${DPX_PATH}tar_dpx_list.txt"
                else
                    descriptor=$(mediainfo --Details=1 "${files}/$dpx" | grep -i "Descriptor" | grep -i "Luma (Y)")
                    if [ -z "$descriptor" ]
                        then
                            log "PASS: RGB $file_scan_name has passed the MediaConch policy and can progress to RAWcooked processing path"
                            echo "${DPX_PATH}$filename" >> "${DPX_PATH}rawcooked_dpx_list.txt"
                        else
                            log "PASS: Luma (Y) $file_scan_name has passed the MediaConch policy and can progress to RAWcooked processing path"
                            echo "${DPX_PATH}$filename" >> "${DPX_PATH}luma_dpx_list.txt"
                    fi
            fi
        else
            log "SKIPPING DPX folder, it has already been processed but has not moved to correct processing path:"
            log "$file_scan_name"
    fi
done

# Prepare luma_dpx_list for DPX splitting script/move to RAWcooked preservation
log "Luma Y path items for size check and Python splitting/moving script:"
list1=$(cat "${DPX_PATH}luma_dpx_list.txt")
log "$list1"
cat "${DPX_PATH}luma_dpx_list.txt" | while IFS= read -r line1; do
  kb_size_path=$(du -s "$line1")
  kb_size=${kb_size_path::11}
  kb_strip=$(echo "$kb_size" | xargs)
  log "Size of $line1 is $kb_strip KB. Passing to Python script..."
  parallel --jobs 1 "$PY3_LAUNCH" "$SPLITTING" "$kb_strip" "$line" "luma";
done

# Prepare tar_dpx_failure_list for DPX splitting script/move to TAR preservation
log "TAR path items for size check and Python splitting/moving script:"
list2=$(cat "${DPX_PATH}tar_dpx_list.txt")
log "$list2"
cat "${DPX_PATH}tar_dpx_list.txt" | while IFS= read -r line2; do
  kb_size_path2=$(du -s "$line2")
  kb_size2=${kb_size_path2::11}
  kb_strip2=$(echo "$kb_size2" | xargs)
  log "Size of $line2 is $kb_strip2 KB. Passing to Python script..."
  parallel --jobs 1 "$PY3_LAUNCH" "$SPLITTING" "$kb_strip2" "$line2" "tar";
done

# Prepare dpx_success_list for DPX splitting script/move to RAWcooked preservation
log "RAWcooked path items for size check and Python splitting/moving script:"
list3=$(cat "${DPX_PATH}rawcooked_dpx_list.txt")
log "$list3"
cat "${DPX_PATH}rawcooked_dpx_list.txt" | while IFS= read -r line3; do
  kb_size_path3=$(du -s "$line3")
  kb_size3=${kb_size_path3::11}
  kb_strip3=$(echo "$kb_size3" | xargs)
  log "Size of $line3 is $kb_strip3 KB. Passing to Python script..."
  parallel --jobs 1 "$PY3_LAUNCH" "$SPLITTING" "$kb_strip3" "$line3" "rawcooked";
done

# Append latest pass/failures to movement logs
cat "${DPX_PATH}rawcooked_dpx_list.txt" >> "${DPX_PATH}rawcook_dpx_success.log"
cat "${DPX_PATH}luma_dpx_list.txt" >> "${DPX_PATH}rawcook_dpx_success.log"
cat "${DPX_PATH}tar_dpx_list.txt" >> "${DPX_PATH}tar_dpx_failures.log"

log "===================== DPX Assessment workflows ends ====================="
