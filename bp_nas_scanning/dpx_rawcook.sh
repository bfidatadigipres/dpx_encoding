#!/bin/bash -x

# =========================================================================
# === RAWcook encoding script, two pass for --check and --check-padding ===
# =========================================================================

# Global variables extracted from environmental variables
LOG="${BP_FILM_SCANNING}${DPX_SCRIPT_LOG}dpx_rawcook.log"
DPX_PATH="${BP_FILM_SCANNING}${DPX_COOK}"
MKV_DEST="${BP_FILM_SCANNING}${MKV_ENCODED}"

# Remove or generate temporary files per script run
ls "${MKV_DEST}mkv_cooked" > "${MKV_DEST}temp_queued_list.txt"
touch "${MKV_DEST}temporary_rawcook_list.txt"
touch "${MKV_DEST}temporary_retry_list.txt"
touch "${MKV_DEST}retry_list.txt"
touch "${MKV_DEST}retry_list_no_flip.txt"
touch "${MKV_DEST}retry_list_image_flip.txt"
touch "${MKV_DEST}rawcook_list.txt"
touch "${MKV_DEST}rawcook_list_no_flip.txt"
touch "${MKV_DEST}rawcook_list_image_flip.txt"

# Write a START note to the logfile if files for encoding, else exit
if [ -s "${MKV_DEST}reversibility_list.txt" ]
  then
    echo "============= DPX RAWcook script START ============= - $timestamp" >> "$LOG"
  else
    if [ -z "$(ls -A ${DPX_PATH})" ]
      then
        echo "No files available for encoding, script exiting"
        exit 1
      else
        echo "============= DPX RAWcook script START ============= - $timestamp" >> "$LOG"
    fi
fi

# ========================
# === RAWcook pass one ===
# ========================

# Run first pass where list generated for large reversibility cases by dpx_post_rawcook.sh
echo "Checking for files that failed RAWcooked due to large reversibility files - $timestamp" >> "$LOG"
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
echo "DPX folder will be cooked using --output-version 2: - $timestamp" >> "$LOG"
echo "${cook_retry} - $timestamp" >> "$LOG"

# Check here for RAWcooked_notes.txt, if found add framemd5 output commands
grep ^N_ "${MKV_DEST}retry_list.txt" | while IFS= read -r dpx_seq; do
  if [ -f "${DPX_PATH}${dpx_seq}RAWcooked_notes.txt" ]
    then
      echo "${dpx_seq}" >> "${MKV_DEST}retry_list_image_flip.txt"
    else
      echo "${dpx_seq}" >> "${MKV_DEST}retry_list_no_flip.txt"
  fi
done

# Begin RAWcooked processing with GNU Parallel using --output-version 2
cat "${MKV_DEST}retry_list_no_flip.txt" | parallel --jobs 6 --joblog "/mnt/qnap_03/automation_dpx/rawcook.log" "rawcooked -y --all --no-accept-gaps --output-version 2 -s 5281680 ${DPX_PATH}{} -o ${MKV_DEST}mkv_cooked/{}.mkv &>> ${MKV_DEST}mkv_cooked/{}.mkv.txt"
cat "${MKV_DEST}retry_list_image_flip.txt" | parallel --jobs 6 --joblog "/mnt/qnap_03/automation_dpx/rawcook.log" "rawcooked -y --all --no-accept-gaps --output-version 2 -s 5281680 --framemd5 ${DPX_PATH}{} -o ${MKV_DEST}mkv_cooked/{}.mkv &>> ${MKV_DEST}mkv_cooked/{}.mkv.txt"

rm "${MKV_DEST}reversibility_list.txt"

# ========================
# === RAWcook pass two ===
# ========================

# Refresh temporary queued list
ls "${MKV_DEST}mkv_cooked" > "${MKV_DEST}temp_queued_list.txt"

# When large reversibility cooks complete target all N_ folders, and pass any not already being processed to temporary_rawcook_list.txt
echo "Outputting files from DPX_PATH to list, if not already queued - $timestamp" >> "$LOG"
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
echo "DPX folder will be cooked: - $timestamp" >> "$LOG"
echo "${cook_list} - $timestamp" >> "$LOG"

# Check here for RAWcooked_notes.txt, if found add framemd5 output commands
grep ^N_ "${MKV_DEST}rawcook_list.txt" | while IFS= read -r dpx_seq; do
  if [ -f "${DPX_PATH}${dpx_seq}RAWcooked_notes.txt" ]
    then
      echo "${dpx_seq}" >> "${MKV_DEST}rawcook_list_image_flip.txt"
    else
      echo "${dpx_seq}" >> "${MKV_DEST}rawcook_list_no_flip.txt"
  fi
done

# Begin RAWcooked processing with GNU Parallel
cat "${MKV_DEST}rawcook_list_no_flip.txt" | parallel --jobs 6 --joblog "/mnt/qnap_03/automation_dpx/rawcook.log" "rawcooked -y --all --no-accept-gaps -s 5281680 ${DPX_PATH}{} -o ${MKV_DEST}mkv_cooked/{}.mkv &>> ${MKV_DEST}mkv_cooked/{}.mkv.txt"
cat "${MKV_DEST}rawcook_list_image_flip.txt" | parallel --jobs 6 --joblog "/mnt/qnap_03/automation_dpx/rawcook.log" "rawcooked -y --all --no-accept-gaps -s 5281680 --framemd5 ${DPX_PATH}{} -o ${MKV_DEST}mkv_cooked/{}.mkv &>> ${MKV_DEST}mkv_cooked/{}.mkv.txt"

echo "===================== DPX RAWcook ENDED ===================== - $timestamp" >> "$LOG"

#rm "${MKV_DEST}temporary_rawcook_list.txt"
#rm "${MKV_DEST}temporary_retry_list.txt"
#rm "${MKV_DEST}retry_list.txt"
#rm "${MKV_DEST}retry_list_no_flip.txt"
#rm "${MKV_DEST}retry_list_image_flip.txt"
#rm "${MKV_DEST}rawcook_list.txt"
#rm "${MKV_DEST}rawcook_list_no_flip.txt"
#rm "${MKV_DEST}rawcook_list_image_flip.txt"
