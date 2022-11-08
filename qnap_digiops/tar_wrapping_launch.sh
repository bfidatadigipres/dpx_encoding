#!/bin/bash -x

# ============================================
# Launch script for Film Ops tar wrap script =
# ============================================

date_FULL=$(date +'%Y-%m-%d - %T')

# Path to folder
FPATH="${QNAP_DIGIOPS}${DPX_WRAP}"
LOGS="${QNAP_DIGIOPS}${DPX_SCRIPT_LOG}tar_wrapping_checksum.log"
FLIST="${QNAP_DIGIOPS}${TAR_PRES}temp_file_list.txt"

if [ -z "$(ls -A ${FPATH})" ]
  then
    echo "Folder empty, for_tar_wrap, script exiting."
    exit 1
  else
    echo " =========== TAR WRAPPING CHECKSUM SCRIPT START =========== $date_FULL" >> "$LOGS"
    echo " Looking for files or folders in $FPATH " >> "$LOGS"
    echo " Writing any files/folders found to $FLIST: " >> "$LOGS"
fi

# Refresh list / add items to list
echo "" > "$FLIST"

find "$FPATH" -maxdepth 1 -mindepth 1 -mmin +10 | while IFS= read -r items; do
  item=$(basename "$items")
  echo "$item"
  echo "${FPATH}${item}" >> "$FLIST"
done

cat "$FLIST" >> "$LOGS"

# Launching Python script using parallel
echo " Launching Python script to TAR wrap files/folders " >> "$LOGS"
grep "/mnt/" "$FLIST" | parallel --jobs 1 "${PY3_ENV} ${DPX_SCRIPTS}qnap_digiops/tar_wrapping_checksum.py {}"
echo " =========== TAR WRAPPING CHECKSUM SCRIPT END =========== $date_FULL\n" >> "$LOGS"
