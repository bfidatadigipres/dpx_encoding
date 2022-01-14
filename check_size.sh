#!/bin/bash -x

# =======================================
# === Size retrieval from Ingest path ===
# =======================================

# Global variables call environmental variables
INGEST="${QNAP_FILM}${AUTOINGEST_VID}"
SIZE_LIST="${QNAP_FILM}size_list.txt"

rm "${SIZE_LIST}"
touch "${SIZE_LIST}"

# Find all MKV files with newer modification time than supplied file
find "${INGEST}" -name '*.mkv' -newer "${INGEST}N_4460230_09of21.mkv" | sort | while IFS= read -r files; do
    # Get kb size output to text file for comparison later
    kb_size=$(du -s "$files")
    echo "$kb_size" >> "${SIZE_LIST}"
done
