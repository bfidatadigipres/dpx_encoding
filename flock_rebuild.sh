#!/bin/bash

#############################################
# CHECK IF FLOCK FILES EXIST, IF NOT RECREATE
#############################################

LOCKS=( "/var/run/dpx_assess.lock"
        "/var/run/dpx_rawcook.lock"
        "/var/run/dpx_post_rawcook.lock"
        "/var/run/dpx_tar_script.lock"
        "/var/run/dpx_clean_up.lock"
)

for lock in "${LOCKS[@]}" ; do
    if [[ -f "$lock" ]];then
        true
    else
        touch "$lock"
    fi
done
