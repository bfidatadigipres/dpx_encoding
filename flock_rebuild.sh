#!/bin/bash

#############################################
# CHECK IF FLOCK FILES EXIST, IF NOT RECREATE
#############################################

LOCKS=( "/var/run/dpx_assess_filmop.lock"
        "/var/run/dpx_assess_qnap.lock"
        "/var/run/dpx_rawcook_filmop.lock"
        "/var/run/dpx_rawcook_qnap.lock"
        "/var/run/dpx_post_rawcook_filmop.lock"
        "/var/run/dpx_post_rawcook_qnap.lock"
        "/var/run/dpx_tar_script_filmop.lock"
        "/var/run/dpx_tar_script_qnap.lock"
        "/var/run/dpx_clean_up_filmop.lock"
        "/var/run/dpx_clean_up_qnap.lock"
)

for lock in "${LOCKS[@]}" ; do
    if [[ -f "$lock" ]];then
        true
    else
        touch "$lock"
    fi
done
