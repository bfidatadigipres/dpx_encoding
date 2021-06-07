#!/bin/bash

#############################################
# CHECK IF FLOCK FILES EXIST, IF NOT RECREATE
#############################################

LOCKS=( "/var/run/dpx_assess1.lock"
        "/var/run/dpx_assess2.lock"
        "/var/run/dpx_assess3.lock"
        "/var/run/dpx_rawcook1.lock"
        "/var/run/dpx_rawcook2.lock"
        "/var/run/dpx_rawcook3.lock"
        "/var/run/dpx_post_rawcook1.lock"
        "/var/run/dpx_post_rawcook2.lock"
        "/var/run/dpx_post_rawcook3.lock"
        "/var/run/dpx_tar_script1.lock"
        "/var/run/dpx_tar_script2.lock"
        "/var/run/dpx_tar_script3.lock"
        "/var/run/dpx_clean_up1.lock"
        "/var/run/dpx_clean_up2.lock"
        "/var/run/dpx_clean_up3.lock"
)

for lock in "${LOCKS[@]}" ; do
    if [[ -f "$lock" ]];then
        true
    else
        touch "$lock"
    fi
done
