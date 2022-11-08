#!/bin/bash

#############################################
# CHECK IF FLOCK FILES EXIST, IF NOT RECREATE
#############################################

LOCKS=( "/var/run/dpx_assess1.lock"
        "/var/run/dpx_assess2.lock"
        "/var/run/dpx_assess3.lock"
        "/var/run/dpx_assess4.lock"
        "/var/run/dpx_assess5.lock"
        "/var/run/dpx_assess6.lock"
        "/var/run/dpx_assess7.lock"
        "/var/run/dpx_rawcook1.lock"
        "/var/run/dpx_rawcook2.lock"
        "/var/run/dpx_rawcook3.lock"
        "/var/run/dpx_rawcook4.lock"
        "/var/run/dpx_rawcook5.lock"
        "/var/run/dpx_rawcook6.lock"
        "/var/run/dpx_rawcook7.lock"
        "/var/run/dpx_post_rawcook1.lock"
        "/var/run/dpx_post_rawcook2.lock"
        "/var/run/dpx_post_rawcook3.lock"
        "/var/run/dpx_post_rawcook4.lock"
        "/var/run/dpx_post_rawcook5.lock"
        "/var/run/dpx_post_rawcook6.lock"
        "/var/run/dpx_post_rawcook7.lock"
        "/var/run/dpx_tar_script1.lock"
        "/var/run/dpx_tar_script2.lock"
        "/var/run/dpx_tar_script3.lock"
        "/var/run/dpx_tar_script4.lock"
        "/var/run/dpx_tar_script5.lock"
        "/var/run/dpx_tar_script6.lock"
        "/var/run/dpx_tar_script7.lock"
        "/var/run/dpx_check_script1.lock"
        "/var/run/dpx_check_script2.lock"
        "/var/run/dpx_check_script3.lock"
        "/var/run/dpx_check_script4.lock"
        "/var/run/dpx_check_script5.lock"
        "/var/run/dpx_check_script6.lock"
        "/var/run/dpx_check_script7.lock"
)

for lock in "${LOCKS[@]}" ; do
    if [[ -f "$lock" ]];then
        true
    else
        sudo touch "$lock"
    fi
done
