#!/usr/bin/env python3

"""
Script to manage transcode of downloaded MKV and DPX files
from Black Pearl, but for use by film ops teams (with an aim
to allow them an option that avoids using DPI downloader).

Joanna White
2023
"""

import logging
import os
import re
import shutil
import subprocess
from pathlib import Path

# Global paths from server environmental variables
PATH_POLICY = os.environ["DPX_SCRIPTS"]
PRORES_POLICY = os.path.join(PATH_POLICY, "prores_transcode_check.xml")
LOG = os.environ["LOG_PATH"]
CONTROL_JSON = os.path.join(LOG, "downtime_control.json")
FPATH = os.path.join(os.environ["QNAP_FILMOPS2"], "ProRes_Transcode/")
FAILURES = os.path.join(FPATH, "failure/")
COMPLETED = os.path.join(FPATH, "completed/")

# Setup logging
logger = logging.getLogger("filmops_transcode_prores")
hdlr = logging.FileHandler(os.path.join(LOG, "filmops_transcode_prores.log"))
formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def get_dar(fullpath):
    """
    Retrieves metadata DAR info and returns as string
    """
    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        "--Inform=Video;%DisplayAspectRatio/String%",
        fullpath,
    ]

    dar_setting = subprocess.check_output(cmd)
    dar_setting = dar_setting.decode("utf-8").rstrip("\n")
    print(f"DAR setting: {dar_setting}")

    if "4:3" in str(dar_setting):
        return "4:3"
    if "16:9" in str(dar_setting):
        return "16:9"
    if "15:11" in str(dar_setting):
        return "4:3"
    if "1.85:1" in str(dar_setting):
        return "1.85:1"
    if "2.2:1" in str(dar_setting):
        return "2.2:1"

    return str(dar_setting)


def get_par(fullpath):
    """
    Retrieves metadata PAR info and returns
    Checks if multiples from multi video tracks
    """
    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        "--Inform=Video;%PixelAspectRatio%",
        fullpath,
    ]

    par_setting = subprocess.check_output(cmd)
    par_full = par_setting.decode("utf-8").rstrip("\n")

    if len(par_full) <= 5:
        return par_full
    return par_full[:5]


def get_framerate(fullpath):
    """
    Retrieves metadata framerate info and returns
    """
    cmd = [
        "mediainfo",
        "--Language=raw",
        "-f",
        "--Output=General;%FrameRate%",
        fullpath,
    ]

    framerate = subprocess.check_output(cmd)
    framerate = framerate.decode("utf-8").rstrip("\n")

    if len(framerate) < 1:
        return framerate
    return None


def get_height(fullpath):
    """
    Retrieves height information via mediainfo
    Using sampled height where original
    height and stored height differ (MXF samples)
    """

    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        "--Inform=Video;%Sampled_Height%",
        fullpath,
    ]

    sampled_height = subprocess.check_output(cmd)
    sampled_height = sampled_height.decode("utf-8").rstrip("\n")

    cmd2 = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Video;%Height%"',
        fullpath,
    ]

    cmd2[3] = cmd2[3].replace('"', "")
    reg_height = subprocess.check_output(cmd2)
    reg_height = reg_height.decode("utf-8").rstrip("\n")

    try:
        int(sampled_height)
    except ValueError:
        sampled_height = 0

    if int(sampled_height) and int(reg_height):
        if int(sampled_height) > int(reg_height):
            height = str(sampled_height)
        else:
            height = str(reg_height)
    else:
        height = str(reg_height)

    if height == "480":
        return "480"
    if height == "486":
        return "486"
    if height == "576":
        return "576"
    if height == "608":
        return "608"
    if height == "720":
        return "720"
    if height in ("1080", "1 080"):
        return "1080"

    height = height.split(" pixel", maxsplit=1)[0]
    return re.sub("[^0-9]", "", height)


def get_width(fullpath):
    """
    Retrieves height information using mediainfo
    """
    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Video;%Width/String%"',
        fullpath,
    ]

    cmd[3] = cmd[3].replace('"', "")
    width = subprocess.check_output(cmd)
    width = width.decode("utf-8").rstrip("\n")

    if width == "720":
        return "720"
    if width == "768":
        return "768"
    if width in ("1024", "1 024"):
        return "1024"
    if width in ("1280", "1 280"):
        return "1280"
    if width in ("1920", "1 920"):
        return "1920"
    if width.isdigit():
        return str(width)

    width = width.split(" p", maxsplit=1)[0]
    return re.sub("[^0-9]", "", width)


def get_duration(fullpath):
    """
    Retrieves duration information via mediainfo
    where more than two returned, file longest of
    first two and return video stream info to main
    for update to ffmpeg map command
    """

    cmd = [
        "mediainfo",
        "--Language=raw",
        "--Full",
        '--Inform="Video;%Duration%"',
        fullpath,
    ]

    cmd[3] = cmd[3].replace('"', "")
    duration = subprocess.check_output(cmd)
    duration = duration.decode("utf-8").rstrip("\n")

    if not duration:
        return ("", "")

    print(f"Mediainfo seconds: {duration}")

    if "." in duration:
        duration = duration.split(".")

    if isinstance(duration, str):
        second_duration = int(duration) // 1000
        return (second_duration, "0")
    if len(duration) == 2:
        print("Just one duration returned")
        num = duration[0]
        second_duration = int(num) // 1000
        print(second_duration)
        return (second_duration, "0")
    if len(duration) > 2:
        print("More than one duration returned")
        dur1 = f"{duration[0]}"
        dur2 = f"{duration[1][6:]}"
        print(dur1, dur2)
        if int(dur1) > int(dur2):
            second_duration = int(dur1) // 1000
            return (second_duration, "0")
        if int(dur1) < int(dur2):
            second_duration = int(dur2) // 1000
            return (second_duration, "1")


def make_mov_of_sequence(dpx_folder, fps, output):
    """
    Convert image sequence into full size MOV
    before passing through again to make smalled
    prores mov easier for viewing
    """
    if not fps:
        framerate = "24"
    elif "." in fps:
        framerate = fps.split(".")[0]
    elif fps:
        framerate = fps

    ffmpeg = ["ffmpeg"]

    image_settings = ["-f", "image2"]

    glob_config = ["-pattern_type", "glob"]

    ffmpeg_input = ["-i", f"{dpx_folder}/*.dpx"]

    codec = ["-c:v", "prores_ks", "-profile:v", "3"]

    color_data = [
        "-pix_fmt",
        "yuv422p10le",
        "-vendor",
        "ap10",
        "-movflags",
        "+faststart",
    ]

    fps = ["-r", framerate]

    output_settings = ["-nostdin", "-y", output, "-f", "null", "-"]

    return (
        ffmpeg
        + image_settings
        + glob_config
        + ffmpeg_input
        + codec
        + color_data
        + fps
        + output_settings
    )


def create_ffmpeg_command(fullpath, output, video_data):
    """
    Subprocess command build, with variations
    added based on metadata extraction
    """

    # Build subprocess call from data list
    ffmpeg_program_call = ["ffmpeg"]

    input_video_file = ["-i", fullpath, "-nostdin"]

    # Map video stream that's longest to 0
    if video_data[0]:
        print(f"VS {video_data[0]}")
        map_video = [
            "-map",
            f"0:v:{video_data[0]}",
        ]
    else:
        map_video = [
            "-map",
            "0:v:0",
        ]

    video_settings = ["-c:v", "prores_ks", "-profile:v", "3"]

    prores_build = [
        "-pix_fmt",
        "yuv422p10le",
        "-vendor",
        "ap10",
        "-movflags",
        "+faststart",
    ]

    map_audio = ["-map", "0:a?", "-dn"]

    no_scale_pad = ["-vf", "bwdif=send_frame"]

    sd_16x9 = ["-vf", "yadif,scale=-1:576:flags=lanczos,pad=1024:576:-1:-1"]

    hd_16x9 = ["-vf", "yadif,scale=-1:720:flags=lanczos,pad=1280:720:-1:-1"]

    pillarbox = ["-vf", "yadif,scale=-1:1080:flags=lanczos,pad=1920:1080:-1:-1"]

    letterbox = ["-vf", "yadif,scale=1920:-1:flags=lanczos,pad=1920:1080:-1:-1"]

    output_settings = ["-nostdin", "-y", output, "-f", "null", "-"]

    height = int(video_data[1])
    width = int(video_data[2])
    # Calculate height/width to decide HD scale path
    aspect = round(width / height, 3)
    cmd_mid = []

    if height <= 576:
        cmd_mid = sd_16x9
    elif height <= 720:
        cmd_mid = hd_16x9
    elif height > 720 and aspect < 1.778:
        cmd_mid = pillarbox
    elif height >= 1080 and aspect >= 1.778:
        cmd_mid = letterbox
    else:
        cmd_mid = no_scale_pad
    print(f"Middle command chose: {cmd_mid}")

    return (
        ffmpeg_program_call
        + input_video_file
        + map_video
        + map_audio
        + video_settings
        + cmd_mid
        + prores_build
        + output_settings
    )


def check_policy(output_path):
    """
    Run mediaconch check against new prores
    """
    new_file = os.path.split(output_path)[1]
    if os.path.isfile(output_path):
        logger.info("Conformance check: comparing %s with policy", new_file)
        result = conformance_check(output_path)
        if "PASS!" in result:
            return "pass!"
        return result


def conformance_check(filepath):
    """
    Checks mediaconch policy against new V210 mov
    """

    mediaconch_cmd = ["mediaconch", "--force", "-p", PRORES_POLICY, filepath]

    try:
        success = subprocess.check_output(mediaconch_cmd)
        success = success.decode("utf-8").rstrip("\n")
    except Exception:
        success = ""
        logger.exception("Mediaconch policy retrieval failure for %s", filepath)

    if "N/A!" in success:
        return "FAIL!"
    if "pass!" in success:
        return "PASS!"
    if "fail!" in success:
        return "FAIL!"
    return "FAIL!"


def main():
    """
    Loads folder content of watched folder 'FPATH'. Where 'mkv/mov' found set encode rule.
    Sets up different transcodes for image sequence or MOV/MKV. Transcode to Prores.
    No clean up actions set -- this is manual responsibility.
    """
    logger.info(
        "========= START FILM OPS TRANSCODE TO PRORES ========================="
    )
    folder_content = os.listdir(FPATH)
    logger.info("Contents found: %s", folder_content)
    for film_item in folder_content:
        fullpath = os.path.join(FPATH, film_item)
        if film_item.endswith("prores.mov"):
            continue
        if film_item in ("completed", "failure"):
            continue
        logger.info("** Processing found item: %s", fullpath)
        if os.path.isdir(fullpath):
            encode_rule = "sequence"
        elif film_item.endswith((".mkv", ".MKV")):
            encode_rule = "matroska"
        elif film_item.endswith((".mov", ".MOV")):
            encode_rule = "mov"
        else:
            logger.info("Film item is not sequence or Matroska. Skipping")
            continue
        print(f"Encode rule: {encode_rule}")

        output_fullpath = os.path.join(FPATH, f"{film_item}_prores.mov")
        if os.path.isfile(output_fullpath):
            logger.info("File already transcoded to Prores. Skipping.")
            continue

        if encode_rule == "sequence":
            # Push to genering Prores transcode, then run again.
            output_fullpath = os.path.join(FPATH, f"{film_item}.mov")
            all_files = list(Path(fullpath).rglob("*.dpx"))
            logger.info("** Sequence encoding: %s", film_item)
            first_dpx = os.path.abspath(all_files[0])
            dpx_folder = os.path.split(first_dpx)[0]
            framerate = get_framerate(first_dpx)
            command = make_mov_of_sequence(dpx_folder, framerate, output_fullpath)
            ffmpeg_neat = " ".join(command)
            logger.info(
                "FFmpeg command to create image sequence to MOV: %s", ffmpeg_neat
            )

            exit_code = subprocess.call(command)
            if exit_code == 0:
                logger.info("Subprocess call for FFmpeg command successful")
            elif not os.path.isfile(output_fullpath):
                logger.info(
                    "WARNING: FFmpeg has not created a MOV file %s", output_fullpath
                )
                logger.info(
                    "Deleting failed MOV and leaving sequence in place for repeat encode attempt."
                )
                if os.path.exists(output_fullpath):
                    os.remove(output_fullpath)
                continue
            else:
                logger.info(
                    "WARNING: FFmpeg command failed: %s - Exit code %s",
                    ffmpeg_neat,
                    exit_code,
                )
                logger.info(
                    "Deleting failed MOV and leaving sequence in place for repeat encode attempt."
                )
                if os.path.exists(output_fullpath):
                    os.remove(output_fullpath)
                continue
            logger.info(
                "Image sequence %s converted to MOV file, waiting for second pass %s",
                film_item,
                output_fullpath,
            )
            logger.info("Moving sequence into completed/ path")
            try:
                shutil.move(fullpath, COMPLETED)
            except Exception as err:
                logger.warning("Failed to move source file to completed path")
                print(err)
            continue

        if encode_rule in ("matroska", "mov"):
            # Collect data
            print(fullpath)
            dar = get_dar(fullpath)
            par = get_par(fullpath)
            height = get_height(fullpath)
            width = get_width(fullpath)
            duration, vs = get_duration(fullpath)
            video_data = [vs, height, width]

            logger.info("** Matroska or MOV file: %s", film_item)
            logger.info(
                "DAR %s PAR %s Height %s Width %s Duration %s",
                dar,
                par,
                height,
                width,
                duration,
            )

            # Execute FFmpeg subprocess call
            ffmpeg_call = create_ffmpeg_command(fullpath, output_fullpath, video_data)
            ffmpeg_call_neat = (" ".join(ffmpeg_call), "\n")
            logger.info("FFmpeg call: %s", ffmpeg_call_neat)

            # Create ProRes
            try:
                subprocess.call(ffmpeg_call)
                logger.info("Subprocess call for FFmpeg command successful")
            except Exception as err:
                logger.warning(
                    "WARNING: FFmpeg command failed: %s\n%s", ffmpeg_call_neat, err
                )
                continue

            if not os.path.isfile(output_fullpath):
                logger.info(
                    "WARNING: FFmpeg has not created a ProRes MOV file %s",
                    output_fullpath,
                )
                logger.info(
                    "Deleting failed MOV and leaving source in place for repeat encode attempt."
                )
                if os.path.exists(output_fullpath):
                    os.remove(output_fullpath)
                continue

            pass_policy = check_policy(output_fullpath)
            if pass_policy == "pass!":
                logger.info("New ProRes file passed MediaConch policy")
                logger.info("Moving source file into completed file: %s", fullpath)
                try:
                    shutil.move(fullpath, COMPLETED)
                except Exception as err:
                    logger.warning(
                        "Shutil move failed for paths:\n%s\n%s", fullpath, COMPLETED
                    )
                    print(err)
                    continue
            else:
                logger.warning(
                    "Prores file failed Mediaconch policy: \n%s", pass_policy
                )
                logger.warning(
                    "Deleting ProRes file to allow for repeated attempt: %s",
                    output_fullpath,
                )
                try:
                    shutil.move(fullpath, FAILURES)
                except Exception as err:
                    logger.warning("Failed to move source file to failures path")
                    print(err)
                try:
                    shutil.move(output_fullpath, FAILURES)
                except Exception as err:
                    logger.warning("Failed to move ProRes file to failures path")
                    print(err)

    logger.info(
        "========= END FILM OPS TRANSCODE TO PRORES ==========================="
    )


if __name__ == "__main__":
    main()
