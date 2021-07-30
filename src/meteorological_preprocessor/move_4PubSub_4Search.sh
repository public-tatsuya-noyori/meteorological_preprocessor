#!/bin/bash
#
# Copyright 2020-2021 Japan Meteorological Agency.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
#   Tatsuya Noyori - Japan Meteorological Agency - https://www.jma.go.jp
#
set -e
IFS=$'\n'
move_4PubSub_4Search() {
  cp /dev/null ${work_directory}/err_log.tmp
  set +e
  timeout -k 3 ${rclone_timeout} rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}/ > ${work_directory}/${pubsub_index_directory}_index.tmp
  exit_code=$?
  set -e
  if test ${exit_code} -ne 0; then
    cat ${work_directory}/err_log.tmp >&2
    echo "ERROR: can not get index file list from ${rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}." >&2
    return ${exit_code}
  fi
  for index_file in `head -n -1 ${work_directory}/${pubsub_index_directory}_index.tmp | grep -v -E "^(${move_index_date_hour_minute_pattern})[0-9][0-9]_.*\.txt\.gz$"`; do
    index_file_date_hour=`echo ${index_file} | cut -c1-10`
    index_file_minute_second_extension=`echo ${index_file} | cut -c11-`
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 ${rclone_timeout} rclone moveto --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}/${index_file} ${rclone_remote_bucket}/${search_index_directory}/${txt_or_bin}/${index_file_date_hour}/${index_file_minute_second_extension}
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      cat ${work_directory}/err_log.tmp >&2
      echo "ERROR: can not move ${rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}/${index_file} ${rclone_remote_bucket}/${search_index_directory}/${txt_or_bin}/${index_file_date_hour}/${index_file_minute_second_extension}." >&2
      return ${exit_code}
    fi
  done
  index_file_count=`grep -E '^[0-9]{14}_.*\.txt\.gz$' ${work_directory}/${pubsub_index_directory}_index.tmp | wc -l`
  if test ${index_file_count} -eq 1; then
    touch_index_file=`grep -E '^[0-9]{14}_.*\.txt\.gz$' ${work_directory}/${pubsub_index_directory}_index.tmp`
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 ${rclone_timeout} rclone touch --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}/${touch_index_file}
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      cat ${work_directory}/err_log.tmp >&2
      echo "ERROR: can not touch ${rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}/${touch_index_file}." >&2
      return ${exit_code}
    fi
  fi
  return ${exit_code}
}
datetime=`date -u "+%Y%m%d%H%M%S"`
datetime_date=`echo ${datetime} | cut -c1-8`
datetime_hour=`echo ${datetime} | cut -c9-10`
datetime_minute=`echo ${datetime} | cut -c11-12`
bandwidth_limit_k_bytes_per_s=0
job_directory=4Move
move_index_date_hour_minute_pattern=${datetime_date}${datetime_hour}${datetime_minute}
move_index_minute=1
for minute_count in `seq ${move_index_minute}`; do
  move_index_date_hour_minute_pattern="${move_index_date_hour_minute_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:${datetime_minute} ${minute_count} minute ago" "+%Y%m%d%H%M"`"|"`date -u -d "${datetime_date} ${datetime_hour}:${datetime_minute} ${minute_count} minute" "+%Y%m%d%H%M"`
done
pubsub_index_directory=4PubSub
rclone_timeout=180
search_index_directory=4Search
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--help" ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--timeout rclone_timeout] local_work_directory_open_or_closed unique_center_id_main_or_sub txt_or_bin rclone_remote_bucket"; exit 0;;
    "--timeout" ) rclone_timeout=$2;set +e;rclone_timeout=`expr 0 + ${rclone_timeout}`;set -e;shift;shift;;
  esac
done
if test -z $4; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory_open_or_closed=$1
unique_center_id_main_or_sub=$2
set +e
txt_or_bin=`echo $3 | grep -E '^(txt|bin)$'`
rclone_remote_bucket=`echo $4 | grep -F ':'`
set -e
if test -z ${txt_or_bin}; then
  echo "ERROR: $3 is not txt or bin." >&2
  exit 199
fi
if test -z "${rclone_remote_bucket}"; then
  echo "ERROR: $4 is not rclone_remote:bucket." >&2
  exit 199
fi
work_directory=${local_work_directory_open_or_closed}/${job_directory}/${unique_center_id_main_or_sub}/${txt_or_bin}
mkdir -p ${work_directory}
sleep 30
if test -s ${work_directory}/pid.txt; then
  running=`cat ${work_directory}/pid.txt | xargs -r ps ho 'pid comm args' | grep -F " $0 " | grep -F " ${unique_center_id_main_or_sub} " | grep -F " ${txt_or_bin} " | wc -l`
else
  running=0
fi
if test ${running} -eq 0; then
  move_4PubSub_4Search &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  wait ${pid}
fi
