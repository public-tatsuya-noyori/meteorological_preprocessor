#!/bin/sh
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
watch(){
  while :; do
    running=`ps ho 'pid' ${pid} | wc -l`
    if test ${running} -eq 0; then
      break
    fi
    for rclone_pid_etimes_comm in `ps --ppid ${pid} ho 'pid etimes comm' | sed -e 's|  *| |g' -e 's|^ ||g' | grep rclone$`; do
      rclone_pid=`echo ${rclone_pid_etimes_comm} | cut -d' ' -f1`
      etimes=`echo ${rclone_pid_etimes_comm} | cut -d' ' -f2`
      set +e
      etimes=`expr 0 + ${etimes}`
      set -e
      if test ${etimes} -gt ${rclone_watch_seconds}; then
        set +e
        kill ${rclone_pid}
        set -e
        echo "Error: killed rclone pid=${rclone_pid}" >&2
      fi
    done
    sleep 1
  done
}

delete_4Search() {
  cp /dev/null ${work_directory}/err_log.tmp
  set +e
  rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/ > ${work_directory}/${search_index_directory}_date_hour_slash_directory.tmp
  exit_code=$?
  set -e
  if test ${exit_code} -eq 0; then
    cp /dev/null ${work_directory}/err_log.tmp
  else
    cat ${work_directory}/err_log.tmp >&2
    echo "ERROR: can not get index directory list from ${rclone_remote_bucket}/${search_index_directory}/${priority}." >&2
    return ${exit_code}
  fi
  if test -s ${work_directory}/${search_index_directory}_date_hour_slash_directory.tmp; then
    rm -rf ${work_directory}/${search_index_directory}/${priority}
    for date_hour_directory in `grep -v -E "^(${delete_index_date_hour_pattern})/$" ${work_directory}/${search_index_directory}_date_hour_slash_directory.tmp | sed -e 's|/$||g'`; do
      set +e
      rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${date_hour_directory}/ | sed -e "s|^|/${search_index_directory}/${priority}/${date_hour_directory}/|g" > ${work_directory}/${search_index_directory}_index.tmp
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        cp /dev/null ${work_directory}/err_log.tmp
      else
        cat ${work_directory}/err_log.tmp >&2
        echo "ERROR: can not get index file list from ${rclone_remote_bucket}/${search_index_directory}/${priority}/${date_hour_directory}." >&2
        return ${exit_code}
      fi
      if test -s ${work_directory}/${search_index_directory}_index.tmp; then
        set +e
        rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --files-from-raw ${work_directory}/${search_index_directory}_index.tmp --local-no-set-modtime --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${rclone_remote_bucket} ${work_directory}
        exit_code=$?
        set -e
        if test ${exit_code} -eq 0; then
          cp /dev/null ${work_directory}/err_log.tmp
        else
          cat ${work_directory}/err_log.tmp >&2
          echo "ERROR: can not get index file from ${rclone_remote_bucket}/${search_index_directory}/${priority}." >&2
          return ${exit_code}
        fi
        ls -1 ${work_directory}/${search_index_directory}/${priority}/${date_hour_directory}/* | xargs -r zcat > ${work_directory}/${search_index_directory}_file.tmp
        if test -s ${work_directory}/${search_index_directory}_file.tmp; then
          set +e
          rclone delete --contimeout ${timeout} --files-from-raw ${work_directory}/${search_index_directory}_file.tmp --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${rclone_remote_bucket}
          exit_code=$?
          set -e
          if test ${exit_code} -eq 0; then
            cp /dev/null ${work_directory}/err_log.tmp
          else
            cat ${work_directory}/err_log.tmp >&2
            echo "ERROR: can not delete file on ${rclone_remote_bucket}." >&2
            return ${exit_code}
          fi
        fi
        set +e
        rclone delete --contimeout ${timeout} --files-from-raw ${work_directory}/${search_index_directory}_index.tmp --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${rclone_remote_bucket}
        exit_code=$?
        set -e
        if test ${exit_code} -eq 0; then
          cp /dev/null ${work_directory}/err_log.tmp
        else
          cat ${work_directory}/err_log.tmp >&2
          echo "ERROR: can not delete index file on ${rclone_remote_bucket}/${search_index_directory}/${priority}." >&2
          return ${exit_code}
        fi
      fi
    done
  fi
  return ${exit_code}
}
bandwidth_limit_k_bytes_per_s=0
datetime=`date -u "+%Y%m%d%H%M%S"`
datetime_date=`echo ${datetime} | cut -c1-8`
datetime_hour=`echo ${datetime} | cut -c9-10`
delete_index_date_hour_pattern=${datetime_date}${datetime_hour}
delete_index_hour=23
for hour_count in `seq ${delete_index_hour}`; do
  delete_index_date_hour_pattern="${delete_index_date_hour_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour ago" "+%Y%m%d%H"`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour" "+%Y%m%d%H"`
done
job_directory=4Del_4Search
rclone_watch_seconds=3600
search_index_directory=4Search
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--debug_shell" ) set -evx;shift;;
    "--help" ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--debug_shell] [--watch rclone_watch_seconds] local_work_directory unique_job_name priority rclone_remote_bucket"; exit 0;;
    "--watch" ) rclone_watch_seconds=$2;set +e;rclone_watch_seconds=`expr 0 + ${rclone_watch_seconds}`;set -e;shift;shift;;
  esac
done
if test -z $4; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
set +e
priority=`echo $3 | grep "^p[1-9]$"`
rclone_remote_bucket=`echo $4 | grep -F ':'`
set -e
if test -z ${priority}; then
  echo "ERROR: $3 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
if test -z "${rclone_remote_bucket}"; then
  echo "ERROR: $4 is not rclone_remote:bucket." >&2
  exit 199
fi
work_directory=${local_work_directory}/${job_directory}/${unique_job_name}/${priority}
mkdir -p ${work_directory}
if test -s ${work_directory}/pid.txt; then
  running=`cat ${work_directory}/pid.txt | xargs -r ps ho 'pid comm args' | grep -F " $0 " | grep -F " ${unique_job_name} " | grep -F " ${priority} " | wc -l`
else
  running=0
fi
if test ${running} -eq 0; then
  delete_4Search &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  watch &
  wait ${pid}
fi
