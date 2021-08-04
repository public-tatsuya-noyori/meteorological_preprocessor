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
publish(){
  grep ^${local_work_directory_open_or_closed}/ ${input_index_file} | sed -e "s|^${local_work_directory_open_or_closed}/||g" | sort -u > ${work_directory}/newly_created_file.tmp
  if test ! -s ${work_directory}/newly_created_file.tmp; then
    echo "ERROR: can not match ^${local_work_directory_open_or_closed}/ on ${input_index_file}." >&2
    return 199
  fi
  for destination_rclone_remote_bucket in `echo ${destination_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 ${rclone_timeout} rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${destination_rclone_remote_bucket}/${pubsub_index_directory} > /dev/null
    exit_code=$?
    set -e
    if test ${exit_code} -eq 0; then
      break
    else
      cat ${work_directory}/err_log.tmp >&2
      echo "WARNING: can not access on ${destination_rclone_remote_bucket}." >&2
    fi
  done
  if test ${exit_code} -ne 0; then
    echo "ERROR: can not access on ${destination_rclone_remote_bucket_main_sub}." >&2
    return ${exit_code}
  fi
  cp /dev/null ${work_directory}/all_processed_file.txt
  cp /dev/null ${work_directory}/filtered_newly_created_file.tmp
  set +e
  ls -1 ${processed_directory} | sed -e "s|^|${processed_directory}/|g" | xargs -r cat >> ${work_directory}/all_processed_file.txt 2>/dev/null
  grep -v -F -f ${work_directory}/all_processed_file.txt ${work_directory}/newly_created_file.tmp > ${work_directory}/filtered_newly_created_file.tmp
  set -e
  cp /dev/null ${work_directory}/processed_file.txt
  if test -s ${work_directory}/filtered_newly_created_file.tmp; then
    cp /dev/null ${work_directory}/info_log.tmp
    set +e
    timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --files-from-raw ${work_directory}/filtered_newly_created_file.tmp --log-file ${work_directory}/info_log.tmp --log-level DEBUG --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${local_work_directory_open_or_closed} ${destination_rclone_remote_bucket}
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      set +e
      grep -F ERROR ${work_directory}/info_log.tmp >&2
      set -e
      echo "ERROR: can not put to ${destination_rclone_remote_bucket} ${txt_or_bin}." >&2
      return ${exit_code}
    fi
    set +e
    grep -E "^(.* DEBUG *: *[^ ]* *:.* Unchanged skipping.*|.* INFO *: *[^ ]* *:.* Copied .*)$" ${work_directory}/info_log.tmp | sed -e "s|^.* DEBUG *: *\([^ ]*\) *:.* Unchanged skipping.*$|/\1|g" -e "s|^.* INFO *: *\([^ ]*\) *:.* Copied .*$|/\1|g" -e 's|^/||g' | grep -v '^ *$' | sort -u > ${work_directory}/processed_file.txt
    set -e
  fi
  if test -s ${work_directory}/processed_file.txt; then
    for retry_count in `seq ${retry_num}`; do
      cp /dev/null ${work_directory}/err_log.tmp
      rm -rf ${work_directory}/prepare
      mkdir ${work_directory}/prepare
      now=`date -u "+%Y%m%d%H%M%S"`
      cp ${work_directory}/processed_file.txt ${work_directory}/prepare/${now}_${unique_center_id}.txt
      gzip -f ${work_directory}/prepare/${now}_${unique_center_id}.txt
      set +e
      timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/err_log.tmp --low-level-retries 1 --no-traverse --quiet --retries 1 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} ${work_directory}/prepare/ ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}/
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        mv ${work_directory}/processed_file.txt ${processed_directory}/${now}_${unique_center_id}.txt
        break
      else
        sleep 1
      fi
    done
    if test ${exit_code} -ne 0; then
      cat ${work_directory}/err_log.tmp >&2
      echo "ERROR: can not put ${now}.txt on ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}/." >&2
      return ${exit_code}
    fi
  fi
  if test ${exit_code} -eq 0; then
    find ${processed_directory} -regextype posix-egrep -regex "^${processed_directory}/[0-9]{14}_${unique_center_id}\.txt$" -type f -mmin +${delete_index_minute} | xargs -r rm -f
    if test ${delete_input_index_file} -eq 1; then
      rm -f ${input_index_file}
    fi
  fi
  return ${exit_code}
}
bandwidth_limit_k_bytes_per_s=0
delete_index_minute=360
delete_input_index_file=0
job_directory=4PubClone
parallel=4
pubsub_index_directory=4PubSub
rclone_timeout=480
retry_num=16
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--delete_index_minute" ) delete_index_minute=$2;shift;shift;;
    "--delete_input_index_file" ) delete_input_index_file=1;shift;;
    "--help" ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--delete_index_minute delete_index_minute] [--delete_input_index_file] [--parallel the_number_of_parallel_transfer] [--timeout rclone_timeout] local_work_directory_open_or_closed unique_center_id txt_or_bin input_index_file 'destination_rclone_remote_bucket_main[;destination_rclone_remote_bucket_sub]'"; exit 0;;
    "--parallel" ) parallel=$2;shift;shift;;
    "--timeout" ) rclone_timeout=$2;shift;shift;;
  esac
done
if test -z $5; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory_open_or_closed=$1
unique_center_id=$2
set +e
txt_or_bin=`echo $3 | grep -E '^(txt|bin)$'`
input_index_file=$4
destination_rclone_remote_bucket_main_sub=`echo $5 | grep -F ':'`
set -e
if test -z "${txt_or_bin}"; then
  echo "ERROR: $3 is not txt or bin." >&2
  exit 199
fi
if test ! -s ${input_index_file}; then
  echo "ERROR: $4 is not a file or empty." >&2
  exit 199
fi
if test -z "${destination_rclone_remote_bucket_main_sub}"; then
  echo "ERROR: $5 is not rclone_remote:bucket." >&2
  exit 199
fi
work_directory=${local_work_directory_open_or_closed}/${job_directory}/${unique_center_id}/${txt_or_bin}
processed_directory=${local_work_directory_open_or_closed}/${job_directory}/processed/${txt_or_bin}
mkdir -p ${work_directory} ${processed_directory}
touch ${processed_directory}/dummy.tmp
if test -s ${work_directory}/pid.txt; then
  running=`cat ${work_directory}/pid.txt | xargs -r ps ho 'pid comm args' | grep -F " $0 " | grep -F " ${unique_center_id} " | grep -F " ${txt_or_bin} " | wc -l`
else
  running=0
fi
if test ${running} -eq 0; then
  publish &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  wait ${pid}
fi
