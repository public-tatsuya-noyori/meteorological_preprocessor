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
publish(){
  exit_code=255
  grep ^${local_work_directory}/ ${input_index_file} | sed -e "s|^${local_work_directory}/||g" | sort -u > ${work_directory}/newly_created_file.tmp
  if test ! -s ${work_directory}/newly_created_file.tmp; then
    echo "ERROR: can not match ^${local_work_directory}/ on ${input_index_file}." >&2
    return 199
  fi
  for destination_rclone_remote_bucket in `echo ${destination_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 ${rclone_timeout} rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${destination_rclone_remote_bucket}/${pubsub_index_directory} > /dev/null
    exit_code=$?
    set -e
    if test ${exit_code} -eq 0; then
      cp /dev/null ${work_directory}/err_log.tmp
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
  ls -1 ${processed_directory}/ | sed -e "s|^|${processed_directory}/|g" | xargs -r cat > ${work_directory}/all_processed_file.txt
  set +e
  grep -v -F -f ${work_directory}/all_processed_file.txt ${work_directory}/newly_created_file.tmp > ${work_directory}/filtered_newly_created_file.tmp
  set -e
  cp /dev/null ${work_directory}/info_log.tmp
  set +e
  timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --files-from-raw ${work_directory}/filtered_newly_created_file.tmp --log-file ${work_directory}/info_log.tmp --log-level DEBUG --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${local_work_directory} ${destination_rclone_remote_bucket}
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
  if test -s ${work_directory}/processed_file.txt; then
    for retry_count in `seq ${retry_num}`; do
      cp /dev/null ${work_directory}/err_log.tmp
      rm -rf ${work_directory}/prepare
      mkdir ${work_directory}/prepare
      now=`date -u "+%Y%m%d%H%M%S"`
      cp ${work_directory}/processed_file.txt ${work_directory}/prepare/${now}.txt
      gzip -f ${work_directory}/prepare/${now}.txt
      set +e
      timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} ${work_directory}/prepare/ ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}/
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        mv ${work_directory}/processed_file.txt ${processed_directory}/${unique_job_name}_${now}.txt
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
  if test ${exit_code} -eq 0 -a ${rm_input_index_file} -eq 1; then
    rm -f ${input_index_file}
  fi
  return ${exit_code}
}
bandwidth_limit_k_bytes_per_s=0
datetime=`date -u "+%Y%m%d%H%M%S"`
datetime_date=`echo ${datetime} | cut -c1-8`
datetime_hour=`echo ${datetime} | cut -c9-10`
delete_index_date_hour_pattern=${datetime_date}${datetime_hour}
delete_index_hour=24
for hour_count in `seq ${delete_index_hour}`; do
  delete_index_date_hour_pattern="${delete_index_date_hour_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour ago" "+%Y%m%d%H"`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour" "+%Y%m%d%H"`
done
job_directory=4PubClone
pubsub_index_directory=4PubSub
rclone_timeout=600
retry_num=8
rm_input_index_file=0
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--help" ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--rm_input_index_file] [--timeout rclone_timeout] local_work_directory unique_job_name txt_or_bin input_index_file 'destination_rclone_remote_bucket_main[;destination_rclone_remote_bucket_sub]' parallel"; exit 0;;
    "--rm_input_index_file" ) rm_input_index_file=1;shift;;
    "--timeout" ) rclone_timeout=$2;set +e;rclone_timeout=`expr 0 + ${rclone_timeout}`;set -e;shift;shift;;
  esac
done
if test -z $6; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
set +e
txt_or_bin=`echo $3 | grep -E '^(txt|bin)$'`
input_index_file=$4
destination_rclone_remote_bucket_main_sub=`echo $5 | grep -F ':'`
parallel=`echo $6 | grep "^[0-9]\+$"`
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
if test -z "${parallel}"; then
  echo "ERROR: $6 is not integer." >&2
  exit 199
elif test $6 -le 0; then
  echo "ERROR: $6 is not more than 1." >&2
  exit 199
fi
work_directory=${local_work_directory}/${job_directory}/${unique_job_name}/${txt_or_bin}
processed_directory=${local_work_directory}/${job_directory}/__processed/${txt_or_bin}
mkdir -p ${work_directory} ${processed_directory}
touch ${processed_directory}/dummy.tmp ${work_directory}/all_processed_file.txt
if test -s ${work_directory}/pid.txt; then
  running=`cat ${work_directory}/pid.txt | xargs -r ps ho 'pid comm args' | grep -F " $0 " | grep -F " ${unique_job_name} " | grep -F " ${txt_or_bin} " | wc -l`
else
  running=0
fi
if test ${running} -eq 0; then
  publish &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  wait ${pid}
  ls -1 ${processed_directory}/  | grep -E "^${unique_job_name}_" | grep -v -E "^${unique_job_name}_(${delete_index_date_hour_pattern})[0-9][0-9][0-9][0-9]\.txt$" | sed -e "s|^|${processed_directory}/|g" | xargs -r rm -f
fi
