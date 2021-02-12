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
publish(){
  exit_code=255
  if test ${wildcard_index} -eq 1; then
    grep ^${local_work_directory}/ ${input_index_file} | sed -e "s|^${local_work_directory}/|/|g" | xargs -r -n 1 dirname | sort -u | sed -e 's|$|/*|g' > ${work_directory}/newly_created_file.tmp
  else
    grep ^${local_work_directory}/ ${input_index_file} | sed -e "s|^${local_work_directory}/|/|g" > ${work_directory}/newly_created_file.tmp
  fi
  if test -s ${work_directory}/newly_created_file.tmp; then
    cp /dev/null ${work_directory}/err_log.tmp
    for destination_rclone_remote_bucket in `echo ${destination_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
      set +e
      rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${destination_rclone_remote_bucket}/${pubsub_index_directory} > /dev/null
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
    set +e
    grep -v -F -f ${work_directory}/all_processed_file.txt ${work_directory}/newly_created_file.tmp > ${work_directory}/filtered_newly_created_file.tmp
    set -e
    cp /dev/null ${work_directory}/info_log.tmp
    set +e
    rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --checksum --contimeout ${timeout} --cutoff-mode=cautious ${file_from_option} ${work_directory}/filtered_newly_created_file.tmp --immutable --log-file ${work_directory}/info_log.tmp --log-level DEBUG --low-level-retries 3 --no-traverse --retries 3 --s3-chunk-size ${cutoff} --s3-upload-concurrency ${parallel} --stats 0 --timeout ${timeout} --transfers ${parallel} ${local_work_directory} ${destination_rclone_remote_bucket}
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      set +e
      grep -F ERROR ${work_directory}/info_log.tmp >&2
      set -e
      echo "ERROR: can not put to ${destination_rclone_remote_bucket} ${priority}." >&2
      return ${exit_code}
    fi
    set +e
    grep -E "^(.* DEBUG *: *[^ ]* *:.* Unchanged skipping.*|.* INFO *: *[^ ]* *:.* Copied .*)$" ${work_directory}/info_log.tmp | sed -e "s|^.* DEBUG *: *\([^ ]*\) *:.* Unchanged skipping.*$|/\1|g" -e "s|^.* INFO *: *\([^ ]*\) *:.* Copied .*$|/\1|g" | grep -v '^ *$' > ${work_directory}/processed_file.txt
    set -e
    if test -s ${work_directory}/processed_file.txt; then
      for retry_count in `seq ${retry_num}`; do
        now=`date -u "+%Y%m%d%H%M%S"`
        set +e
        rclone copyto --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${work_directory}/processed_file.txt ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${now}.txt
        exit_code=$?
        set -e
        if test ${exit_code} -eq 0; then
          cp /dev/null ${work_directory}/err_log.tmp
          cp ${work_directory}/processed_file.txt ${work_directory}/processed/${now}.txt
          break
        else
          sleep 1
        fi
      done
      if test ${exit_code} -ne 0; then
        cat ${work_directory}/err_log.tmp >&2
        echo "ERROR: can not put ${now}.txt on ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${priority}/." >&2
        return ${exit_code}
      fi
    fi
    ls -1 ${work_directory}/processed/* | grep -v -F "${work_directory}/processed/dummy.tmp" | grep -v -E "^${work_directory}/processed/(${delete_index_date_hour_pattern})[0-9][0-9][0-9][0-9]\.txt$" | xargs -r rm -f
    ls -1 ${work_directory}/processed/* | xargs -r cat > ${work_directory}/all_processed_file.txt
  else
    echo "ERROR: can not match ^${local_work_directory}/ on ${input_index_file}." >&2
    return 199
  fi
  if test ${exit_code} -eq 0 -a ${rm_input_index_file} -eq 1; then
    rm -f ${input_index_file}
  fi
  return ${exit_code}
}
bandwidth_limit_k_bytes_per_s=0
cron=0
cutoff=16M
datetime=`date -u "+%Y%m%d%H%M%S"`
datetime_date=`echo ${datetime} | cut -c1-8`
datetime_hour=`echo ${datetime} | cut -c9-10`
delete_index_date_hour_pattern=${datetime_date}${datetime_hour}
delete_index_hour=23
for hour_count in `seq ${delete_index_hour}`; do
  delete_index_date_hour_pattern="${delete_index_date_hour_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour ago" "+%Y%m%d%H"`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour" "+%Y%m%d%H"`
done
file_from_option=--files-from-raw
job_directory=4Pub
pubsub_index_directory=4PubSub
retry_num=8
rm_input_index_file=0
timeout=8s
wildcard_index=0
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--cron" ) cron=1;shift;;
    "--debug_shell" ) set -evx;shift;;
    "--help" ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--cron] [--debug_shell] [--rm_input_index_file] [--wildcard_index] local_work_directory unique_job_name input_index_file 'destination_rclone_remote_bucket_main[;destination_rclone_remote_bucket_sub]' priority parallel"; exit 0;;
    "--rm_input_index_file" ) rm_input_index_file=1;shift;;
    "--wildcard_index" ) wildcard_index=1;file_from_option=--include-from;shift;;
  esac
done
if test -z $6; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
input_index_file=$3
if test ! -s ${input_index_file}; then
  echo "ERROR: ${input_index_file} is not a file or empty." >&2
  exit 199
fi
set +e
destination_rclone_remote_bucket_main_sub=`echo $4 | grep -F ':'`
priority=`echo $5 | grep "^p[1-9]$"`
parallel=`echo $6 | grep "^[0-9]\+$"`
set -e
if test -z "${destination_rclone_remote_bucket_main_sub}"; then
  echo "ERROR: $4 is not rclone_remote:bucket." >&2
  exit 199
fi
if test -z "${priority}"; then
  echo "ERROR: $5 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
if test -z "${parallel}"; then
  echo "ERROR: $6 is not integer." >&2
  exit 199
elif test $6 -le 0; then
  echo "ERROR: $6 is not more than 1." >&2
  exit 199
fi
work_directory=${local_work_directory}/${job_directory}/${unique_job_name}/${priority}
mkdir -p ${work_directory}/processed
cp /dev/null ${work_directory}/processed/dummy.tmp
touch ${work_directory}/all_processed_file.txt
if test ${cron} -eq 1; then
  if test -s ${work_directory}/pid.txt; then
    running=`cat ${work_directory}/pid.txt | xargs -r ps ho "pid comm args" | grep -F " $0 " | grep -F " ${unique_job_name} " | grep -F " ${priority} " | wc -l`
  else
    running=0
  fi
  if test ${running} -eq 0; then
    publish &
    pid=$!
    echo ${pid} > ${work_directory}/pid.txt
    wait ${pid}
  fi
else
  publish
fi
