#!/bin/sh
#
# Copyright 2020 Japan Meteorological Agency.
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
  grep ^${local_work_directory}/ ${list_file} | sed -e "s|^${local_work_directory}/|/|g" > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp
  if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp; then
    rclone --transfers ${parallel} --ignore-existing --quiet --ignore-checksum --contimeout ${timeout} --low-level-retries 1 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} copy --files-from-raw ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp ${local_work_directory} ${dest_rclone_remote}:${dest_bucket}
    exit_code=1
    retry_count=1
    rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_log.tmp
    while test ${exit_code} -ne 0; do
      now=`date -u "+%Y%m%d%H%M%S"`
      set +e
      rclone --immutable --quiet --log-file ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_log.tmp --ignore-checksum --contimeout ${timeout} --low-level-retries 1 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} copyto ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp ${dest_rclone_remote}:${dest_bucket}/${index_directory}/${priority}/${now}.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0 -a ${retry_count} -ge ${retry_num}; then
        cat ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_log.tmp >&2
        exit ${exit_code}
      fi
      retry_count=`expr 1 + ${retry_count}`
    done
    rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_log.tmp
    rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp
  else
    echo "ERROR: can not match ^${local_work_directory}/ on ${list_file}." >&2
    exit 199
  fi
}
index_directory=4PubSub
job_directory=4Pub
timeout=10s
retry_num=8
cron=0
rm_list_file=0
for arg in "$@"; do
  case "${arg}" in
    "--help" ) echo "$0 [--cron] [--rm_list_file] local_work_directory unique_job_name list_file dest_rclone_remote dest_bucket priority parallel"; exit 0;;
    "--cron" ) cron=1;shift;;
    "--rm_list_file" ) rm_list_file=1;shift;;
  esac
done
if test -z $7; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
list_file=$3
if test ! -s ${list_file}; then
  echo "ERROR: ${list_file} is not a file or empty." >&2
  exit 199
fi
dest_rclone_remote=$4
dest_bucket=$5
set +e
priority=`echo $6 | grep "^p[0-9]$"`
parallel=`echo $7 | grep "^[0-9]\+$"`
set -e
if test -z ${priority}; then
  echo "ERROR: $6 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
if test -z ${parallel}; then
  echo "ERROR: $7 is not integer." >&2
  exit 199
elif test $7 -le 0; then
  echo "ERROR: $7 is not more than 1." >&2
  exit 199
fi
if test ${cron} -eq 1; then
  if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt; then
    running=`cat ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt | xargs ps -f --no-headers | grep " $0 " | grep " ${unique_job_name} " | wc -l`
  else
    mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
    running=0
  fi
  if test ${running} -eq 0; then
    publish &
    pid=$!
    echo ${pid} > ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt
    wait ${pid}
    if test ${rm_list_file} -eq 1; then
      rm -f ${list_file}
    fi
  fi
else
  mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
  publish
  if test ${rm_list_file} -eq 1; then
    rm -f ${list_file}
  fi
fi
