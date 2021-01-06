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
  if test ${pub_with_wildcard} -eq 1; then
    grep ^${local_work_directory}/ ${list_file} | sed -e "s|^${local_work_directory}/|/|g" | xargs -r -n 1 dirname | sort -u | sed -e 's|$|/*|g' > ${work_directory}/${priority}_newly_created_index.tmp
  else
    grep ^${local_work_directory}/ ${list_file} | sed -e "s|^${local_work_directory}/|/|g" > ${work_directory}/${priority}_newly_created_index.tmp
  fi
  if test -s ${work_directory}/${priority}_newly_created_index.tmp; then
    if test ${pub_with_wildcard} -eq 1; then
      rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --ignore-checksum --include-from ${work_directory}/${priority}_newly_created_index.tmp --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 1 --s3-upload-concurrency ${parallel} --s3-chunk-size ${cutoff} --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${local_work_directory} ${dest_rclone_remote}:${dest_bucket}
    else
      rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/${priority}_newly_created_index.tmp --ignore-checksum --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 1 --s3-upload-concurrency ${parallel} --s3-chunk-size ${cutoff} --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${local_work_directory} ${dest_rclone_remote}:${dest_bucket}
    fi
    exit_code=1
    retry_count=1
    rm -f ${work_directory}/${priority}_log.tmp
    while test ${exit_code} -ne 0; do
      now=`date -u "+%Y%m%d%H%M%S"`
      set +e
      rclone copyto --contimeout ${timeout} --ignore-checksum --immutable --log-file ${work_directory}/${priority}_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} ${work_directory}/${priority}_newly_created_index.tmp ${dest_rclone_remote}:${dest_bucket}/${pubsub_index_directory}/${priority}/${now}.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0 -a ${retry_count} -ge ${retry_num}; then
        cat ${work_directory}/${priority}_log.tmp >&2
        exit ${exit_code}
      fi
      retry_count=`expr 1 + ${retry_count}`
    done
    rm -f ${work_directory}/${priority}_log.tmp
    rm -f ${work_directory}/${priority}_newly_created_index.tmp
  else
    echo "ERROR: can not match ^${local_work_directory}/ on ${list_file}." >&2
    exit 199
  fi
}
cron=0
cutoff=16M
job_directory=4Pub
pub_with_wildcard=0
pubsub_index_directory=4PubSub
retry_num=8
rm_list_file=0
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--cron" ) cron=1;shift;;
    "--help" ) echo "$0 [--cron] [--pub_with_wildcard] [--rm_list_file] local_work_directory unique_job_name list_file dest_rclone_remote dest_bucket priority parallel"; exit 0;;
    "--pub_with_wildcard" ) pub_with_wildcard=1;shift;;
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
priority=`echo $6 | grep "^p[1-9]$"`
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
work_directory=${local_work_directory}/${job_directory}/${unique_job_name}
mkdir -p ${work_directory}
if test ${cron} -eq 1; then
  if test -s ${work_directory}/pid.txt; then
    running=`cat ${work_directory}/pid.txt | xargs -r ps ho "pid comm args" | grep " $0 " | grep " ${unique_job_name} " | wc -l`
  else
    running=0
  fi
  if test ${running} -eq 0; then
    publish &
    pid=$!
    echo ${pid} > ${work_directory}/pid.txt
    wait ${pid}
    if test ${rm_list_file} -eq 1; then
      rm -f ${list_file}
    fi
  fi
else
  publish
  if test ${rm_list_file} -eq 1; then
    rm -f ${list_file}
  fi
fi
