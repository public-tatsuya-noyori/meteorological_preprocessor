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
delete_4Search() {
  hour_pattern=`date -u "+%Y%m%d%H"`
  hour_count=1
  while test ${hour_count} -le ${hour_ago}; do
    hour_pattern="${hour_pattern}|"`date -u "+%Y%m%d%H" -d "${hour_count} hour ago"`
    hour_pattern="${hour_pattern}|"`date -u "+%Y%m%d%H" -d "${hour_count} hour ago"`
    hour_count=`expr 1 + ${hour_count}`
  done
  rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/ | grep -v -E "^(${hour_pattern})/$" > ${work_directory}/${priority}_old_directory.tmp
  if test -s ${work_directory}/${priority}_old_directory.tmp; then
    for old_directory in `cat ${work_directory}/${priority}_old_directory.tmp`; do
      rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${old_directory} | sed -e "s|^|/${search_index_directory}/${priority}/${old_directory}|g" > ${work_directory}/${priority}_old_index.tmp
      rclone copy --contimeout ${timeout} --files-from-raw ${work_directory}/${priority}_old_index.tmp --ignore-checksum --local-no-set-modtime --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} ${rclone_remote_bucket} ${work_directory}
      ls -1 ${work_directory}/${search_index_directory}/${priority}/${old_directory}/* | xargs -r cat > ${work_directory}/${priority}_old_file.tmp
      if test -s ${work_directory}/${priority}_old_file.tmp; then
        rclone delete --contimeout ${timeout} --files-from-raw ${work_directory}/${priority}_old_file.tmp --low-level-retries 3 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}
        rm -f ${work_directory}/${priority}_old_file.tmp
      fi
      rclone delete --contimeout ${timeout} --files-from-raw ${work_directory}/${priority}_old_index.tmp --low-level-retries 3 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}
      rm -f ${work_directory}/${priority}_old_index.tmp
      rm -rf ${work_directory}/${search_index_directory}/${priority}/${old_directory}
    done
    rm -f ${work_directory}/${priority}_old_directory.tmp
  fi
}
cron=0
job_directory=4Del_4Search
hour_ago=24
search_index_directory=4Search
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--cron" ) cron=1;shift;;
    "--help" ) echo "$0 [--cron] local_work_directory unique_job_name rclone_remote_bucket priority"; exit 0;;
  esac
done
if test -z $4; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
rclone_remote_bucket=$3
set +e
priority=`echo $4 | grep "^p[1-9]$"`
set -e
if test -z ${priority}; then
  echo "ERROR: $4 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
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
    delete_4Search &
    pid=$!
    echo ${pid} > ${work_directory}/pid.txt
    wait ${pid}
  fi
else
  delete_4Search
fi
