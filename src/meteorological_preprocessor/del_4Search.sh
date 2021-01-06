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
  rclone lsf --contimeout ${timeout} --low-level-retries 3 --min-age ${hours_ago}h --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote}:${bucket}/${search_index_directory}/${priority} | head -n -1 | sed -e "s|^|/${search_index_directory}/${priority}/|g" > ${work_directory}/${priority}_old_index.tmp
  if test -s ${work_directory}/${priority}_old_index.tmp; then
    rclone copy --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${work_directory}/${priority}_old_index.tmp --ignore-checksum --local-no-set-modtime --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote}:${bucket} ${work_directory}
    set +e
    if test -n "${exclusive_pattern_file}"; then
      ls -1 ${work_directory}/${search_index_directory}/${priority}/* | xargs -r cat | grep -v -E -f ${exclusive_pattern_file} > ${work_directory}/${priority}_old_file.tmp
    else
      ls -1 ${work_directory}/${search_index_directory}/${priority}/* | xargs -r cat > ${work_directory}/${priority}_old_file.tmp
    fi
    set -e
    if test -s ${work_directory}/${priority}_old_file.tmp; then
      set +e
      rclone delete --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${work_directory}/${priority}_old_file.tmp --low-level-retries 3 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote}:${bucket}
      set -e
      rm -f ${work_directory}/${priority}_old_file.tmp
    fi
    rclone delete --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${work_directory}/${priority}_old_index.tmp --low-level-retries 3 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote}:${bucket}
    rm -f ${work_directory}/${priority}_old_index.tmp
  fi
}
cron=0
job_directory=4Del_4Search
search_index_directory=4Search
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--cron" ) cron=1;shift;;
    "--help" ) echo "$0 [--cron] local_work_directory unique_job_name rclone_remote bucket priority hours_ago parallel [exclusive_pattern_file]"; exit 0;;
  esac
done
if test -z $7; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
rclone_remote=$3
bucket=$4
set +e
priority=`echo $5 | grep "^p[1-9]$"`
hours_ago=`echo $6 | grep '^[0-9]\+$'`
parallel=`echo $7 | grep '^[0-9]\+$'`
set -e
if test -z ${priority}; then
  echo "ERROR: $5 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
if test -z "${hours_ago}"; then
  echo "ERROR: $6 is not integer." >&2
  exit 199
elif test $6 -le 0; then
  echo "ERROR: $6 is not more than 1." >&2
  exit 199
fi
if test -z "${parallel}"; then
  echo "ERROR: $7 is not integer." >&2
  exit 199
elif test $7 -le 0; then
  echo "ERROR: $7 is not more than 1." >&2
  exit 199
fi
exclusive_pattern_file=''
if test -n $8; then
  exclusive_pattern_file=$8
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
