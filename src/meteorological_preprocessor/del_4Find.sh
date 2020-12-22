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
del_4Find() {
  rclone --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --stats 0 --timeout ${timeout} --quiet lsf --min-age ${hours_ago}h --max-depth 1 ${rclone_remote}:${bucket}/${find_directory}/${priority} > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.old
  if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.old; then
    sed -e "s|^|/${find_directory}/${priority}/|g" ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.old > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.old.tmp
    mv ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.old.tmp ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.old 
    rclone --checkers ${parallel} --transfers ${parallel} --no-check-dest --quiet --ignore-checksum --contimeout ${timeout} --local-no-set-modtime --low-level-retries 3 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} copy --files-from-raw ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.old ${rclone_remote}:${bucket} ${local_work_directory}/${job_directory}/${unique_job_name}
    ls -1 ${local_work_directory}/${job_directory}/${unique_job_name}/${find_directory}/${priority}/* | xargs cat > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_old_files.tmp
    if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_old_files.tmp; then
      rclone --checkers ${parallel} --transfers ${parallel} --quiet --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --stats 0 --timeout ${timeout} delete --files-from-raw ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_old_files.tmp --rmdirs ${rclone_remote}:${bucket}
      rclone --checkers ${parallel} --transfers ${parallel} --quiet --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --stats 0 --timeout ${timeout} delete --files-from-raw ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.old --rmdirs ${rclone_remote}:${bucket}
      rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_old_files.tmp
    fi
    rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.old
  fi
}
find_directory=4Find
job_directory=4Del_4Find
timeout=8s
retry_num=8
cron=0
for arg in "$@"; do
  case "${arg}" in
    "--help" ) echo "$0 local_work_directory unique_job_name rclone_remote bucket priority hours_ago parallel"; exit 0;;
    "--cron" ) cron=1;shift;;
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
priority=`echo $5 | grep "^p[0-9]$"`
hours_ago=$6
parallel=`echo $7 | grep '^[0-9]\+$'`
set -e
if test -z ${priority}; then
  echo "ERROR: $5 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
if test -z "${parallel}"; then
  echo "ERROR: $7 is not integer." >&2
  exit 199
elif test $7 -le 0; then
  echo "ERROR: $7 is not more than 1." >&2
  exit 199
fi
if test ${cron} -eq 1; then
  if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt; then
    running=`cat ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt | xargs ps ho "pid comm args" | grep " $0 " | grep " ${unique_job_name} " | wc -l`
  else
    mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
    running=0
  fi
  if test ${running} -eq 0; then
    del_4Find &
    pid=$!
    echo ${pid} > ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt
    wait ${pid}
  fi
else
  mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
  del_4Find
fi
