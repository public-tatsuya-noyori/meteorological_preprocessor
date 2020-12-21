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
move_4PubSub_4Find() {
  rclone --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --stats 0 --timeout ${timeout} --quiet lsf --min-age ${hours_ago}h --max-depth 1 ${rclone_remote}:${bucket}/${index_directory}/${priority}/ | xargs -n 1 -I {} rclone move ${rclone_remote}:${bucket}/${index_directory}/${priority}/{} ${rclone_remote}:${bucket}/${find_directory}/${priority}/
}
index_directory=4PubSub
find_directory=4Find
job_directory=4Find
timeout=8s
retry_num=8
cron=0
for arg in "$@"; do
  case "${arg}" in
    "--help" ) echo "$0 local_work_directory unique_job_name rclone_remote bucket priority hours_ago"; exit 0;;
    "--cron" ) cron=1;shift;;
  esac
done
if test -z $6; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
rclone_remote=$3
bucket=$4
priority=`echo $5 | grep "^p[0-9]$"`
set -e
if test -z ${priority}; then
  echo "ERROR: $5 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
hours_ago=$6
if test ${cron} -eq 1; then
  if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt; then
    running=`cat ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt | xargs ps ho "pid comm args" | grep " $0 " | grep " ${unique_job_name} " | wc -l`
  else
    mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
    running=0
  fi
  if test ${running} -eq 0; then
    move_4PubSub_4Find &
    pid=$!
    echo ${pid} > ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt
    wait ${pid}
  fi
else
  mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
  move_4PubSub_4Find
fi
