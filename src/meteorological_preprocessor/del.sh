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
delete() {
  rclone --transfers ${parallel} --quiet --contimeout ${timeout} --low-level-retries 3 --retries 1 --size-only --stats 0 --timeout ${timeout} delete --min-age "${hours_ago}h" --rmdirs ${rclone_remote}:${bucket_directory}
}
job_directory=4Del
timeout=10s
retry_num=8
cron=0
for arg in "$@"; do
  case "${arg}" in
    "--help" ) echo "$0 local_work_directory unique_job_name rclone_remote bucket/directory hours_ago parallel"; exit 0;;
    "--cron" ) cron=1;shift;;
  esac
done
if test -z $4; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
rclone_remote=$3
bucket_directory=$4
hours_ago=$5
parallel=$6
if test ${cron} -eq 1; then
  if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt; then
    running=`cat ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt | xargs ps -f --no-headers | grep " $0 " | grep " ${unique_job_name} " | wc -l`
  else
    mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
    running=0
  fi
  if test ${running} -eq 0; then
    delete &
    pid=$!
    echo ${pid} > ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt
    wait ${pid}
  fi
else
  mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
  delete
fi
