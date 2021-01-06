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
move_4PubSub_4Search() {
  rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --min-age ${minutes_ago}m --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote}:${bucket}/${pubsub_index_directory}/${priority}/ | head -n -1 | sed -e "s/^\([0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]\)[0-9][0-9][0-9][0-9]\.txt$/\1 \0/g" | xargs -r -n 2 -I {} sh -c 'date_hour_directory=`echo {} | cut -d" " -f1`;index_file_to_move=`echo {} | cut -d" " -f2`;rclone move --contimeout ${timeout} --ignore-checksum --low-level-retries 3 --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} ${rclone_remote}:${bucket}/${pubsub_index_directory}/${priority}/${index_file_to_mov
e} ${rclone_remote}:${bucket}/${search_index_directory}/${priority}/${date_hour_directory}/'
}
cron=0
job_directory=4Search
minutes_ago=15
pubsub_index_directory=4PubSub
search_index_directory=4Search
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--cron" ) cron=1;shift;;
    "--help" ) echo "$0 [--cron] local_work_directory unique_job_name rclone_remote bucket priority"; exit 0;;
  esac
done
if test -z $5; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
rclone_remote=$3
bucket=$4
set +e
priority=`echo $5 | grep "^p[1-9]$"`
set -e
if test -z ${priority}; then
  echo "ERROR: $5 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
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
    move_4PubSub_4Search &
    pid=$!
    echo ${pid} > ${work_directory}/pid.txt
    wait ${pid}
  fi
else
  move_4PubSub_4Search
fi
