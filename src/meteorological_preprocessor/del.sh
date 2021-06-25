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
delete() {
  cp /dev/null ${work_directory}/err_log.tmp
  set +e
  timeout -k 3 ${rclone_timeout} rclone delete --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --min-age ${days_ago}d --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} ${rclone_remote_bucket}
  exit_code=$?
  set -e
  if test ${exit_code} -ne 0; then
    cat ${work_directory}/err_log.tmp >&2
    echo "ERROR: can not delete on ${rclone_remote_bucket}." >&2
  fi
  return ${exit_code}
}
job_directory=4Del
rclone_timeout=43200
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--debug_shell" ) set -evx;shift;;
    "--help" ) echo "$0 [--debug_shell] [--timeout rclone_timeout] local_work_directory unique_job_name rclone_remote_bucket days_ago"; exit 0;;
    "--timeout" ) rclone_timeout=$2;set +e;rclone_timeout=`expr 0 + ${rclone_timeout}`;set -e;shift;shift;;
  esac
done
if test -z $4; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
set +e
rclone_remote_bucket=`echo $3 | grep -F ':'`
days_ago=`echo $4 | grep "^[0-9]\+$"`
set -e
if test -z "${rclone_remote_bucket}"; then
  echo "ERROR: $3 is not rclone_remote:bucket." >&2
  exit 199
fi
if test -z ${days_ago}; then
  echo "ERROR: $4 is not integer." >&2
  exit 199
elif test $4 -le 0; then
  echo "ERROR: $4 is not more than 1." >&2
  exit 199
fi
work_directory=${local_work_directory}/${job_directory}/${unique_job_name}
mkdir -p ${work_directory}
if test -s ${work_directory}/pid.txt; then
  running=`cat ${work_directory}/pid.txt | xargs -r ps ho "pid comm args" | grep -F " $0 " | grep -F " ${unique_job_name} " | wc -l`
else
  running=0
fi
if test ${running} -eq 0; then
  delete &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  wait ${pid}
fi
