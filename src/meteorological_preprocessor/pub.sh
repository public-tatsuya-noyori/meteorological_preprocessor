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
job_datetime=`date -u "+%Y%m%d%H%M%S"`
for arg in "$@"; do
  case "${arg}" in
    '--help' ) echo "$0 unique_job_name raw_list_file local_dir dst_rclone_remote dst_bucket priority_name parallel access"; exit 0;;
  esac
done
if test $# -lt 8; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
unique_job_name=$1
raw_list_file=$2
if test ! -f "${raw_list_file}"; then
  echo "ERROR: ${raw_list_file} is not a file." >&2
  exit 199
fi
local_dir=$3
dst_rclone_remote=$4
dst_bucket=$5
set +e
priority_name=`echo "$6" | grep '^p[0-9]$'`
parallel=`echo "$7" | grep '^[0-9]\+$'`
set -e
if test -z "${priority_name}"; then
  echo "ERROR: $6 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
if test -z "${parallel}"; then
  echo "ERROR: $7 is not integer." >&2
  exit 199
elif test $7 -le 0; then
  echo "ERROR: $7 is not more than 1." >&2
  exit 199
fi
access=$8
job_dir=4Pub
mkdir -p ${local_dir}/${access}/${job_dir}/${unique_job_name}
is_running=0 
if test -s ${local_dir}/${access}/${job_dir}/${unique_job_name}/pid.txt; then
  is_running=`cat ${local_dir}/${access}/${job_dir}/${unique_job_name}/pid.txt | xargs ps -f --no-headers | grep "$0 ${unique_job_name}" | wc -l`
fi
if test ${is_running} -eq 0; then
  {
    mkdir -p ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log
    grep "^${local_dir}/${access}/" ${raw_list_file} | sed -e "s|^${local_dir}/${access}/|/|g" > ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp
    if test -s ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp; then
      now=`date -u "+%Y%m%d%H%M%S"`
      rclone --retries 1 --ignore-checksum --update --use-server-modtime --no-gzip-encoding --no-traverse --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level ERROR --log-file ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log copy --files-from-raw ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp ${local_dir}/${access} ${dst_rclone_remote}:${dst_bucket}
      error_count=`grep ERROR ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log | wc -l`
      if test ${error_count} -ne 0; then
        cat ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log >&2
        rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log
        exit 199
      fi
      rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log
      error_count=1
      retry_count=1
      while test ${error_count} -ne 0; do
        now=`date -u "+%Y%m%d%H%M%S"`
        set +e
        rclone --retries 1 --ignore-checksum --immutable --no-traverse --size-only --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log copyto ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp ${dst_rclone_remote}:${dst_bucket}/4PubSub/${priority_name}/${now}.txt
        exit_code=$?
        set -e
        error_count=`grep ERROR ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log | wc -l`
        if test ${exit_code} -ne 0 -o ${error_count} -ne 0; then
          if test ${retry_count} -ge 8; then
       	    cat ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log >&2
            rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log
            exit 199
          else
            sleep 1
          fi
          retry_count=`expr 1 + ${retry_count}`
        fi
        rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log
      done
      rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp
    fi
  } &
  echo $! > ${local_dir}/${access}/${job_dir}/${unique_job_name}/pid.txt
fi
