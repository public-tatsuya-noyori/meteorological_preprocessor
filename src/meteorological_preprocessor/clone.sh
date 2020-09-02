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
clone() {
  job_count=1
  while test ${job_count} -le ${job_num}; do
    if test ${urgent} -eq 1 -a ${job_count} -ne 1; then
      now_unixtime=`date -u "+%s"`
      now_unixtime=`expr 0 + ${now_unixtime}`
      count_time_limit=`expr \( ${job_count} - 1 \) \* ${time_limit}`
      if test ${now_unixtime} -gt ${job_start_unixtime}; then
        delta_time=`expr ${now_unixtime} - ${job_start_unixtime}`
      else
        delta_time=0
      fi
      if test ${delta_time} -gt ${deadline}; then
        break
      fi
      if test ${delta_time} -lt ${count_time_limit}; then
        sleep_time=`expr ${count_time_limit} - ${delta_time}`
        sleep ${sleep_time}
      fi
    fi
    if test ! -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.txt; then
      set +e
      rclone --contimeout ${timeout} --low-level-retries 1 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --quiet lsf --max-depth 1 ${source_rclone_remote}:${source_bucket}/${index_directory}/${priority} > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.txt
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        exit 0
      else
        rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.txt
        job_count=`expr 1 + ${job_count}`
        continue
      fi
    fi
    set +e
    rclone --contimeout ${timeout} --low-level-retries 1 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --quiet lsf --max-depth 1 ${source_rclone_remote}:${source_bucket}/${index_directory}/${priority} > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      job_count=`expr 1 + ${job_count}`
      continue
    fi
    if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new; then
      diff ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.txt ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new | grep '>' | cut -c3- | sed -e "s|^|/${index_directory}/${priority}/|g" > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff
      if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff; then
        rm -rf ${local_work_directory}/${job_directory}/${unique_job_name}/${index_directory}/${priority}
        set +e
        rclone --transfers ${parallel} --no-check-dest --quiet --ignore-checksum --contimeout ${timeout} --low-level-retries 1 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} copy --files-from-raw ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff ${source_rclone_remote}:${source_bucket} ${local_work_directory}/${job_directory}/${unique_job_name}
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          job_count=`expr 1 + ${job_count}`
          continue
        fi
        if test -n "${inclusive_pattern_file}"; then
          set +e
          if test -n "${exclusive_pattern_file}"; then
            ls -1 ${local_work_directory}/${job_directory}/${unique_job_name}/${index_directory}/${priority}/* | xargs cat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp
          else
            ls -1 ${local_work_directory}/${job_directory}/${unique_job_name}/${index_directory}/${priority}/* | xargs cat | grep -E -f ${inclusive_pattern_file} > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp
          fi
          set -e
        else
          ls -1 ${local_work_directory}/${job_directory}/${unique_job_name}/${index_directory}/${priority}/* | xargs cat > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp
        fi
      fi
      if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp; then
        set +e
        rclone --transfers ${parallel} --no-check-dest --quiet --ignore-checksum --contimeout ${timeout} --low-level-retries 1 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} copy --files-from-raw ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp ${source_rclone_remote}:${source_bucket} ${dest_rclone_remote}:${dest_bucket}
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          job_count=`expr 1 + ${job_count}`
          continue
        fi
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
        if test ${exit_code} -ne 0; then
          mv -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.txt
        fi
        rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_log.tmp
        rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp
      fi
    else
      echo "ERROR: can not get a list of ${priority}." >&2
      exit 199
    fi
    job_count=`expr 1 + ${job_count}`
  done
  rm -rf ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff
  return ${exit_code}
}
index_directory=4PubSub
job_directory=4Clone
timeout=10s
retry_num=8
cron=0
job_period=60
urgent=0
job_num=1
job_start_unixtime=`date -u "+%s"`
job_start_unixtime=`expr 0 + ${job_start_unixtime}`
for arg in "$@"; do
  case "${arg}" in
    '--help' ) echo "$0 [--clone] local_work_directory unique_job_name source_rclone_remote source_bucket dest_rclone_remote dest_bucket priority_pattern parallel [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
    "--cron" ) cron=1;shift;;
  esac
done
if test -z $8; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
source_rclone_remote=$3
source_bucket=$4
dest_rclone_remote=$5
dest_bucket=$6
set +e
priority=`echo $7 | grep "^p[0-9]$"`
parallel=`echo "$8" | grep '^[0-9]\+$'`
set -e
if test -z ${priority}; then
  echo "ERROR: $7 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
if test -z "${parallel}"; then
  echo "ERROR: $8 is not integer." >&2
  exit 199
elif test $8 -le 0; then
  echo "ERROR: $8 is not more than 1." >&2
  exit 199
fi
if test ${priority} = 'p1'; then
  urgent=1
  job_num=4
fi
time_limit=`expr ${job_period} / ${job_num}`
deadline=`expr \( ${job_num} - 1 \) \* ${time_limit}`
inclusive_pattern_file=''
if test $# -ge 9; then
  inclusive_pattern_file=$9
fi
exclusive_pattern_file=''
if test $# -ge 10; then
  exclusive_pattern_file=$10
fi
if test ${cron} -eq 1; then
  if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt; then
    running=`cat ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt | xargs ps -f --no-headers | grep " $0 " | grep " ${unique_job_name} " | wc -l`
  else
    mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
    running=0
  fi
  if test ${running} -eq 0; then
    clone &
    pid=$!
    echo ${pid} > ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt
    wait ${pid}
  fi
else
  mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
  clone
fi
