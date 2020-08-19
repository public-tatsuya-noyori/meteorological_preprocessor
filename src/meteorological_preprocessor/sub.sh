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
job_unixtime=`date -u "+%s"`
job_unixtime=`expr 0 + ${job_unixtime}`
for arg in "$@"; do
  case "${arg}" in
    '--help' ) echo "$0 unique_job_name src_rclone_remote src_bucket priority_name_pattern local_dir parallel access [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
  esac
done
if test $# -lt 7; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
unique_job_name=$1
src_rclone_remote=$2
src_bucket=$3
priority_name_pattern=$4
job_loop_period=60
is_urgent=0
job_loop_num=1
if test "${priority_name_pattern}" = 'p1'; then
  is_urgent=1
  job_loop_num=2
fi
time_limit=`expr ${job_loop_period} / ${job_loop_num}`
local_dir=$5
set +e
parallel=`echo "$6" | grep '^[0-9]\+$'`
set -e
if test -z "${parallel}"; then
  echo "ERROR: $6 is not integer." >&2
  exit 199
elif test $6 -le 0; then
  echo "ERROR: $6 is not more than 1." >&2
  exit 199
fi
access=$7
inclusive_pattern_file=''
if test $# -ge 8; then
  inclusive_pattern_file=$8
fi
exclusive_pattern_file=''
if test $# -ge 9; then
  exclusive_pattern_file=$9
fi
job_dir=4Sub
mkdir -p ${local_dir}/${access}/${job_dir}/${unique_job_name}
is_running=0
if test -s ${local_dir}/${access}/${job_dir}/${unique_job_name}/pid.txt; then
  is_running=`cat ${local_dir}/${access}/${job_dir}/${unique_job_name}/pid.txt | xargs ps -f --no-headers | grep "$0 ${unique_job_name}" | wc -l`
fi
if test ${is_running} -eq 0; then
  {
    job_loop_count=1
    while test ${job_loop_count} -le ${job_loop_num}; do
      priority_name_list=`rclone --contimeout 30s --timeout 30s --low-level-retries 1 --retries 1 --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub | grep -E "${priority_name_pattern}"`
      if test -z "${priority_name_list}"; then
        echo "ERROR: can not get priority_name_list." >&2
        exit 199
      fi
      for priority_name in `echo ${priority_name_list}`; do
        mkdir -p ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log
        if test ! -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/index.txt; then
          rclone --contimeout 30s --timeout 30s --low-level-retries 1 --retries 1 --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name} > ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/index.txt
          exit 0
        fi
        rclone --contimeout 30s --timeout 30s --low-level-retries 1 --retries 1 --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name} > ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/index.txt.new
        if test -s ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/index.txt.new; then
          for newly_created in `diff ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/index.txt ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/index.txt.new | grep '>' | cut -c3-`; do
            now=`date -u "+%Y%m%d%H%M%S"`
            rclone --contimeout 30s --timeout 30s --low-level-retries 1 --retries 1 --update --use-server-modtime --no-traverse --size-only --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log copyto ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name}/${newly_created} ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/${newly_created}.tmp
            error_count=`grep ERROR ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log | wc -l`
            if test ${error_count} -ne 0; then
              cat ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log >&2
              rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log
              exit 199
            fi
            rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log
            if test -n "${inclusive_pattern_file}"; then
              if test -n "${exclusive_pattern_file}"; then
                grep -v -E -f ${exclusive_pattern_file} ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/${newly_created}.tmp | grep -E -f ${inclusive_pattern_file} | uniq >> ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp
              else
                grep -E -f ${inclusive_pattern_file} ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/${newly_created}.tmp | uniq >> ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp
              fi
            else
              cat ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/${newly_created}.tmp >> ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp
            fi
            rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/${newly_created}.tmp
          done
          if test -s ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp; then
            now=`date -u "+%Y%m%d%H%M%S"`
            rclone --contimeout 30s --timeout 30s --low-level-retries 1 --retries 1 --update --use-server-modtime --no-traverse --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level INFO --log-file ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log copy --files-from-raw ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp ${src_rclone_remote}:${src_bucket} ${local_dir}/${access}
            error_count=`grep ERROR ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log | wc -l`
            if test ${error_count} -ne 0; then
              cat ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log >&2
              rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log
              exit 199
            fi
            sed -e "s|^.* INFO *: *\(.*\) *: Copied .*$|${local_dir}/${access}/\1|g" ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log
            rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/log/${job_datetime}_${now}.log
            rm -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/output_${job_datetime}.tmp
          fi
          mv -f ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/index.txt.new ${local_dir}/${access}/${job_dir}/${unique_job_name}/${priority_name}/index.txt
        else
          echo "ERROR: can not get a list of ${priority_name}." >&2
          exit 199
        fi
      done
      if test ${is_urgent} -eq 1 -a ${job_loop_count} -lt ${job_loop_num}; then
        now_unixtime=`date -u "+%s"`
        now_unixtime=`expr 0 + ${now_unixtime}`
        set +e
        delta_time=`expr ${now_unixtime} - ${job_unixtime}`
        if test ${delta_time} -lt ${time_limit}; then
          sleep_time=`expr ${time_limit} - ${delta_time}`
          sleep ${sleep_time}
        fi
        set -e
      fi
      job_loop_count=`expr 1 + ${job_loop_count}`
    done
  } &
  pid=$!
  echo ${pid} > ${local_dir}/${access}/${job_dir}/${unique_job_name}/pid.txt
  wait ${pid}
fi
