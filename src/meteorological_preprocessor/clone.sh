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
for arg in "$@"; do
  case "${arg}" in
    '--help' ) echo "$0 job_name src_rclone_remote src_bucket priority_name_pattern dst_rclone_remote dst_bucket local_dir parallel access [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
  esac
done
if test $# -lt 9; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
job_name=$1
src_rclone_remote=$2
src_bucket=$3
priority_name_pattern=$4
dst_rclone_remote=$5
dst_bucket=$6
local_dir=$7
parallel=$8
set +e
parallel=`echo "$8" | grep '^[0-9]\+$'`
set -e
if test -z "${parallel}"; then
  echo "ERROR: $8 is not integer."
  exit 199
elif test $8 -le 0; then
  echo "ERROR: $8 is not more than 1."
  exit 199
fi
access=$9
inclusive_pattern_file=''
if test $# -ge 10; then
  inclusive_pattern_file=$10
fi
exclusive_pattern_file=''
if test $# -ge 11; then
  exclusive_pattern_file=$11
fi
job_dir=4Clone
mkdir -p ${local_dir}/${access}/${job_dir}/${job_name}
is_running=0
if test -s ${local_dir}/${access}/${job_dir}/${job_name}/pid.txt; then
  is_running=`cat ${local_dir}/${access}/${job_dir}/${job_name}/pid.txt | xargs ps -f --no-headers | grep "$0 ${job_name}" | wc -l`
fi
if test ${is_running} -eq 0; then
  {
    job_datetime=`date -u "+%Y%m%d%H%M%S"`
    priority_name_list=`rclone --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub | grep -E "${priority_name_pattern}"`
    if test -z "${priority_name_list}"; then
      echo "ERROR: can not get priority_name_list."
      exit 199
    fi
    for priority_name in `echo ${priority_name_list}`; do
      mkdir -p ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log
      if test ! -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/index.txt; then
        rclone --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name} > ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/index.txt
        exit 0
      fi
      rclone --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name} > ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/index.txt.new
      if test -s ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/index.txt.new; then
        for newly_created in `diff ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/index.txt ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/index.txt.new | grep '>' | cut -c3-`; do
          now=`date -u "+%Y%m%d%H%M%S"`
          rclone --update --use-server-modtime --no-traverse --size-only --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log copyto ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name}/${newly_created} ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/${newly_created}.tmp
          error_num=`grep ERROR ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log | wc -l`
          if test ${error_num} -ne 0; then
            cat ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log
            rm -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log
            exit 199
          fi
          rm -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_sub.log
          if test -n "${inclusive_pattern_file}"; then
            if test -n "${exclusive_pattern_file}"; then
              grep -v -E -f ${exclusive_pattern_file} ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/${newly_created}.tmp | grep -E -f ${inclusive_pattern_file} | uniq >> ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/output_${job_datetime}.tmp
            else
              grep -E -f ${inclusive_pattern_file} ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/${newly_created}.tmp | uniq >> ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/output_${job_datetime}.tmp
            fi
          else
            cat ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/${newly_created}.tmp >> ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/output_${job_datetime}.tmp
          fi
          rm -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/${newly_created}.tmp
        done
        if test -s ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/output_${job_datetime}.tmp; then
          now=`date -u "+%Y%m%d%H%M%S"`
          rclone --ignore-checksum --update --use-server-modtime --no-gzip-encoding --no-traverse --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level ERROR --log-file ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}.log copy --files-from-raw ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/output_${job_datetime}.tmp ${src_rclone_remote}:${src_bucket} ${dst_rclone_remote}:${dst_bucket}
          error_num=`grep ERROR ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}.log | wc -l`
          if test ${error_num} -ne 0; then
            cat ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}.log
            rm -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}.log
            exit 199
          fi
          rm -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}.log
          error_num=1
          retry_num=0
          while test ${error_num} -ne 0; do
            now=`date -u "+%Y%m%d%H%M%S"`
            set +e
            rclone --ignore-checksum --immutable --no-traverse --size-only --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log copyto ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/output_${job_datetime}.tmp ${dst_rclone_remote}:${dst_bucket}/4PubSub/${priority_name}/${now}.txt
            exit_code=$?
            set -e
            error_num=`grep ERROR ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log | wc -l`
            if test ${exit_code} -ne 0 -o ${error_num} -ne 0; then
              if test ${retry_num} -ge 8; then
                cat ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log
                rm -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log
                exit 199
              else
                sleep 1
              fi
              set +e
              retry_num=`expr 1 + ${retry_num}`
              set -e
            fi
            rm -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/log/${job_datetime}_${now}_index_pub.log
          done
          rm -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/output_${job_datetime}.tmp
        fi
        mv -f ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/index.txt.new ${local_dir}/${access}/${job_dir}/${job_name}/${priority_name}/index.txt
      else
        echo "ERROR: can not get a list of ${priority_name}."
        exit 199
      fi
    done
  } &
  echo $! > ${local_dir}/${access}/${job_dir}/${job_name}/pid.txt
fi
