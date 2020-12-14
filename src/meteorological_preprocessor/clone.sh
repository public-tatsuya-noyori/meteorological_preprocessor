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
  exit_code=0
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
    if test ${switchable} -eq 0 -a ! -f ${local_work_directory}/${job_directory}/${unique_job_name}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt; then
      set +e
      rclone --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --quiet lsf --max-depth 1 ${source_rclone_remote}:${source_bucket}/${index_directory}/${priority} > ${local_work_directory}/${job_directory}/${unique_job_name}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt
      tmp_exit_code=$?
      set -e
      if test ${tmp_exit_code} -ne 0; then
        exit_code=${tmp_exit_code}
        rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt
        job_count=`expr 1 + ${job_count}`
        continue
      fi
    fi
    set +e
    rclone lsf ${dest_rclone_remote}:${dest_bucket}/${index_directory} > /dev/null
    tmp_exit_code=$?
    set -e
    if test ${tmp_exit_code} -ne 0; then
      exit_code=${tmp_exit_code}
      job_count=`expr 1 + ${job_count}`
      continue
    fi
    set +e
    rclone --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --quiet lsf --max-depth 1 ${source_rclone_remote}:${source_bucket}/${index_directory}/${priority} > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new
    tmp_exit_code=$?
    set -e
    if test ${tmp_exit_code} -ne 0; then
      exit_code=${tmp_exit_code}
      job_count=`expr 1 + ${job_count}`
      continue
    fi
    if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new; then
      if test ${switchable} -eq 0; then
        diff ${local_work_directory}/${job_directory}/${unique_job_name}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new | grep '>' | cut -c3- | sed -e "s|^|/${index_directory}/${priority}/|g" > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff
      else
        if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt; then
          find ${local_work_directory}/${job_directory}/${unique_job_name}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt -type f -mmin +${preserve_index_minutes} | xargs rm -f
        fi
        if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt; then
          diff ${local_work_directory}/${job_directory}/${unique_job_name}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new | grep '>' | cut -c3- | sed -e "s|^|/${index_directory}/${priority}/|g" > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff
        else
          tail -${preserve_index_minutes} ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new | sed -e "s|^|/${index_directory}/${priority}/|g" > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff
        fi
      fi
      if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff; then
        rm -rf ${local_work_directory}/${job_directory}/${unique_job_name}/${index_directory}/${priority}
        set +e
        rclone --transfers ${parallel} --no-check-dest --quiet --ignore-checksum --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} copy --files-from-raw ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff ${source_rclone_remote}:${source_bucket} ${local_work_directory}/${job_directory}/${unique_job_name}
        tmp_exit_code=$?
        set -e
        if test ${tmp_exit_code} -ne 0; then
          exit_code=${tmp_exit_code}
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
      if test ${switchable} -ne 0 -a -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp; then
        ls -1 ${local_work_directory}/${job_directory}/${unique_job_name}/${preserved_index_directory}/*/*/* | grep -v /${source_rclone_remote}/${source_bucket}/ | xargs cat | sort -u > ${local_work_directory}/${job_directory}/${unique_job_name}/preserved_filter.tmp
        grep -F -v -f ${local_work_directory}/${job_directory}/${unique_job_name}/preserved_filter.tmp ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp > ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp2
        mv -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp2 ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp
      fi
      if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp; then
        set +e
        if test ${pub_dir_list_index} -eq 1; then
          rclone --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --s3-copy-cutoff ${cutoff} --s3-upload-cutoff ${cutoff} --s3-upload-concurrency ${parallel} --transfers ${parallel} --no-check-dest --quiet --ignore-checksum --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} copy --include-from ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp ${source_rclone_remote}:${source_bucket} ${dest_rclone_remote}:${dest_bucket}
        else
          rclone --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --s3-copy-cutoff ${cutoff} --s3-upload-cutoff ${cutoff} --s3-upload-concurrency ${parallel} --transfers ${parallel} --no-check-dest --quiet --ignore-checksum --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} copy --files-from-raw ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp ${source_rclone_remote}:${source_bucket} ${dest_rclone_remote}:${dest_bucket}
        fi
        tmp_exit_code=$?
        set -e
        if test ${tmp_exit_code} -ne 0; then
          exit_code=${tmp_exit_code}
          job_count=`expr 1 + ${job_count}`
          continue
        fi
        tmp_exit_code=1
        retry_count=1
        rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_log.tmp
        while test ${tmp_exit_code} -ne 0; do
          now=`date -u "+%Y%m%d%H%M%S"`
          set +e
          rclone --immutable --quiet --log-file ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_log.tmp --ignore-checksum --contimeout ${timeout} --low-level-retries 3 --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} copyto ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp ${dest_rclone_remote}:${dest_bucket}/${index_directory}/${priority}/${now}.txt
          tmp_exit_code=$?
          set -e
          if test ${tmp_exit_code} -ne 0 -a ${retry_count} -ge ${retry_num}; then
            cat ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_log.tmp >&2
            exit ${tmp_exit_code}
          fi
          retry_count=`expr 1 + ${retry_count}`
        done
        if test ${tmp_exit_code} -eq 0; then
          mv -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new ${local_work_directory}/${job_directory}/${unique_job_name}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt
        else
          exit_code=${tmp_exit_code}
        fi
        rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_log.tmp
        if test ${switchable} -eq 0; then
          rm -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp
        else
          now=`date -u "+%Y%m%d%H%M%S"`
          mv -f ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_newly_created_index.tmp ${local_work_directory}/${job_directory}/${unique_job_name}/${preserved_index_directory}/${source_rclone_remote}/${source_bucket}/${now}.txt
          find ${local_work_directory}/${job_directory}/${unique_job_name}/${preserved_index_directory} -mindepth 3 -maxdepth 3 -type f -mmin +${preserve_index_minutes} | xargs rm -f > /dev/null 2>&1
        fi
      else
        if test ${switchable} -ne 0; then
          should_switch=`ls -1 ${local_work_directory}/${job_directory}/${unique_job_name}/${preserved_index_directory}/${source_rclone_remote}/${source_bucket}/* | tail -1 | xargs -I{} find {} -type f -mmin +${switch_minutes} | wc -l`
          if test ${should_switch} -ne 0 -a ${permit_empty_newly_index} -eq 0;then
            exit_code=222
            job_count=`expr 1 + ${job_count}`
            continue
          fi
        fi
      fi
    fi
    job_count=`expr 1 + ${job_count}`
  done
  rm -rf ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.new ${local_work_directory}/${job_directory}/${unique_job_name}/${priority}_index.diff
  return ${exit_code}
}
index_directory=4PubSub
job_directory=4Clone
timeout=8s
retry_num=8
cutoff=32M
job_period=60
urgent=0
job_num=1
job_start_unixtime=`date -u "+%s"`
job_start_unixtime=`expr 0 + ${job_start_unixtime}`
cron=0
pub_dir_list_index=0
switchable=0
preserved_index_directory=preserved_index
preserve_index_minutes=180
switch_minutes=5
permit_empty_newly_index=0
for arg in "$@"; do
  case "${arg}" in
    '--help' ) echo "$0 [--clone] [--pub_dir_list_index] [--urgent] [--switchable] [--permit_empty_newly_index] local_work_directory unique_job_name source_rclone_remote source_bucket dest_rclone_remote dest_bucket priority parallel [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
    "--cron" ) cron=1;shift;;
    "--pub_dir_list_index" ) pub_dir_list_index=1;shift;;
    "--urgent" ) urgent=1;shift;;
    "--switchable" ) switchable=1;shift;;
    "--permit_empty_newly_index" ) permit_empty_newly_index=1;shift;;
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
if test ${urgent} -eq 1; then
  job_num=4
  time_limit=`expr ${job_period} / ${job_num}`
  deadline=`expr \( ${job_num} - 1 \) \* ${time_limit}`
fi
inclusive_pattern_file=''
if test $# -ge 9; then
  inclusive_pattern_file=$9
fi
exclusive_pattern_file=''
if test $# -ge 10; then
  exclusive_pattern_file=$10
fi
if test ${switchable} -eq 0; then
  mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}
else
  mkdir -p ${local_work_directory}/${job_directory}/${unique_job_name}/${preserved_index_directory}/${source_rclone_remote}/${source_bucket}
fi
if test ${cron} -eq 1; then
  if test -s ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt; then
    running=`cat ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt | xargs ps ho "pid comm args" | grep " $0 " | grep " ${unique_job_name} " | wc -l`
  else
    running=0
  fi
  if test ${running} -eq 0; then
    clone &
    pid=$!
    echo ${pid} > ${local_work_directory}/${job_directory}/${unique_job_name}/pid.txt
    wait ${pid}
  fi
else
  clone
fi
