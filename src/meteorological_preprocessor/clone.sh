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
    if test ${switchable} -eq 0 -a ! -f ${work_directory}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt; then
      set +e
      rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote}:${source_bucket}/${pubsub_index_directory}/${priority} > ${work_directory}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt
      tmp_exit_code=$?
      set -e
      if test ${tmp_exit_code} -ne 0; then
        exit_code=${tmp_exit_code}
        rm -f ${work_directory}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt
        job_count=`expr 1 + ${job_count}`
        continue
      fi
    fi
    set +e
    rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${dest_rclone_remote}:${dest_bucket}/${pubsub_index_directory} > /dev/null
    tmp_exit_code=$?
    set -e
    if test ${tmp_exit_code} -ne 0; then
      exit_code=${tmp_exit_code}
      job_count=`expr 1 + ${job_count}`
      continue
    fi
    set +e
    rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote}:${source_bucket}/${pubsub_index_directory}/${priority} > ${work_directory}/${priority}_new_index.tmp
    tmp_exit_code=$?
    set -e
    if test ${tmp_exit_code} -ne 0; then
      exit_code=${tmp_exit_code}
      job_count=`expr 1 + ${job_count}`
      continue
    fi
    if test -s ${work_directory}/${priority}_new_index.tmp; then
      if test ${switchable} -eq 0; then
        diff ${work_directory}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt ${work_directory}/${priority}_new_index.tmp | grep '>' | cut -c3- | sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" > ${work_directory}/${priority}_index_diff.tmp
      else
        if test -s ${work_directory}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt; then
          find ${work_directory}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt -type f -mmin +${switchable_minutes} | xargs -r rm -f
        fi
        if test -s ${work_directory}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt; then
          diff ${work_directory}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt ${work_directory}/${priority}_new_index.tmp | grep '>' | cut -c3- | sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" > ${work_directory}/${priority}_index_diff.tmp
        else
          date_hours_regex_pattern="("
          date_hours_regex_pattern="${date_hours_regex_pattern}"`date -u "+%Y%m%d%H"`
          switchable_hours_count=1
          while test ${switchable_hours_count} -lt ${switchable_hours}; do
            date_hours_regex_pattern="${date_hours_regex_pattern}|"`date -u "+%Y%m%d%H" -d "${switchable_hours_count} hour ago"`
            switchable_hours_count=`expr 1 + ${switchable_hours_count}`
          done
          date_hours_regex_pattern="${date_hours_regex_pattern})"
          grep -E ${date_hours_regex_pattern} ${work_directory}/${priority}_new_index.tmp | sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" > ${work_directory}/${priority}_index_diff.tmp
        fi
      fi
      if test -s ${work_directory}/${priority}_index_diff.tmp; then
        cmp -s ${work_directory}/${priority}_index_diff.tmp ${work_directory}/${priority}_new_index.tmp
        cmp_exit_code=$?
        if test ${cmp_exit_code} -eq 0; then
          set +e
          rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote}:${source_bucket}/${search_index_directory}/${priority} > ${work_directory}/${priority}_new_index_2.tmp
          tmp_exit_code=$?
          set -e
          if test ${tmp_exit_code} -ne 0; then
            exit_code=${tmp_exit_code}
            job_count=`expr 1 + ${job_count}`
            continue
          fi
          cat ${work_directory}/${priority}_new_index.tmp ${work_directory}/${priority}_${search_index_directory}_index.tmp | sort -u > ${work_directory}/${priority}_merged_index.tmp
          diff ${work_directory}/${priority}_index.txt ${work_directory}/${priority}_merged_index.tmp | grep '>' | cut -c3- | sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" > ${work_directory}/${priority}_index_diff.tmp
        fi
        rm -rf ${work_directory}/${pubsub_index_directory}/${priority}
        set +e
        rclone copy --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${work_directory}/${priority}_index_diff.tmp --ignore-checksum --local-no-set-modtime --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote}:${source_bucket} ${work_directory}
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
            ls -1 ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/${priority}_newly_created_index.tmp
          else
            ls -1 ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -E -f ${inclusive_pattern_file} > ${work_directory}/${priority}_newly_created_index.tmp
          fi
          set -e
        else
          ls -1 ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat > ${work_directory}/${priority}_newly_created_index.tmp
        fi
      fi
      if test ${switchable} -eq 1 -a -s ${work_directory}/${priority}_newly_created_index.tmp; then
        ls -1 ${work_directory}/${preserved_index_directory}/*/*/* | grep -v "^.*\.tmp$" | xargs -r cat | sort -u > ${work_directory}/preserved_filter.tmp
        grep -F -v -f ${work_directory}/preserved_filter.tmp ${work_directory}/${priority}_newly_created_index.tmp > ${work_directory}/${priority}_newly_created_index.tmp2
        mv -f ${work_directory}/${priority}_newly_created_index.tmp2 ${work_directory}/${priority}_newly_created_index.tmp
      fi
      if test -s ${work_directory}/${priority}_newly_created_index.tmp; then
        set +e
        if test ${pub_with_wildcard} -eq 1; then
          rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --ignore-checksum --include-from ${work_directory}/${priority}_newly_created_index.tmp --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --quiet --retries 1 --s3-chunk-size ${cutoff} --s3-upload-concurrency ${parallel} --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote}:${source_bucket} ${dest_rclone_remote}:${dest_bucket}
        else
          rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/${priority}_newly_created_index.tmp --ignore-checksum --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --quiet --retries 1 --s3-chunk-size ${cutoff} --s3-upload-concurrency ${parallel} --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote}:${source_bucket} ${dest_rclone_remote}:${dest_bucket}
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
        rm -f ${work_directory}/${priority}_log.tmp
        while test ${tmp_exit_code} -ne 0; do
          now=`date -u "+%Y%m%d%H%M%S"`
          set +e
          rclone copyto --contimeout ${timeout} --ignore-checksum --immutable --log-file ${work_directory}/${priority}_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} ${work_directory}/${priority}_newly_created_index.tmp ${dest_rclone_remote}:${dest_bucket}/${pubsub_index_directory}/${priority}/${now}.txt
          tmp_exit_code=$?
          set -e
          if test ${tmp_exit_code} -ne 0 -a ${retry_count} -ge ${retry_num}; then
            cat ${work_directory}/${priority}_log.tmp >&2
            exit ${tmp_exit_code}
          fi
          retry_count=`expr 1 + ${retry_count}`
        done
        if test ${tmp_exit_code} -eq 0; then
          mv -f ${work_directory}/${priority}_new_index.tmp ${work_directory}/${source_rclone_remote}_${source_bucket}_${priority}_index.txt
        else
          exit_code=${tmp_exit_code}
        fi
        rm -f ${work_directory}/${priority}_log.tmp
        if test ${switchable} -eq 0; then
          rm -f ${work_directory}/${priority}_newly_created_index.tmp
        else
          now=`date -u "+%Y%m%d%H%M%S"`
          mv -f ${work_directory}/${priority}_newly_created_index.tmp ${work_directory}/${preserved_index_directory}/${source_rclone_remote}/${source_bucket}/${now}.txt
          find ${work_directory}/${preserved_index_directory} -mindepth 3 -maxdepth 3 -type f -mmin +${switchable_minutes} | xargs -r rm -f > /dev/null 2>&1
        fi
      else
        if test ${switchable} -eq 1 -a ${empty_newly_index_failed} -eq 1; then
          count_within_empty_newly_index_failed_minutes=`ls -1 ${work_directory}/${preserved_index_directory}/${source_rclone_remote}/${source_bucket}/* | grep -v "^.*\.tmp$" | tail -1 | xargs -r -I {} find {} -type f -mmin -${empty_newly_index_failed_minutes} | wc -l`
          if test ${count_within_empty_newly_index_failed_minutes} -eq 0;then
            exit_code=222
            job_count=`expr 1 + ${job_count}`
            continue
          fi
        fi
      fi
    fi
    job_count=`expr 1 + ${job_count}`
  done
  rm -rf ${work_directory}/${priority}_new_index.tmp ${work_directory}/${priority}_index_diff.tmp ${work_directory}/${priority}_${search_index_directory}_index.tmp ${work_directory}/${priority}_merged_index.tmp ${work_directory}/preserved_filter.tmp
  return ${exit_code}
}
cron=0
cutoff=32M
empty_newly_index_failed=0
empty_newly_index_failed_minutes=5
job_directory=4Clone
job_period=60
job_num=1
job_start_unixtime=`date -u "+%s"`
job_start_unixtime=`expr 0 + ${job_start_unixtime}`
pub_with_wildcard=0
pubsub_index_directory=4PubSub
retry_num=8
switchable=0
switchable_hours=3
switchable_minutes=`expr 60 \* ${switchable_hours}`
timeout=8s
urgent=0
for arg in "$@"; do
  case "${arg}" in
    "--cron" ) cron=1;shift;;
    '--help' ) echo "$0 [--cron] [--pub_with_wildcard] [--switchable] [--switchable_end] [--urgent] local_work_directory unique_job_name source_rclone_remote source_bucket dest_rclone_remote dest_bucket priority parallel [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
    "--pub_with_wildcard" ) pub_with_wildcard=1;shift;;
    "--switchable" ) switchable=1;empty_newly_index_failed=1;shift;;
    "--switchable_end" ) switchable=1;empty_newly_index_failed=0;shift;;
    "--urgent" ) urgent=1;shift;;
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
priority=`echo $7 | grep "^p[1-9]$"`
parallel=`echo $8 | grep '^[0-9]\+$'`
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
preserved_index_directory=preserved_${priority}_index
work_directory=${local_work_directory}/${job_directory}/${unique_job_name}
if test ${switchable} -eq 0; then
  mkdir -p ${work_directory}
else
  mkdir -p ${work_directory}/${preserved_index_directory}/${source_rclone_remote}/${source_bucket}
  touch ${work_directory}/${preserved_index_directory}/${source_rclone_remote}/${source_bucket}/dummy.tmp
fi
if test ${cron} -eq 1; then
  if test -s ${work_directory}/pid.txt; then
    running=`cat ${work_directory}/pid.txt | xargs -r ps ho "pid comm args" | grep " $0 " | grep " ${unique_job_name} " | wc -l`
  else
    running=0
  fi
  if test ${running} -eq 0; then
    clone &
    pid=$!
    echo ${pid} > ${work_directory}/pid.txt
    wait ${pid}
  fi
else
  clone
fi
