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
subscribe() {
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
    if test ! -f ${work_directory}/${priority}_index.txt; then
      set +e
      rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote}:${source_bucket}/${pubsub_index_directory}/${priority} > ${work_directory}/${priority}_index.txt
      tmp_exit_code=$?
      set -e
      if test ${tmp_exit_code} -ne 0; then
        exit_code=${tmp_exit_code}
        rm -f ${work_directory}/${priority}_index.txt
        job_count=`expr 1 + ${job_count}`
        continue
      fi
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
      diff ${work_directory}/${priority}_index.txt ${work_directory}/${priority}_new_index.tmp | grep '>' | cut -c3- > ${work_directory}/${priority}_index_diff.tmp
      if test -s ${work_directory}/${priority}_index_diff.tmp; then
        set +e
        cmp -s ${work_directory}/${priority}_index_diff.tmp ${work_directory}/${priority}_new_index.tmp
        cmp_exit_code=$?
        set -e
        if test ${cmp_exit_code} -eq 0; then
          set +e
          rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote}:${source_bucket}/${search_index_directory}/${priority} > ${work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp
          tmp_exit_code=$?
          set -e
          if test ${tmp_exit_code} -ne 0; then
            exit_code=${tmp_exit_code}
            job_count=`expr 1 + ${job_count}`
            continue
          fi
          if test -s ${work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp; then
            mv -f ${work_directory}/${priority}_new_index.tmp ${work_directory}/${priority}_new_index_last_part.tmp
            for date_hour_directory in `tac ${work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp`; do
              set +e
              rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote}:${source_bucket}/${search_index_directory}/${priority}/${date_hour_directory} > ${work_directory}/${priority}_${search_index_directory}_date_hour_directory_index.tmp
              tmp_exit_code=$?
              set -e
              if test ${tmp_exit_code} -ne 0; then
                exit_code=${tmp_exit_code}
                job_count=`expr 1 + ${job_count}`
                continue
              fi
              cat ${work_directory}/${priority}_${search_index_directory}_date_hour_directory_index.tmp ${work_directory}/${priority}_new_index_last_part.tmp | sort -u > ${work_directory}/${priority}_new_index.tmp
              diff ${work_directory}/${priority}_index.txt ${work_directory}/${priority}_new_index.tmp | grep '>' | cut -c3- > ${work_directory}/${priority}_index_diff.tmp
              if test -s ${work_directory}/${priority}_index_diff.tmp; then
                set +e
                cmp -s ${work_directory}/${priority}_index_diff.tmp ${work_directory}/${priority}_new_index.tmp
                cmp_exit_code=$?
                set -e
                if test ${cmp_exit_code} -eq 1; then
                  break
                fi
              fi
            done
          fi
        fi
        rm -rf ${work_directory}/${pubsub_index_directory}/${priority}
        sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" ${work_directory}/${priority}_index_diff.tmp > ${work_directory}/${priority}_newly_created_indexs.tmp
        set +e
        rclone copy --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${work_directory}/${priority}_newly_created_indexs.tmp --ignore-checksum --local-no-set-modtime --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote}:${source_bucket} ${work_directory}
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
            ls -1 ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/${priority}_newly_created_files.tmp
          else
            ls -1 ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -E -f ${inclusive_pattern_file} > ${work_directory}/${priority}_newly_created_files.tmp
          fi
          set -e
        else
          ls -1 ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat > ${work_directory}/${priority}_newly_created_files.tmp
        fi
      fi
      if test -s ${work_directory}/${priority}_newly_created_files.tmp; then
        rm -f ${work_directory}/${priority}_log.tmp
        set +e
        if test ${pub_with_wildcard} -eq 1; then
          rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --ignore-checksum --include-from ${work_directory}/${priority}_newly_created_files.tmp --log-file ${work_directory}/${priority}_log.tmp --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote}:${source_bucket} ${local_work_directory}
        else
          rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/${priority}_newly_created_files.tmp --ignore-checksum --log-file ${work_directory}/${priority}_log.tmp --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote}:${source_bucket} ${local_work_directory}
        fi
        tmp_exit_code=$?
        set -e
        if test ${tmp_exit_code} -ne 0; then
          exit_code=${tmp_exit_code}
          set +e
          grep ERROR ${work_directory}/${priority}_log.tmp >&2
          set -e
        fi
        if test ${tmp_exit_code} -eq 0; then
          if test -s ${work_directory}/${priority}_log.tmp; then
            sed -e "s|^.* INFO *: *\(.*\) *: Copied .*$|${local_work_directory}/\1|g" ${work_directory}/${priority}_log.tmp
          fi
          index_preserve_days_regex_pattern="("
          index_preserve_days_count=0
          while test ${index_preserve_days_count} -le ${index_preserve_days}; do
            index_preserve_days_regex_pattern="${index_preserve_days_regex_pattern}|"`date -u "+%Y%m%d" -d "${index_preserve_days_count} day ago"`
            index_preserve_days_count=`expr 1 + ${index_preserve_days_count}`
          done
          index_preserve_days_regex_pattern="${index_preserve_days_regex_pattern})"
          cat ${work_directory}/${priority}_index.tmp ${work_directory}/${priority}_index_diff.tmp | grep -E ${index_preserve_days_regex_pattern} | sort -u | ${work_directory}/${priority}_new_index.txt
        fi
        rm -f ${work_directory}/${priority}_log.tmp
        rm -f ${work_directory}/${priority}_newly_created_files.tmp
      fi
    fi
    job_count=`expr 1 + ${job_count}`
  done
  rm -rf ${work_directory}/${priority}_new_index.tmp ${work_directory}/${priority}_index_diff.tmp ${work_directory}/${priority}_${search_index_directory}_index.tmp ${work_directory}/${pubsub_index_directory}/${priority}
  return ${exit_code}
}
cron=0
cutoff=16M
index_preserve_days=1
job_directory=4Sub
job_num=1
job_period=60
job_start_unixtime=`date -u "+%s"`
job_start_unixtime=`expr 0 + ${job_start_unixtime}`
pub_with_wildcard=0
pubsub_index_directory=4PubSub
search_index_directory=4Search
timeout=8s
urgent=0
for arg in "$@"; do
  case "${arg}" in
    "--cron" ) cron=1;shift;;
    '--help' ) echo "$0 [--cron] [--pub_with_wildcard] [--urgent] local_work_directory unique_job_name source_rclone_remote source_bucket priority parallel [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
    "--pub_with_wildcard" ) pub_with_wildcard=1;shift;;
    "--urgent" ) urgent=1;shift;;
  esac
done
if test -z $6; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
source_rclone_remote=$3
source_bucket=$4
set +e
priority=`echo $5 | grep "^p[1-9]$"`
parallel=`echo $6 | grep '^[0-9]\+$'`
set -e
if test -z ${priority}; then
  echo "ERROR: $5 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
if test -z "${parallel}"; then
  echo "ERROR: $6 is not integer." >&2
  exit 199
elif test $6 -le 0; then
  echo "ERROR: $6 is not more than 1." >&2
  exit 199
fi
if test ${urgent} -eq 1; then
  job_num=4
  time_limit=`expr ${job_period} / ${job_num}`
  deadline=`expr \( ${job_num} - 1 \) \* ${time_limit}`
fi
inclusive_pattern_file=''
if test -n $7; then
  inclusive_pattern_file=$7
fi
exclusive_pattern_file=''
if test -n $8; then
  exclusive_pattern_file=$8
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
    subscribe &
    pid=$!
    echo ${pid} > ${work_directory}/pid.txt
    wait ${pid}
  fi
else
  subscribe
fi
