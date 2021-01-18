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
  for job_count in `seq ${job_num}`; do
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
    cp /dev/null ${work_directory}/${priority}_processed_file.tmp
    mkdir -p ${work_directory}/${priority}
    cp /dev/null ${work_directory}/${priority}_processed/dummy.tmp
    source_rclone_remote_bucket_exit_code_list='0'
    for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_list} | tr ';' '\n'`; do
      if test "${source_rclone_remote_bucket_exit_code_list}" != '0'; then
        source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
      fi
      source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
      source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
      if test ! -d ${source_work_directory}/${priority}; then
        mkdir -p ${source_work_directory}/${priority}
      fi
      rm -rf ${source_work_directory}/${pubsub_index_directory}/${priority}
      rm -rf ${source_work_directory}/${search_index_directory}/${priority}
      if test ! -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt; then
        if test ${sub} -eq 1; then
          cp /dev/null ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
        else
          set +e
          rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
          exit_code=$?
          set -e
          if test ${exit_code} -ne 0; then
            source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e 's|^\(.*\)[^ ]\+$|\1 ${exit_code}|g' -e 's|^ ||g'`
            echo "ERROR: can not get index file list from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >&2
            rm -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
            continue
          fi
        fi
      fi
      set +e
      rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e 's|^\(.*\)[^ ]\+$|\1 ${exit_code}|g' -e 's|^ ||g'`
        echo "ERROR: can not get index file list from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >&2
        continue
      fi
      if test -s ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp; then
        diff ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp | grep '>' | cut -c3- > ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.tmp
        if test -s ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.tmp; then
          sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.tmp > ${source_work_directory}/${priority}_${pubsub_index_directory}_newly_created_index.tmp
          set +e
          rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${source_work_directory}/${priority}_${pubsub_index_directory}_newly_created_index.tmp --ignore-checksum --log-level ${debug_index_file} --local-no-set-modtime --low-level-retries 3 --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${source_work_directory}
          exit_code=$?
          set -e
          if test ${exit_code} -ne 0; then
            source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e 's|^\(.*\)[^ ]\+$|\1 ${exit_code}|g' -e 's|^ ||g'`
            echo "ERROR: can not get index file from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >&2
            continue
          fi
          ls -1 ${source_work_directory}/${pubsub_index_directory}/${priority} > ${source_work_directory}/${priority}_${pubsub_index_directory}_gotten_new_index.tmp
          set +e
          cmp -s ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.tmp ${source_work_directory}/${priority}_${pubsub_index_directory}_gotten_new_index.tmp
          cmp_exit_code_1=$?
          set -e
          if test ${cmp_exit_code_1} -gt 1; then
            echo "ERROR: can not compare." >&2
            continue
          fi
          set +e
          cmp -s ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.tmp ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp
          cmp_exit_code_2=$?
          set -e
          if test ${cmp_exit_code_2} -gt 1; then
            echo "ERROR: can not compare." >&2
            continue
          fi
          cp /dev/null ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp
          if test ${cmp_exit_code_1} -eq 1 -o ${cmp_exit_code_2} -eq 0; then
            set +e
            rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${priority} > ${source_work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp
            exit_code=$?
            set -e
            if test ${exit_code} -ne 0; then
              source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e 's|^\(.*\)[^ ]\+$|\1 ${exit_code}|g' -e 's|^ ||g'`
              echo "ERROR: can not get index directory list from ${source_rclone_remote_bucket}/${search_index_directory}/${priority}." >&2
              continue
            fi
            if test ${sub} -eq 1; then
              grep -E "^(${switchable_backup_date_hour_pattern})" ${source_work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp | sed -e 's|/$||g' > ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp
            else
              sed -e 's|/$||g' ${source_work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp > ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp
            fi
            if test -s ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp; then
              former_index_file_first_line=`head -1 ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt`
              search_index_directory_exit_code=0
              for date_hour_directory in `tac ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp`; do
                set +e
                rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${priority}/${date_hour_directory} > ${source_work_directory}/${priority}_${search_index_directory}_minute_second_index.tmp
                exit_code=$?
                set -e
                if test ${exit_code} -ne 0; then
                  search_index_directory_exit_code=${exit_code}
                  source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e 's|^\(.*\)[^ ]\+$|\1 ${exit_code}|g' -e 's|^ ||g'`
                  echo "ERROR: can not get index file list from ${source_rclone_remote_bucket}/${search_index_directory}/${priority}/${date_hour_directory}." >&2
                  break
                fi
                sed -e "s|^|${date_hour_directory}|g" ${source_work_directory}/${priority}_${search_index_directory}_minute_second_index.tmp > ${source_work_directory}/${priority}_${search_index_directory}_index.tmp
                former_index_file_first_line_count=0
                if test -n "${former_index_file_first_line}"; then
                  former_index_file_first_line_count=`grep ${former_index_file_first_line} ${source_work_directory}/${priority}_${search_index_directory}_index.tmp | wc -l`
                fi
                if test ${former_index_file_first_line_count} -eq 0; then
                  set +e
                  grep -v -F -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt ${source_work_directory}/${priority}_${search_index_directory}_index.tmp | grep -v -F -f ${source_work_directory}/${priority}_${pubsub_index_directory}_gotten_new_index.tmp >> ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp
                  set -e
                else
                  set +e
                  sed -ne "/${former_index_file_first_line}/,\$p" ${source_work_directory}/${priority}_${search_index_directory}_index.tmp | grep -v -F -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt | grep -v -F -f ${source_work_directory}/${priority}_${pubsub_index_directory}_gotten_new_index.tmp >> ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp
                  set -e
                  break
                fi
              done
              if test ${search_index_directory_exit_code} -ne 0; then
                continue
              fi
              if test ${sub} -eq 1; then
                grep -E "^(${switchable_date_hour_ten_minute_pattern})" ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp | sort -u | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${priority}'/${index_file_date_hour}/${index_file_minute_second_extension}' > ${source_work_directory}/${priority}_${search_index_directory}_newly_created_index.tmp
              else
                cat ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp | sort -u | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${priority}'/${index_file_date_hour}/${index_file_minute_second_extension}' > ${source_work_directory}/${priority}_${search_index_directory}_newly_created_index.tmp
              fi
              set +e
              rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${source_work_directory}/${priority}_${search_index_directory}_newly_created_index.tmp --ignore-checksum --log-level ${debug_index_file} --local-no-set-modtime --low-level-retries 3 --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${source_work_directory}
              exit_code=$?
              set -e
              if test ${exit_code} -ne 0; then
                source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e 's|^\(.*\)[^ ]\+$|\1 ${exit_code}|g' -e 's|^ ||g'`
                echo "ERROR: can not get index file from ${source_rclone_remote_bucket}/${search_index_directory}." >&2
                continue
              fi
            fi
          fi
          cp /dev/null ${source_work_directory}/${priority}_newly_created_file.tmp
          if test -n "${inclusive_pattern_file}"; then
            set +e
            if test -n "${exclusive_pattern_file}"; then
              if test -d ${source_work_directory}/${search_index_directory}/${priority}; then
                ls -1 ${source_work_directory}/${search_index_directory}/${priority}/*/* ${source_work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/${priority}_newly_created_file.tmp
              else
                ls -1 ${source_work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/${priority}_newly_created_file.tmp
              fi
            else
              if test -d ${source_work_directory}/${search_index_directory}/${priority}; then
                ls -1 ${source_work_directory}/${search_index_directory}/${priority}/*/* ${source_work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/${priority}_newly_created_file.tmp
              else
                ls -1 ${source_work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/${priority}_newly_created_file.tmp
              fi
            fi
            set -e
          else
            if test -d ${source_work_directory}/${search_index_directory}/${priority}; then
              ls -1 ${source_work_directory}/${search_index_directory}/${priority}/*/* ${source_work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat > ${source_work_directory}/${priority}_newly_created_file.tmp
            else
              ls -1 ${source_work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat > ${source_work_directory}/${priority}_newly_created_file.tmp
            fi
          fi
          cp /dev/null ${source_work_directory}/${priority}_filtered_newly_created_file.tmp
          if test -s ${source_work_directory}/${priority}_newly_created_file.tmp; then
            set +e
            grep -v -F -f ${work_directory}/${priority}_all_processed_file.txt ${source_work_directory}/${priority}_newly_created_file.tmp | grep -v -F -f ${work_directory}/${priority}_processed_file.tmp > ${source_work_directory}/${priority}_filtered_newly_created_file.tmp
            set -e
          fi
          if test -s ${source_work_directory}/${priority}_filtered_newly_created_file.tmp; then
            cp /dev/null ${source_work_directory}/${priority}_log.tmp
            set +e
            rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious ${file_from_option} ${source_work_directory}/${priority}_filtered_newly_created_file.tmp --ignore-checksum --log-file ${source_work_directory}/${priority}_log.tmp --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${local_work_directory}
            exit_code=$?
            set -e
            if test ${exit_code} -ne 0; then
              source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e 's|^\(.*\)[^ ]\+$|\1 ${exit_code}|g' -e 's|^ ||g'`
              set +e
              grep ERROR ${source_work_directory}/${priority}_log.tmp >&2
              set -e
              echo "ERROR: can not get file from ${source_rclone_remote_bucket}." >&2
              continue
            fi
            sed -e "s|^.* INFO *: *\(.*\) *: Copied .*$|/\1|g" ${source_work_directory}/${priority}_log.tmp >> ${work_directory}/${priority}_processed_file.tmp
          fi
        fi
      fi
    done
    if test -s ${work_directory}/${priority}_processed_file.tmp; then
      now=`date -u "+%Y%m%d%H%M%S"`
      cp ${work_directory}/${priority}_processed_file.tmp ${work_directory}/${priority}_processed/${now}.txt
      if test ${standard_output_processed_file} -eq 1; then
        cat ${work_directory}/${priority}_processed_file.tmp
      fi
    fi
    source_rclone_remote_bucket_count=1
    for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_list} | tr ';' '\n'`; do
      source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
      source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
      source_rclone_remote_bucket_exit_code=`echo "${source_rclone_remote_bucket_exit_code_list}" | cut -d' ' -f${source_rclone_remote_bucket_count}`
      if test ${source_rclone_remote_bucket_exit_code} = '0'; then
        mv -f ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
      fi
      source_rclone_remote_bucket_count=`expr 1 + ${source_rclone_remote_bucket_count}`
    done
    ls -1 ${work_directory}/${priority}_processed/* | grep -v "^${work_directory}/${priority}_processed/dummy\.tmp$" | grep -v -E "^${work_directory}/${priority}_processed/(${delete_index_date_hour_pattern})[0-9][0-9][0-9][0-9]\.txt$" | xargs -r rm -f
    ls -1 ${work_directory}/${priority}_processed/* | xargs -r cat > ${work_directory}/${priority}_all_processed_file.txt
  done
  if test ${exit_code} -eq 0 -a ${switchable} -gt 0; then
    switch=1
    for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_list} | tr ';' '\n'`; do
      source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
      source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
      tmp_switch=`find ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt -type f -mmin +${switchable_minute} | wc -l`
      set +e
      switch=`expr ${tmp_switch} \* ${switch}`
      set -e
    done
    if test ${switch} -eq 1; then
      if test ${switchable} -eq 1; then
        echo "ERROR: All index file list has not been updated for 5 minutes on ${source_rclone_remote_bucket_list}." >&2
        return 255
      elif test ${switchable} -eq 2; then
        echo "WARNING: All index file list has not been updated for 5 minutes on ${source_rclone_remote_bucket_list}." >&2
        return 0
      fi
    fi
  fi
  return ${exit_code}
}
bandwidth_limit_k_bytes_per_s=0
cron=0
cutoff=16M
debug_index_file=ERROR
datetime=`date -u "+%Y%m%d%H%M%S"`
datetime_date=`echo ${datetime} | cut -c1-8`
datetime_hour=`echo ${datetime} | cut -c9-10`
delete_index_date_hour_pattern=${datetime_date}${datetime_hour}
delete_index_hour=25
for hour_count in `seq ${delete_index_hour}`; do
  delete_index_date_hour_pattern="${delete_index_date_hour_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour ago" "+%Y%m%d%H"`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour" "+%Y%m%d%H"`
done
job_directory=4Sub
job_num=1
job_period=60
job_start_unixtime=`date -u "+%s"`
job_start_unixtime=`expr 0 + ${job_start_unixtime}`
pubsub_index_directory=4PubSub
search_index_directory=4Search
standard_output_processed_file=0
sub=0
switchable=0
switchable_backup_date_hour_pattern=${datetime_date}${datetime_hour}
switchable_backup_hour=1
for hour_count in `seq ${switchable_backup_hour}`; do
  switchable_backup_date_hour_pattern="${switchable_backup_date_hour_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour ago" "+%Y%m%d%H"`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour" "+%Y%m%d%H"`
done
switchable_date_hour_ten_minute_pattern=${datetime_date}${datetime_hour}`echo ${datetime} | cut -c11`
ten_minute_ago=`expr 60 \* ${switchable_backup_hour}`
for ten_minute_count in `seq 10 10 ${ten_minute_ago}`; do
  switchable_date_hour_ten_minute_pattern="${switchable_date_hour_ten_minute_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${ten_minute_count} minute ago" "+%Y%m%d%H%M" | cut -c1-11`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${ten_minute_count} minute" "+%Y%m%d%H%M" | cut -c1-11`
done
switchable_minute=5
timeout=8s
urgent=0
wildcard_index=0;file_from_option=--files-from-raw
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") shift;bandwidth_limit_k_bytes_per_s=$1;shift;;
    "--cron" ) cron=1;shift;;
    "--debug_shell" ) set -evx;shift;;
    "--debug_index_file" ) debug_index_file=INFO;shift;;
    '--help' ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--cron] [--debug_shell] [--debug_index_file] [--standard_output_processed_file] [--switchable_main/--switchable_sub/--switchable_sub_end] [--urgent] [--wildcard_index] local_work_directory unique_job_name 'source_rclone_remote_bucket_main[;source_rclone_remote_bucket_sub]' priority parallel [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
    "--standard_output_processed_file" ) standard_output_processed_file=1;shift;;
    "--switchable_main" ) switchable=1;shift;;
    "--switchable_sub" ) sub=1;switchable=1;shift;;
    "--switchable_sub_end" ) sub=1;switchable=2;shift;;
    "--urgent" ) urgent=1;shift;;
    "--wildcard_index" ) wildcard_index=1;file_from_option=--include-from;shift;;
  esac
done
if test -z $5; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
set +e
source_rclone_remote_bucket_list=`echo $3 | grep ':'`
priority=`echo $4 | grep "^p[1-9]$"`
parallel=`echo $5 | grep '^[0-9]\+$'`
set -e
if test -z "${source_rclone_remote_bucket_list}"; then
  echo "ERROR: $3 is not rclone_remote:bucket." >&2
  exit 199
fi
if test -z ${priority}; then
  echo "ERROR: $4 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
if test -z "${parallel}"; then
  echo "ERROR: $5 is not integer." >&2
  exit 199
elif test $5 -le 0; then
  echo "ERROR: $5 is not more than 1." >&2
  exit 199
fi
if test ${urgent} -eq 1; then
  job_num=4
  time_limit=`expr ${job_period} / ${job_num}`
  deadline=`expr \( ${job_num} - 1 \) \* ${time_limit}`
fi
inclusive_pattern_file=''
if test -n $6; then
  inclusive_pattern_file=$6
fi
exclusive_pattern_file=''
if test -n $7; then
  exclusive_pattern_file=$7
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
