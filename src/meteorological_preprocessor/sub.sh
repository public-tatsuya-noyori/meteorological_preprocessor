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
    for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_list} | tr ';' '\n'`; do
      source_rclone_remote_bucket_exit_code=0
      source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
      source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
      if test ! -d ${source_work_directory}/index; then
        mkdir -p ${source_work_directory}/index
      fi
      cp /dev/null ${source_work_directory}/${priority}_ok.tmp
      cp /dev/null ${source_work_directory}/index/dummy.tmp
      rm -rf ${source_work_directory}/${pubsub_index_directory}/${priority}
      rm -rf ${source_work_directory}/${search_index_directory}/${priority}
      if test ! -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt; then
        if test ${sub} -eq 1; then
          cp /dev/null ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
        else
          set +e
          rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
          tmp_exit_code=$?
          set -e
          if test ${tmp_exit_code} -ne 0; then
            source_rclone_remote_bucket_exit_code=${tmp_exit_code}
            exit_code=${source_rclone_remote_bucket_exit_code}
            echo "ERROR: can not get index file list from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >&2
            rm -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
            continue
          fi
        fi
      fi
      set +e
      rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp
      tmp_exit_code=$?
      set -e
      if test ${tmp_exit_code} -ne 0; then
        source_rclone_remote_bucket_exit_code=${tmp_exit_code}
        exit_code=${source_rclone_remote_bucket_exit_code}
        echo "ERROR: can not get index file list from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >&2
        continue
      fi
      if test -s ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp; then
        diff ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp | grep '>' | cut -c3- > ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.tmp
        if test -s ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.tmp; then
          sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.tmp > ${source_work_directory}/${priority}_${pubsub_index_directory}_newly_created_index.tmp
          set +e
          rclone copy --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${source_work_directory}/${priority}_${pubsub_index_directory}_newly_created_index.tmp --ignore-checksum --local-no-set-modtime --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${work_directory}/${source_rclone_remote_bucket_directory}
          tmp_exit_code=$?
          set -e
          if test ${tmp_exit_code} -ne 0; then
            source_rclone_remote_bucket_exit_code=${tmp_exit_code}
            exit_code=${source_rclone_remote_bucket_exit_code}
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
            rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${priority} > ${source_work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp
            tmp_exit_code=$?
            set -e
            if test ${tmp_exit_code} -ne 0; then
              source_rclone_remote_bucket_exit_code=${tmp_exit_code}
              exit_code=${source_rclone_remote_bucket_exit_code}
              echo "ERROR: can not get index directory list from ${source_rclone_remote_bucket}/${search_index_directory}/${priority}." >&2
              continue
            fi
            if test ${sub} -eq 1; then
              grep -E "^(${date_hour_pattern})" ${source_work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp | sed -e 's|/$||g' > ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp
            else
              sed -e 's|/$||g' ${source_work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp > ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp
            fi
            if test -s ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp; then
              former_index_file_first_line=`head -1 ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt`
              for date_hour_directory in `tac ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp`; do
                set +e
                rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${priority}/${date_hour_directory} > ${source_work_directory}/${priority}_${search_index_directory}_minute_second_index.tmp
                tmp_exit_code=$?
                set -e
                if test ${tmp_exit_code} -ne 0; then
                  source_rclone_remote_bucket_exit_code=${tmp_exit_code}
                  exit_code=${source_rclone_remote_bucket_exit_code}
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
                  grep -v -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt ${source_work_directory}/${priority}_${search_index_directory}_index.tmp | grep -v -f ${source_work_directory}/${priority}_${pubsub_index_directory}_gotten_new_index.tmp >> ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp
                  set -e
                else
                  set +e
                  sed -ne "/${former_index_file_first_line}/,\$p" ${source_work_directory}/${priority}_${search_index_directory}_index.tmp | grep -v -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt | grep -v -f ${source_work_directory}/${priority}_${pubsub_index_directory}_gotten_new_index.tmp >> ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp
                  set -e
                  break
                fi
              done
              if test ${source_rclone_remote_bucket_exit_code} -ne 0; then
                continue
              fi
              if test ${sub} -eq 1; then
                date_hour_ten_minute_pattern=`date -u "+%Y%m%d%H%M" | cut -c1-11`
                ten_minute_ago=`expr 60 \* ${hour_ago}`
                ten_minute_count=10
                while test ${ten_minute_count} -le ${ten_minute_ago}; do
                  date_hour_ten_minute_pattern="${date_hour_ten_minute_pattern}|"`date -u "+%Y%m%d%H%M" -d "${ten_minute_count} minute ago" | cut -c1-11`"|"`date -u "+%Y%m%d%H%M" -d -"${ten_minute_count} minute ago" | cut -c1-11`
                  ten_minute_count=`expr 10 + ${ten_minute_count}`
                done
                grep -E "^(${date_hour_ten_minute_pattern})" ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp | sort -u | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${priority}'/${index_file_date_hour}/${index_file_minute_second_extension}' > ${source_work_directory}/${priority}_${search_index_directory}_newly_created_index.tmp
              else
                cat ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp | sort -u | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${priority}'/${index_file_date_hour}/${index_file_minute_second_extension}' > ${source_work_directory}/${priority}_${search_index_directory}_newly_created_index.tmp
              fi
              set +e
              rclone copy --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${source_work_directory}/${priority}_${search_index_directory}_newly_created_index.tmp --ignore-checksum --local-no-set-modtime --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${work_directory}/${source_rclone_remote_bucket_directory}
              tmp_exit_code=$?
              set -e
              if test ${tmp_exit_code} -ne 0; then
                source_rclone_remote_bucket_exit_code=${tmp_exit_code}
                exit_code=${source_rclone_remote_bucket_exit_code}
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
            ls -1 ${work_directory}/*/index/* | grep "^${work_directory}/[^/]*/index/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]\.txt$" | xargs -r cat | sort -u > ${source_work_directory}/${priority}_file_filter.tmp
            set +e
            grep -v -f ${source_work_directory}/${priority}_file_filter.tmp ${source_work_directory}/${priority}_newly_created_file.tmp > ${source_work_directory}/${priority}_filtered_newly_created_file.tmp
            set -e
          fi
          if test -s ${source_work_directory}/${priority}_filtered_newly_created_file.tmp; then
            cp /dev/null ${source_work_directory}/${priority}_log.tmp
            set +e
            if test ${wildcard_index} -eq 1; then
              rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --ignore-checksum --include-from ${source_work_directory}/${priority}_filtered_newly_created_file.tmp --log-file ${source_work_directory}/${priority}_log.tmp --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${local_work_directory}
            else
              rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${source_work_directory}/${priority}_filtered_newly_created_file.tmp --ignore-checksum --log-file ${source_work_directory}/${priority}_log.tmp --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${local_work_directory}
            fi
            tmp_exit_code=$?
            set -e
            if test ${tmp_exit_code} -ne 0; then
              source_rclone_remote_bucket_exit_code=${tmp_exit_code}
              exit_code=${source_rclone_remote_bucket_exit_code}
              set +e
              grep ERROR ${source_work_directory}/${priority}_log.tmp >&2
              set -e
              echo "ERROR: can not get file from ${source_rclone_remote_bucket}." >&2
              continue
            fi
            sed -e "s|^.* INFO *: *\(.*\) *: Copied .*$|${local_work_directory}/\1|g" ${source_work_directory}/${priority}_log.tmp
            echo 1 > ${source_work_directory}/${priority}_ok.tmp
          fi
          if test -d ${source_work_directory}/${search_index_directory}/${priority}; then
            ls -1 ${source_work_directory}/${search_index_directory}/${priority}/*/* | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_name=`echo ${index_file} | sed -e "s|'${source_work_directory}/${search_index_directory}/${priority}/'||g" -e "s|/||g"`;mv -f ${index_file} '${work_directory}/${source_rclone_remote_bucket_directory}'/index/${index_file_name}'
          fi
          if test -d ${source_work_directory}/${pubsub_index_directory}/${priority}; then
            mv -f ${source_work_directory}/${pubsub_index_directory}/${priority}/* ${source_work_directory}/index/
          fi
        fi
      fi
    done
    for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_list} | tr ';' '\n'`; do
      source_rclone_remote_bucket_exit_code=0
      source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
      source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
      if test -s ${source_work_directory}/${priority}_ok.tmp; then
        mv -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt.old
        mv -f ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
      fi
      ls -1 ${source_work_directory}/index/* | grep -v "^${source_work_directory}/index/dummy\.tmp$" | grep -v -E "^${source_work_directory}/index/(${date_hour_pattern})[0-9][0-9][0-9][0-9]\.txt$" | xargs -r rm -f
    done
    job_count=`expr 1 + ${job_count}`
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
cron=0
cutoff=16M
date_hour_pattern=`date -u "+%Y%m%d%H"`
hour_ago=1
hour_count=1
while test ${hour_count} -le ${hour_ago}; do
  date_hour_pattern="${date_hour_pattern}|"`date -u "+%Y%m%d%H" -d "${hour_count} hour ago"`"|"`date -u "+%Y%m%d%H" -d -"${hour_count} hour ago"`
  hour_count=`expr 1 + ${hour_count}`
done
job_directory=4Sub
job_num=1
job_period=60
job_start_unixtime=`date -u "+%s"`
job_start_unixtime=`expr 0 + ${job_start_unixtime}`
pubsub_index_directory=4PubSub
search_index_directory=4Search
sub=0
switchable=0
switchable_minute=5
timeout=8s
urgent=0
wildcard_index=0
for arg in "$@"; do
  case "${arg}" in
    "--cron" ) cron=1;shift;;
    "--debug" ) set -evx;shift;;
    '--help' ) echo "$0 [--cron] [--wildcard_index] [--switchable_main/--switchable_sub/--switchable_sub_end] [--urgent] local_work_directory unique_job_name 'source_rclone_remote_bucket_main[;source_rclone_remote_bucket_sub]' priority parallel [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
    "--wildcard_index" ) wildcard_index=1;shift;;
    "--switchable_main" ) switchable=1;shift;;
    "--switchable_sub" ) sub=1;switchable=1;shift;;
    "--switchable_sub_end" ) sub=1;switchable=2;shift;;
    "--urgent" ) urgent=1;shift;;
  esac
done
if test -z $5; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
unique_job_name=$2
source_rclone_remote_bucket_list=$3
set +e
priority=`echo $4 | grep "^p[1-9]$"`
parallel=`echo $5 | grep '^[0-9]\+$'`
set -e
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
