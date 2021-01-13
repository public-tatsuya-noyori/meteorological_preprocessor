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
      if test ! -d ${work_directory}/${source_rclone_remote_bucket_directory}/index; then
        mkdir -p ${work_directory}/${source_rclone_remote_bucket_directory}/index
      fi
      cp /dev/null ${work_directory}/${source_rclone_remote_bucket_directory}/index/dummy.tmp
      if test ! -f ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt; then
        set +e
        rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt
        tmp_exit_code=$?
        set -e
        if test ${tmp_exit_code} -ne 0; then
          source_rclone_remote_bucket_exit_code=${tmp_exit_code}
          rm -f ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt
          continue
        fi
      fi
      set +e
      rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp
      tmp_exit_code=$?
      set -e
      if test ${tmp_exit_code} -ne 0; then
        source_rclone_remote_bucket_exit_code=${tmp_exit_code}
        continue
      fi
      if test -s ${work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp; then
        diff ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt ${work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp | grep '>' | cut -c3- > ${work_directory}/${priority}_index_diff.tmp
        if test -s ${work_directory}/${priority}_index_diff.tmp; then
          cmp_exit_code=0
          set +e
          cmp -s ${work_directory}/${priority}_index_diff.tmp ${work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp
          cmp_exit_code=$?
          set -e
          cp /dev/null ${work_directory}/${priority}_newly_created_index.tmp
          if test ${cmp_exit_code} -eq 0; then
            set +e
            rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${priority} > ${work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp
            tmp_exit_code=$?
            set -e
            if test ${tmp_exit_code} -ne 0; then
              source_rclone_remote_bucket_exit_code=${tmp_exit_code}
              continue
            fi
            if test ${backup} -eq 1; then
              grep -E "^(${date_hour_pattern})" ${work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp | sed -e 's|/$||g' > ${work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp
            else
              sed -e 's|/$||g' ${work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp > ${work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp
            fi
            if test -s ${work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp; then
              cp /dev/null ${work_directory}/${priority}_${search_index_directory}_index.tmp
              former_index_file_first_line=`head -1 ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt`
              for date_hour_directory in `tac ${work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp`; do
                set +e
                rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${priority}/${date_hour_directory} > ${work_directory}/${priority}_${search_index_directory}_minute_second_index.tmp
                tmp_exit_code=$?
                set -e
                if test ${tmp_exit_code} -ne 0; then
                  source_rclone_remote_bucket_exit_code=${tmp_exit_code}
                  continue
                fi
                sed -e "s|^|${date_hour_directory}|g" ${work_directory}/${priority}_${search_index_directory}_minute_second_index.tmp >> ${work_directory}/${priority}_${search_index_directory}_index.tmp
                cat ${work_directory}/${priority}_${search_index_directory}_index.tmp ${work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp | sort -u > ${work_directory}/${priority}_${pubsub_index_directory}_${search_index_directory}_new_index.tmp
                is_former_index_file_first_line=`grep ${former_index_file_first_line} ${work_directory}/${priority}_${pubsub_index_directory}_${search_index_directory}_new_index.tmp | wc -l`
                if test ${is_former_index_file_first_line} -eq 0; then
                  diff ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt ${work_directory}/${priority}_${pubsub_index_directory}_${search_index_directory}_new_index.tmp | grep '>' | cut -c3- > ${work_directory}/${priority}_index_diff.tmp
                else
                  sed -ne "/${former_index_file_first_line}/,\$p" ${work_directory}/${priority}_${pubsub_index_directory}_${search_index_directory}_new_index.tmp > ${work_directory}/${priority}_${pubsub_index_directory}_${search_index_directory}_filtered_new_index.tmp
                  diff ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt ${work_directory}/${priority}_${pubsub_index_directory}_${search_index_directory}_filtered_new_index.tmp | grep '>' | cut -c3- > ${work_directory}/${priority}_index_diff.tmp
                fi
                set +e
                cmp -s ${work_directory}/${priority}_index_diff.tmp ${work_directory}/${priority}_${pubsub_index_directory}_${search_index_directory}_new_index.tmp
                cmp_exit_code=$?
                set -e
                if test ${cmp_exit_code} -ne 0; then
                  break
                fi
              done
              grep -f ${work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp ${work_directory}/${priority}_index_diff.tmp | sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" > ${work_directory}/${priority}_newly_created_index.tmp
              if test ${backup} -eq 1; then
                date_hour_ten_minute_pattern=`date -u "+%Y%m%d%H%M" | cut -c1-11`
                ten_minute_ago=`expr 60 \* ${hour_ago}`
                ten_minute_count=10
                while test ${ten_minute_count} -le ${ten_minute_ago}; do
                  date_hour_ten_minute_pattern="${date_hour_ten_minute_pattern}|"`date -u "+%Y%m%d%H%M" -d "${ten_minute_count} minute ago" | cut -c1-11`"|"`date -u "+%Y%m%d%H%M" -d -"${ten_minute_count} minute ago" | cut -c1-11`
                  ten_minute_count=`expr 10 + ${ten_minute_count}`
                done
                grep -f ${work_directory}/${priority}_${search_index_directory}_index.tmp ${work_directory}/${priority}_index_diff.tmp | grep -E "^(${date_hour_ten_minute_pattern})" | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${priority}'/${index_file_date_hour}/${index_file_minute_second_extension}' >> ${work_directory}/${priority}_newly_created_index.tmp
              else
                grep -f ${work_directory}/${priority}_${search_index_directory}_index.tmp ${work_directory}/${priority}_index_diff.tmp | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${priority}'/${index_file_date_hour}/${index_file_minute_second_extension}' >> ${work_directory}/${priority}_newly_created_index.tmp
              fi
            fi
          elif test ${cmp_exit_code} -eq 1; then
            sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" ${work_directory}/${priority}_index_diff.tmp > ${work_directory}/${priority}_newly_created_index.tmp
          fi
          if test ${cmp_exit_code} -ne 0 -a ${cmp_exit_code} -ne 1; then
            source_rclone_remote_bucket_exit_code=${cmp_exit_code}
            continue
          fi
          if test -s ${work_directory}/${priority}_newly_created_index.tmp; then
            rm -rf ${work_directory}/${pubsub_index_directory}/${priority} ${work_directory}/${search_index_directory}/${priority}
            set +e
            rclone copy --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${work_directory}/${priority}_newly_created_index.tmp --ignore-checksum --local-no-set-modtime --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${work_directory}
            tmp_exit_code=$?
            set -e
            if test ${tmp_exit_code} -ne 0; then
              source_rclone_remote_bucket_exit_code=${tmp_exit_code}
              continue
            fi
            cp /dev/null ${work_directory}/${priority}_newly_created_file.tmp
            if test -n "${inclusive_pattern_file}"; then
              set +e
              if test -n "${exclusive_pattern_file}"; then
                if test -d ${work_directory}/${search_index_directory}/${priority}; then
                  ls -1 ${work_directory}/${search_index_directory}/${priority}/*/* ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/${priority}_newly_created_file.tmp
                else
                  ls -1 ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/${priority}_newly_created_file.tmp
                fi
              else
                if test -d ${work_directory}/${search_index_directory}/${priority}; then
                  ls -1 ${work_directory}/${search_index_directory}/${priority}/*/* ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -E -f ${inclusive_pattern_file} > ${work_directory}/${priority}_newly_created_file.tmp
                else
                  ls -1 ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat | grep -E -f ${inclusive_pattern_file} > ${work_directory}/${priority}_newly_created_file.tmp
                fi
              fi
              set -e
            else
              if test -d ${work_directory}/${search_index_directory}/${priority}; then
                ls -1 ${work_directory}/${search_index_directory}/${priority}/*/* ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat > ${work_directory}/${priority}_newly_created_file.tmp
              else
                ls -1 ${work_directory}/${pubsub_index_directory}/${priority}/* | xargs -r cat > ${work_directory}/${priority}_newly_created_file.tmp
              fi
            fi
            if test -s ${work_directory}/${priority}_newly_created_file.tmp -a ${backup} -eq 1; then
              ls -1 ${work_directory}/*/index/* | grep "^${work_directory}/[^/]*/index/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]\.txt$" | xargs -r cat | sort -u > ${work_directory}/${priority}_file_filter.tmp
              set +e
              grep -v -f ${work_directory}/${priority}_file_filter.tmp ${work_directory}/${priority}_newly_created_file.tmp > ${work_directory}/${priority}_filtered_newly_created_file.tmp
              set -e
              mv -f ${work_directory}/${priority}_filtered_newly_created_file.tmp ${work_directory}/${priority}_newly_created_file.tmp
            fi
            if test -s ${work_directory}/${priority}_newly_created_file.tmp; then
              cp /dev/null ${work_directory}/${priority}_log.tmp
              set +e
              if test ${wildcard_index} -eq 1; then
                rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --ignore-checksum --include-from ${work_directory}/${priority}_newly_created_file.tmp --log-file ${work_directory}/${priority}_log.tmp --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${local_work_directory}
              else
                rclone copy --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/${priority}_newly_created_file.tmp --ignore-checksum --log-file ${work_directory}/${priority}_log.tmp --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${local_work_directory}
              fi
              tmp_exit_code=$?
              set -e
              if test ${tmp_exit_code} -ne 0; then
                source_rclone_remote_bucket_exit_code=${tmp_exit_code}
                set +e
                grep ERROR ${work_directory}/${priority}_log.tmp >&2
                set -e
              elif test -s ${work_directory}/${priority}_log.tmp; then
                sed -e "s|^.* INFO *: *\(.*\) *: Copied .*$|${local_work_directory}/\1|g" ${work_directory}/${priority}_log.tmp
              fi
            fi
            if test ${source_rclone_remote_bucket_exit_code} -eq 0; then
              if test -d ${work_directory}/${search_index_directory}/${priority}; then
                ls -1 ${work_directory}/${search_index_directory}/${priority}/*/* | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_name=`echo ${index_file} | sed -e "s|'${work_directory}/${search_index_directory}/${priority}/'||g" -e "s|/||g"`;mv -f ${index_file} '${work_directory}/${source_rclone_remote_bucket_directory}'/index/${index_file_name}'
              fi
              mv -f ${work_directory}/${pubsub_index_directory}/${priority}/* ${work_directory}/${source_rclone_remote_bucket_directory}/index/
            fi
          fi
          if test ${source_rclone_remote_bucket_exit_code} -eq 0; then
            mv -f ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt.old
            mv -f ${work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt
            ls -1 ${work_directory}/${source_rclone_remote_bucket_directory}/index/* | grep -v -E "^${work_directory}/${source_rclone_remote_bucket_directory}/index/(${date_hour_pattern})[0-9][0-9][0-9][0-9]\.txt$" | xargs -r rm -f
          fi
        else
          if test ${source_rclone_remote_bucket_exit_code} -eq 0; then
            mv -f ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt.old
            mv -f ${work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt
            ls -1 ${work_directory}/${source_rclone_remote_bucket_directory}/index/* | grep -v -E "^${work_directory}/${source_rclone_remote_bucket_directory}/index/(${date_hour_pattern})[0-9][0-9][0-9][0-9]\.txt$" | xargs -r rm -f
          fi
          if test ${switchable} -eq 1; then
            is_switch=`find ${work_directory}/${source_rclone_remote_bucket_directory}/${priority}_index.txt -type f -mmin +${switchable_minute} | wc -l`
            if test ${is_switch} -eq 1; then
              source_rclone_remote_bucket_exit_code=255
              continue
            fi
          fi
        fi
      fi
      if test ${source_rclone_remote_bucket_exit_code} -ne 0; then
        exit_code=${source_rclone_remote_bucket_exit_code}
      fi
    done
    job_count=`expr 1 + ${job_count}`
  done
  return ${exit_code}
}
cron=0
cutoff=16M
hour_ago=1
job_directory=4Sub
job_num=1
job_period=60
job_start_unixtime=`date -u "+%s"`
job_start_unixtime=`expr 0 + ${job_start_unixtime}`
pubsub_index_directory=4PubSub
search_index_directory=4Search
switchable=0
switchable_minute=5
timeout=8s
backup=0
urgent=0
wildcard_index=0
date_hour_pattern=`date -u "+%Y%m%d%H"`
hour_count=1
while test ${hour_count} -le ${hour_ago}; do
  date_hour_pattern="${date_hour_pattern}|"`date -u "+%Y%m%d%H" -d "${hour_count} hour ago"`"|"`date -u "+%Y%m%d%H" -d -"${hour_count} hour ago"`
  hour_count=`expr 1 + ${hour_count}`
done
for arg in "$@"; do
  case "${arg}" in
    "--backup" ) backup=1;shift;;
    "--cron" ) cron=1;shift;;
    '--help' ) echo "$0 [--backup] [--cron] [--wildcard_index] [--switchable] [--urgent] local_work_directory unique_job_name 'source_rclone_remote_bucket_main[;source_rclone_remote_bucket_sub]' priority parallel [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
    "--wildcard_index" ) wildcard_index=1;shift;;
    "--switchable" ) switchable=1;shift;;
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
