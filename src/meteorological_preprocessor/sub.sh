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
  cp /dev/null ${work_directory}/${priority}_err_log.tmp
  return_code=255
  source_rclone_remote_bucket_main_sub_list_length=`echo "${source_rclone_remote_bucket_main_sub_list}" | sed -e 's|;;|\n|g' | wc -l`
  source_rclone_remote_bucket_main_sub_counter=1
  for source_rclone_remote_bucket_main_sub in `echo "${source_rclone_remote_bucket_main_sub_list}" | sed -e 's|;;|\n|g'`; do
    backup_source_rclone_remote_bucket=0
    if test ${source_rclone_remote_bucket_main_sub_counter} -ne 1 -a ${source_rclone_remote_bucket_main_sub_counter} -ne ${source_rclone_remote_bucket_main_sub_list_length}; then
      backup_source_rclone_remote_bucket=1
    fi
    job_start_unixtime=`date -u "+%s"`
    job_start_unixtime=`expr 0 + ${job_start_unixtime}`
    for job_count in `seq ${job_num}`; do
      exit_code=255
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
      cp /dev/null ${work_directory}/${priority}_processed_file.txt
      source_rclone_remote_bucket_exit_code_list=' 0'
      for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
        source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
        source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
        if test ! -d ${source_work_directory}/${priority}; then
          mkdir -p ${source_work_directory}/${priority}
        fi
        cp /dev/null ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.txt
        rm -rf ${source_work_directory}/${pubsub_index_directory}/${priority}
        rm -rf ${source_work_directory}/${search_index_directory}/${priority}
        if test ! -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt; then
          if test ${backup_source_rclone_remote_bucket} -eq 1; then
            cp /dev/null ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
          else
            set +e
            rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
            exit_code=$?
            set -e
            if test ${exit_code} -ne 0; then
              source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e "s|^\(.*\) \([0-9]\+\)$|\1 ${exit_code}|g"`
              echo "ERROR: ${exit_code}: can not get index file list from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >> ${work_directory}/${priority}_err_log.tmp
              rm -f ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
              source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
              continue
            fi
          fi
        fi
        set +e
        rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e "s|^\(.*\) \([0-9]\+\)$|\1 ${exit_code}|g"`
          echo "ERROR: ${exit_code}: can not get index file list from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >> ${work_directory}/${priority}_err_log.tmp
          source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
          continue
        fi
        if test -s ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp; then
          set +e
          diff ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp | grep -F '>' | cut -c3- | grep -v '^ *$' > ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.txt
          set -e
          if test -s ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.txt; then
            sed -e "s|^|/${pubsub_index_directory}/${priority}/|g" ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.txt > ${source_work_directory}/${priority}_${pubsub_index_directory}_newly_created_index.tmp
            set +e
            rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${source_work_directory}/${priority}_${pubsub_index_directory}_newly_created_index.tmp --ignore-checksum --local-no-set-modtime --log-file ${work_directory}/${priority}_err_log.tmp --log-level ${debug_index_file} --low-level-retries 3 --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${source_work_directory}
            exit_code=$?
            set -e
            if test ${exit_code} -ne 0; then
              source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e "s|^\(.*\) \([0-9]\+\)$|\1 ${exit_code}|g"`
              echo "ERROR: ${exit_code}: can not get index file from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >> ${work_directory}/${priority}_err_log.tmp
              source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
              continue
            fi
            ls -1 ${source_work_directory}/${pubsub_index_directory}/${priority} > ${source_work_directory}/${priority}_${pubsub_index_directory}_gotten_new_index.tmp
            set +e
            cmp -s ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.txt ${source_work_directory}/${priority}_${pubsub_index_directory}_gotten_new_index.tmp
            cmp_exit_code_1=$?
            set -e
            if test ${cmp_exit_code_1} -gt 1; then
              exit_code=${cmp_exit_code_1}
              source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e "s|^\(.*\) \([0-9]\+\)$|\1 ${cmp_exit_code_1}|g"`
              echo "ERROR: ${exit_code}: can not compare." >> ${work_directory}/${priority}_err_log.tmp
              source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
              continue
            fi
            set +e
            cmp -s ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.txt ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp
            cmp_exit_code_2=$?
            set -e
            if test ${cmp_exit_code_2} -gt 1; then
              exit_code=${cmp_exit_code_2}
              source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e "s|^\(.*\) \([0-9]\+\)$|\1 ${cmp_exit_code_2}|g"`
              echo "ERROR: ${exit_code}: can not compare." >> ${work_directory}/${priority}_err_log.tmp
              source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
              continue
            fi
            cp /dev/null ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp
            if test ${cmp_exit_code_1} -eq 1 -o ${cmp_exit_code_2} -eq 0; then
              set +e
              rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${priority} > ${source_work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp
              exit_code=$?
              set -e
              if test ${exit_code} -ne 0; then
                source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e "s|^\(.*\) \([0-9]\+\)$|\1 ${exit_code}|g"`
                echo "ERROR: ${exit_code}: can not get index directory list from ${source_rclone_remote_bucket}/${search_index_directory}/${priority}." >> ${work_directory}/${priority}_err_log.tmp
                source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
                continue
              fi
              if test ${backup_source_rclone_remote_bucket} -eq 1; then
                grep -E "^(${backup_date_hour_pattern})" ${source_work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp | sed -e 's|/$||g' > ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp
              else
                sed -e 's|/$||g' ${source_work_directory}/${priority}_${search_index_directory}_date_hour_slash_directory.tmp > ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp
              fi
              if test -s ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp; then
                former_index_file_first_line=`head -1 ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt`
                search_index_directory_exit_code=0
                for date_hour_directory in `tac ${source_work_directory}/${priority}_${search_index_directory}_date_hour_directory.tmp`; do
                  set +e
                  rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${priority}/${date_hour_directory} > ${source_work_directory}/${priority}_${search_index_directory}_minute_second_index.tmp
                  exit_code=$?
                  set -e
                  if test ${exit_code} -ne 0; then
                    search_index_directory_exit_code=${exit_code}
                    source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e "s|^\(.*\) \([0-9]\+\)$|\1 ${exit_code}|g"`
                    echo "ERROR: ${exit_code}: can not get index file list from ${source_rclone_remote_bucket}/${search_index_directory}/${priority}/${date_hour_directory}." >> ${work_directory}/${priority}_err_log.tmp
                    source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
                    break
                  fi
                  sed -e "s|^|${date_hour_directory}|g" ${source_work_directory}/${priority}_${search_index_directory}_minute_second_index.tmp > ${source_work_directory}/${priority}_${search_index_directory}_index.tmp
                  former_index_file_first_line_count=0
                  if test -n "${former_index_file_first_line}"; then
                    former_index_file_first_line_count=`grep -F ${former_index_file_first_line} ${source_work_directory}/${priority}_${search_index_directory}_index.tmp | wc -l`
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
                if test ${backup_source_rclone_remote_bucket} -eq 1; then
                  grep -E "^(${backup_date_hour_ten_minute_pattern})" ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp | sort -u | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${priority}'/${index_file_date_hour}/${index_file_minute_second_extension}' > ${source_work_directory}/${priority}_${search_index_directory}_newly_created_index.tmp
                else
                  cat ${source_work_directory}/${priority}_${search_index_directory}_new_index.tmp | sort -u | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${priority}'/${index_file_date_hour}/${index_file_minute_second_extension}' > ${source_work_directory}/${priority}_${search_index_directory}_newly_created_index.tmp
                fi
                set +e
                rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --files-from-raw ${source_work_directory}/${priority}_${search_index_directory}_newly_created_index.tmp --ignore-checksum --local-no-set-modtime --log-file ${work_directory}/${priority}_err_log.tmp --log-level ${debug_index_file} --low-level-retries 3 --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${source_work_directory}
                exit_code=$?
                set -e
                if test ${exit_code} -ne 0; then
                  source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e "s|^\(.*\) \([0-9]\+\)$|\1 ${exit_code}|g"`
                  echo "ERROR: ${exit_code}: can not get index file from ${source_rclone_remote_bucket}/${search_index_directory}." >> ${work_directory}/${priority}_err_log.tmp
                  source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
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
              grep -v -F -f ${work_directory}/${priority}_all_processed_file.txt ${source_work_directory}/${priority}_newly_created_file.tmp | grep -v -F -f ${work_directory}/${priority}_processed_file.txt > ${source_work_directory}/${priority}_filtered_newly_created_file.tmp
              set -e
            fi
            if test -s ${source_work_directory}/${priority}_filtered_newly_created_file.tmp; then
              cp /dev/null ${source_work_directory}/${priority}_info_log.tmp
              set +e
              rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious ${file_from_option} ${source_work_directory}/${priority}_filtered_newly_created_file.tmp --ignore-checksum --local-no-set-modtime --log-file ${source_work_directory}/${priority}_info_log.tmp --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${local_work_directory}
              exit_code=$?
              set -e
              if test ${exit_code} -ne 0; then
                source_rclone_remote_bucket_exit_code_list=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e "s|^\(.*\) \([0-9]\+\)$|\1 ${exit_code}|g"`
                set +e
                grep -F ERROR ${source_work_directory}/${priority}_info_log.tmp >> ${work_directory}/${priority}_err_log.tmp
                set -e
                echo "ERROR: ${exit_code}: can not get file from ${source_rclone_remote_bucket}." >> ${work_directory}/${priority}_err_log.tmp
                source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
                continue
              fi
              grep "^.* INFO *: *.* *:.* Copied .*$" ${source_work_directory}/${priority}_info_log.tmp | sed -e "s|^.* INFO *: *\(.*\) *:.* Copied .*$|/\1|g" >> ${work_directory}/${priority}_processed_file.txt
            fi
          fi
        fi
        source_rclone_remote_bucket_exit_code_list="${source_rclone_remote_bucket_exit_code_list} 0"
      done
      if test -s ${work_directory}/${priority}_processed_file.txt; then
        now=`date -u "+%Y%m%d%H%M%S"`
        cp ${work_directory}/${priority}_processed_file.txt ${work_directory}/${priority}_processed/${now}.txt
        if test ${standard_output_processed_file} -eq 1; then
          cat ${work_directory}/${priority}_processed_file.txt
        fi
      fi
      source_rclone_remote_bucket_count=1
      for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
        source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
        source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
        source_rclone_remote_bucket_exit_code=`echo "${source_rclone_remote_bucket_exit_code_list}" | sed -e 's|^ ||g' | cut -d' ' -f${source_rclone_remote_bucket_count}`
        if test -s ${source_work_directory}/${priority}_${pubsub_index_directory}_index_diff.txt; then
          if test "${source_rclone_remote_bucket_exit_code}" = '0'; then
            mv -f ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt
            if test -s ${work_directory}/${priority}_err_log.tmp; then
              echo "INFO: ${source_rclone_remote_bucket} ${source_rclone_remote_bucket_exit_code}: moved ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp" >> ${work_directory}/${priority}_err_log.tmp
            fi
          else
            echo "ERROR: ${source_rclone_remote_bucket} ${source_rclone_remote_bucket_exit_code}: no move ${source_work_directory}/${priority}_${pubsub_index_directory}_new_index.tmp" >> ${work_directory}/${priority}_err_log.tmp
          fi
        fi
        source_rclone_remote_bucket_count=`expr 1 + ${source_rclone_remote_bucket_count}`
      done
      ls -1 ${work_directory}/${priority}_processed/* | grep -v -F "${work_directory}/${priority}_processed/dummy.tmp" | grep -v -E "^${work_directory}/${priority}_processed/(${delete_index_date_hour_pattern})[0-9][0-9][0-9][0-9]\.txt$" | xargs -r rm -f
      ls -1 ${work_directory}/${priority}_processed/* | xargs -r cat > ${work_directory}/${priority}_all_processed_file.txt
      if test ${exit_code} -eq 0; then
        backup=1
        for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
          source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
          source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
          tmp_backup=`find ${source_work_directory}/${priority}_${pubsub_index_directory}_index.txt -type f -mmin +${no_update_index_backup_minute} | wc -l`
          set +e
          backup=`expr ${tmp_backup} \* ${backup}`
          set -e
        done
        if test ${backup} -ne 1; then
          return_code=0
        elif test ${source_rclone_remote_bucket_main_sub_counter} -eq ${source_rclone_remote_bucket_main_sub_list_length}; then
          return_code=0
        else
          return_code=255
        fi
      else
        return_code=${exit_code}
      fi
    done
    if test ${return_code} -eq 0;then
      break
    fi
    source_rclone_remote_bucket_main_sub_counter=`expr 1 + ${source_rclone_remote_bucket_main_sub_counter}`
  done
  if test -s ${work_directory}/${priority}_err_log.tmp; then
    cat ${work_directory}/${priority}_err_log.tmp >&2
  fi
  return ${return_code}
}
bandwidth_limit_k_bytes_per_s=0
cron=0
cutoff=16M
debug_index_file=ERROR
datetime=`date -u "+%Y%m%d%H%M%S"`
datetime_date=`echo ${datetime} | cut -c1-8`
datetime_hour=`echo ${datetime} | cut -c9-10`
delete_index_date_hour_pattern=${datetime_date}${datetime_hour}
delete_index_hour=24
for hour_count in `seq ${delete_index_hour}`; do
  delete_index_date_hour_pattern="${delete_index_date_hour_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour ago" "+%Y%m%d%H"`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour" "+%Y%m%d%H"`
done
file_from_option=--files-from-raw
job_directory=4Sub
job_num=1
job_period=60
pubsub_index_directory=4PubSub
search_index_directory=4Search
standard_output_processed_file=0
backup_date_hour_pattern=${datetime_date}${datetime_hour}
backup_hour=1
for hour_count in `seq ${backup_hour}`; do
  backup_date_hour_pattern="${backup_date_hour_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour ago" "+%Y%m%d%H"`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour" "+%Y%m%d%H"`
done
backup_date_hour_ten_minute_pattern=${datetime_date}${datetime_hour}`echo ${datetime} | cut -c11`
backup_ten_minute=`expr 60 \* ${backup_hour}`
for ten_minute_count in `seq 10 10 ${backup_ten_minute}`; do
  backup_date_hour_ten_minute_pattern="${backup_date_hour_ten_minute_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${ten_minute_count} minute ago" "+%Y%m%d%H%M" | cut -c1-11`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${ten_minute_count} minute" "+%Y%m%d%H%M" | cut -c1-11`
done
no_update_index_backup_minute=5
timeout=8s
urgent=0
wildcard_index=0
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") shift;bandwidth_limit_k_bytes_per_s=$1;shift;;
    "--cron" ) cron=1;shift;;
    "--debug_shell" ) set -evx;shift;;
    "--debug_index_file" ) debug_index_file=INFO;shift;;
    '--help' ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--cron] [--debug_shell] [--debug_index_file] [--standard_output_processed_file] [--urgent] [--wildcard_index] local_work_directory unique_job_name 'source_rclone_remote_bucket_main[;source_rclone_remote_bucket_sub][;;backup_source_rclone_remote_bucket_main[;backup_source_rclone_remote_bucket_sub]]' priority parallel [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
    "--standard_output_processed_file" ) standard_output_processed_file=1;shift;;
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
source_rclone_remote_bucket_main_sub_list=`echo $3 | grep -F ':'`
priority=`echo $4 | grep "^p[1-9]$"`
parallel=`echo $5 | grep '^[0-9]\+$'`
set -e
if test -z "${source_rclone_remote_bucket_main_sub_list}"; then
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
mkdir -p ${work_directory}/${priority}_processed
cp /dev/null ${work_directory}/${priority}_processed/dummy.tmp
touch ${work_directory}/${priority}_all_processed_file.txt
if test ${cron} -eq 1; then
  if test -s ${work_directory}/pid.txt; then
    running=`cat ${work_directory}/pid.txt | xargs -r ps ho "pid comm args" | grep -F " $0 " | grep -F " ${unique_job_name} " | wc -l`
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
