#!/bin/bash
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
IFS=$'\n'
clone() {
  return_code=0
  exit_code=0
  for destination_rclone_remote_bucket in `echo ${destination_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 ${rclone_timeout} rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${destination_rclone_remote_bucket}/${pubsub_index_directory} > /dev/null
    exit_code=$?
    set -e
    if test ${exit_code} -eq 0; then
      break
    else
      cat ${work_directory}/err_log.tmp >&2
      echo "WARNING: can not access on ${destination_rclone_remote_bucket}." >&2
    fi
  done
  if test ${exit_code} -ne 0; then
    echo "ERROR: can not access on ${destination_rclone_remote_bucket_main_sub}." >&2
    return ${exit_code}
  fi
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    if test ${exit_code} -ne 0; then
      return_code=${exit_code}
    fi
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
    mkdir -p ${source_work_directory}
    rm -rf ${source_work_directory}/${pubsub_index_directory}/${txt_or_bin}
    rm -rf ${source_work_directory}/${search_index_directory}/${txt_or_bin}
    if test ! -f ${source_work_directory}/${pubsub_index_directory}_index.txt; then
      cp /dev/null ${source_work_directory}/err_log.tmp
      set +e
      timeout -k 3 ${rclone_timeout} rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin} > ${source_work_directory}/${pubsub_index_directory}_index.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        cat ${source_work_directory}/err_log.tmp >&2
        echo "ERROR: ${exit_code}: can not get index file list from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}." >&2
        rm -f ${source_work_directory}/${pubsub_index_directory}_index.txt
        continue
      fi
    fi
    cp /dev/null ${source_work_directory}/${pubsub_index_directory}_new_index.tmp
    cp /dev/null ${source_work_directory}/err_log.tmp
    set +e
    timeout -k 3 ${rclone_timeout} rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin} > ${source_work_directory}/${pubsub_index_directory}_new_index.tmp
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      cat ${source_work_directory}/err_log.tmp >&2
      echo "ERROR: ${exit_code}: can not get index file list from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}." >&2
      continue
    fi
    if test -s ${source_work_directory}/${pubsub_index_directory}_new_index.tmp; then
      cp /dev/null ${source_work_directory}/${pubsub_index_directory}_index_diff.txt
      set +e
      diff ${source_work_directory}/${pubsub_index_directory}_index.txt ${source_work_directory}/${pubsub_index_directory}_new_index.tmp | grep -F '>' | cut -c3- | grep -v '^ *$' > ${source_work_directory}/${pubsub_index_directory}_index_diff.txt
      set -e
      if test -s ${source_work_directory}/${pubsub_index_directory}_index_diff.txt; then
        sed -e "s|^|/${pubsub_index_directory}/${txt_or_bin}/|g" ${source_work_directory}/${pubsub_index_directory}_index_diff.txt > ${source_work_directory}/${pubsub_index_directory}_newly_created_index.tmp
        cp /dev/null ${source_work_directory}/err_log.tmp
        set +e
        timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --files-from-raw ${source_work_directory}/${pubsub_index_directory}_newly_created_index.tmp --local-no-set-modtime --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${source_work_directory}
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          cat ${source_work_directory}/err_log.tmp >&2
          echo "ERROR: ${exit_code}: can not get index file from ${source_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}." >&2
          continue
        fi
        ls -1 ${source_work_directory}/${pubsub_index_directory}/${txt_or_bin} > ${source_work_directory}/${pubsub_index_directory}_gotten_new_index.tmp
        set +e
        cmp -s ${source_work_directory}/${pubsub_index_directory}_index_diff.txt ${source_work_directory}/${pubsub_index_directory}_gotten_new_index.tmp
        cmp_exit_code_1=$?
        set -e
        if test ${cmp_exit_code_1} -gt 1; then
          exit_code=${cmp_exit_code_1}
          echo "ERROR: ${exit_code}: can not compare." >&2
          continue
        fi
        set +e
        cmp -s ${source_work_directory}/${pubsub_index_directory}_index_diff.txt ${source_work_directory}/${pubsub_index_directory}_new_index.tmp
        cmp_exit_code_2=$?
        set -e
        if test ${cmp_exit_code_2} -gt 1; then
          exit_code=${cmp_exit_code_2}
          echo "ERROR: ${exit_code}: can not compare." >&2
          continue
        fi
        cp /dev/null ${source_work_directory}/${search_index_directory}_new_index.tmp
        if test ${cmp_exit_code_1} -eq 1 -o ${cmp_exit_code_2} -eq 0; then
          cp /dev/null ${source_work_directory}/err_log.tmp
          set +e
          timeout -k 3 ${rclone_timeout} rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${txt_or_bin} > ${source_work_directory}/${search_index_directory}_date_hour_slash_directory.tmp
          exit_code=$?
          set -e
          if test ${exit_code} -ne 0; then
            cat ${source_work_directory}/err_log.tmp >&2
            echo "ERROR: ${exit_code}: can not get index directory list from ${source_rclone_remote_bucket}/${search_index_directory}/${txt_or_bin}." >&2
            continue
          fi
          sed -e 's|/$||g' ${source_work_directory}/${search_index_directory}_date_hour_slash_directory.tmp > ${source_work_directory}/${search_index_directory}_date_hour_directory.tmp
          if test -s ${source_work_directory}/${search_index_directory}_date_hour_directory.tmp; then
            former_index_file_first_line_prefix=`head -1 ${source_work_directory}/${pubsub_index_directory}_index.txt | sed -e 's|_.*\.txt\.gz$||g'`
            search_index_directory_exit_code=0
            for date_hour_directory in `tac ${source_work_directory}/${search_index_directory}_date_hour_directory.tmp`; do
              cp /dev/null ${source_work_directory}/err_log.tmp
              set +e
              timeout -k 3 ${rclone_timeout} rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${txt_or_bin}/${date_hour_directory} > ${source_work_directory}/${search_index_directory}_minute_second_index.tmp
              exit_code=$?
              set -e
              if test ${exit_code} -ne 0; then
                search_index_directory_exit_code=${exit_code}
                cat ${source_work_directory}/err_log.tmp >&2
                echo "ERROR: ${exit_code}: can not get index file list from ${source_rclone_remote_bucket}/${search_index_directory}/${txt_or_bin}/${date_hour_directory}." >&2
                break
              fi
              sed -e "s|^|${date_hour_directory}|g" ${source_work_directory}/${search_index_directory}_minute_second_index.tmp > ${source_work_directory}/${search_index_directory}_index.tmp
              former_index_file_first_line_prefix_count=0
              if test -n "${former_index_file_first_line_prefix}"; then
                former_index_file_first_line_prefix_count=`grep -F ${former_index_file_first_line_prefix} ${source_work_directory}/${search_index_directory}_index.tmp | wc -l`
              fi
              if test ${former_index_file_first_line_prefix_count} -eq 0; then
                set +e
                grep -v -F -f ${source_work_directory}/${pubsub_index_directory}_index.txt ${source_work_directory}/${search_index_directory}_index.tmp | grep -v -F -f ${source_work_directory}/${pubsub_index_directory}_gotten_new_index.tmp >> ${source_work_directory}/${search_index_directory}_new_index.tmp
                set -e
              else
                set +e
                sed -ne "/${former_index_file_first_line_prefix}/,\$p" ${source_work_directory}/${search_index_directory}_index.tmp | grep -v -F -f ${source_work_directory}/${pubsub_index_directory}_index.txt | grep -v -F -f ${source_work_directory}/${pubsub_index_directory}_gotten_new_index.tmp >> ${source_work_directory}/${search_index_directory}_new_index.tmp
                set -e
                break
              fi
            done
            if test ${search_index_directory_exit_code} -ne 0; then
              continue
            fi
            cat ${source_work_directory}/${search_index_directory}_new_index.tmp | sort -u | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${txt_or_bin}'/${index_file_date_hour}/${index_file_minute_second_extension}' > ${source_work_directory}/${search_index_directory}_newly_created_index.tmp
            cp /dev/null ${source_work_directory}/err_log.tmp
            set +e
            timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --files-from-raw ${source_work_directory}/${search_index_directory}_newly_created_index.tmp --local-no-set-modtime --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${source_work_directory}
            exit_code=$?
            set -e
            if test ${exit_code} -ne 0; then
              cat ${source_work_directory}/err_log.tmp >&2
              echo "ERROR: ${exit_code}: can not get index file from ${source_rclone_remote_bucket}/${search_index_directory}/${txt_or_bin}." >&2
              continue
            fi
          fi
        fi
        cp /dev/null ${source_work_directory}/newly_created_file.tmp
        if test -n "${inclusive_pattern_file}"; then
          set +e
          if test -n "${exclusive_pattern_file}"; then
            if test -d ${source_work_directory}/${search_index_directory}/${txt_or_bin}; then
              ls -1 ${source_work_directory}/${search_index_directory}/${txt_or_bin}/*/* ${source_work_directory}/${pubsub_index_directory}/${txt_or_bin}/* | xargs -r zcat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/newly_created_file.tmp
            else
              ls -1 ${source_work_directory}/${pubsub_index_directory}/${txt_or_bin}/* | xargs -r zcat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/newly_created_file.tmp
            fi
          else
            if test -d ${source_work_directory}/${search_index_directory}/${txt_or_bin}; then
              ls -1 ${source_work_directory}/${search_index_directory}/${txt_or_bin}/*/* ${source_work_directory}/${pubsub_index_directory}/${txt_or_bin}/* | xargs -r zcat | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/newly_created_file.tmp
            else
              ls -1 ${source_work_directory}/${pubsub_index_directory}/${txt_or_bin}/* | xargs -r zcat | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/newly_created_file.tmp
            fi
          fi
          set -e
        else
          if test -d ${source_work_directory}/${search_index_directory}/${txt_or_bin}; then
            ls -1 ${source_work_directory}/${search_index_directory}/${txt_or_bin}/*/* ${source_work_directory}/${pubsub_index_directory}/${txt_or_bin}/* | xargs -r zcat > ${source_work_directory}/newly_created_file.tmp
          else
            ls -1 ${source_work_directory}/${pubsub_index_directory}/${txt_or_bin}/* | xargs -r zcat > ${source_work_directory}/newly_created_file.tmp
          fi
        fi
        cp /dev/null ${source_work_directory}/filtered_newly_created_file.tmp
        if test -s ${source_work_directory}/newly_created_file.tmp; then
          cp /dev/null ${work_directory}/all_processed_file.txt
          set +e
          ls -1 ${processed_directory} | sed -e "s|^|${processed_directory}/|g" | xargs -r cat >> ${work_directory}/all_processed_file.txt 2>/dev/null
          grep -v -F -f ${work_directory}/all_processed_file.txt ${source_work_directory}/newly_created_file.tmp > ${source_work_directory}/filtered_newly_created_file.tmp
          set -e
        fi
        cp /dev/null ${work_directory}/processed_file.txt
        if test -s ${source_work_directory}/filtered_newly_created_file.tmp; then
          cp /dev/null ${source_work_directory}/info_log.tmp
          set +e
          timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --files-from-raw ${source_work_directory}/filtered_newly_created_file.tmp --log-file ${source_work_directory}/info_log.tmp --log-level DEBUG --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${destination_rclone_remote_bucket}
          exit_code=$?
          set -e
          if test ${exit_code} -ne 0; then
            if test ${exit_code} -eq 124; then
              touch ${source_work_directory}/rclone_timeout.txt
              if test -s ${source_work_directory}/rclone_timeout.txt; then
                echo "ERROR: rclone timeout ${exit_code}: terminated clone file from ${source_rclone_remote_bucket} ${txt_or_bin} to ${destination_rclone_remote_bucket} ${txt_or_bin}." >&2
                rm -f ${source_work_directory}/${pubsub_index_directory}_index.txt
                echo "INFO: clear index: deleted ${source_work_directory}/${pubsub_index_directory}_index.txt." >&2
                cp /dev/null ${source_work_directory}/rclone_timeout.txt
              else
                echo 124 > ${source_work_directory}/rclone_timeout.txt
                echo "ERROR: rclone timeout ${exit_code}: terminated clone file from ${source_rclone_remote_bucket} ${txt_or_bin} to ${destination_rclone_remote_bucket} ${txt_or_bin}." >&2
              fi
            else
              set +e
              grep -F ERROR ${source_work_directory}/info_log.tmp >&2
              set -e
              echo "ERROR: ${exit_code}: can not clone file from ${source_rclone_remote_bucket} ${txt_or_bin} to ${destination_rclone_remote_bucket} ${txt_or_bin}." >&2
            fi
            continue
          fi
          cp /dev/null ${source_work_directory}/rclone_timeout.txt
          set +e
          grep -E "^(.* DEBUG *: *[^ ]* *:.* Unchanged skipping.*|.* INFO *: *[^ ]* *:.* Copied .*)$" ${source_work_directory}/info_log.tmp | sed -e "s|^.* DEBUG *: *\([^ ]*\) *:.* Unchanged skipping.*$|/\1|g" -e "s|^.* INFO *: *\([^ ]*\) *:.* Copied .*$|/\1|g" -e 's|^/||g' | grep -v '^ *$' > ${work_directory}/processed_file.txt
          set -e
        fi
        if test -s ${work_directory}/processed_file.txt; then
          for retry_count in `seq ${retry_num}`; do
            rm -rf ${work_directory}/prepare
            mkdir ${work_directory}/prepare
            now=`date -u "+%Y%m%d%H%M%S"`
            cp ${work_directory}/processed_file.txt ${work_directory}/prepare/${now}_${unique_center_id}.txt
            gzip -f ${work_directory}/prepare/${now}_${unique_center_id}.txt
            cp /dev/null ${work_directory}/err_log.tmp
            set +e
            timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/err_log.tmp --low-level-retries 1 --no-traverse --quiet --retries 1 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} ${work_directory}/prepare ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}
            exit_code=$?
            set -e
            if test ${exit_code} -eq 0; then
              mv ${work_directory}/processed_file.txt ${processed_directory}/${now}_${unique_center_id}.txt
              break
            else
              sleep 1
            fi
          done
          if test ${exit_code} -ne 0; then
            cat ${work_directory}/err_log.tmp >&2
            echo "ERROR: ${exit_code}: can not put ${now}.txt on ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${txt_or_bin}/." >&2
          fi
        fi
        if test ${exit_code} -eq 0; then
          mv -f ${source_work_directory}/${pubsub_index_directory}_new_index.tmp ${source_work_directory}/${pubsub_index_directory}_index.txt
        fi
      fi
    fi
    if test ${exit_code} -eq 0; then
      find ${processed_directory} -regextype posix-egrep -regex "^${processed_directory}/[0-9]{14}_${unique_center_id}\.txt$" -type f -mmin +${delete_index_minute} | xargs -r rm -f
#      mkdir -p ${processed_directory}_old
#      find ${processed_directory} -regextype posix-egrep -regex "^${processed_directory}/[0-9]{14}_${unique_center_id}\.txt$" -type f -mmin +${delete_index_minute} | xargs -r mv -t ${processed_directory}_old
    fi
  done
  if test ${exit_code} -ne 0; then
    return_code=${exit_code}
  fi
  return ${return_code}
}
bandwidth_limit_k_bytes_per_s=0
delete_index_minute=360
job_directory=4PubClone
parallel=4
pubsub_index_directory=4PubSub
rclone_timeout=480
retry_num=16
search_index_directory=4Search
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--delete_index_minute" ) delete_index_minute=$2;shift;shift;;
    '--help' ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--delete_index_minute delete_index_minute] [--parallel the_number_of_parallel_transfer] [--timeout rclone_timeout] local_work_directory_open_or_closed unique_center_id txt_or_bin 'source_rclone_remote_bucket_main[;source_rclone_remote_bucket_sub]' 'destination_rclone_remote_bucket_main[;destination_rclone_remote_bucket_sub]' [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
    "--parallel" ) parallel=$2;shift;shift;;
    "--timeout" ) rclone_timeout=$2;shift;shift;;
  esac
done
if test -z "$5"; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory_open_or_closed=$1
unique_center_id=$2
set +e
txt_or_bin=`echo $3 | grep -E '^(txt|bin)$'`
source_rclone_remote_bucket_main_sub=`echo $4 | grep -F ':'`
destination_rclone_remote_bucket_main_sub=`echo $5 | grep -F ':'`
set -e
if test -z "${txt_or_bin}"; then
  echo "ERROR: $3 is not txt or bin." >&2
  exit 199
fi
if test -z "${source_rclone_remote_bucket_main_sub}"; then
  echo "ERROR: $4 is not rclone_remote:bucket." >&2
  exit 199
fi
if test -z "${destination_rclone_remote_bucket_main_sub}"; then
  echo "ERROR: $5 is not rclone_remote:bucket." >&2
  exit 199
fi
inclusive_pattern_file=''
if test -n $6; then
  inclusive_pattern_file=$6
fi
exclusive_pattern_file=''
if test -n $7; then
  exclusive_pattern_file=$7
fi
work_directory=${local_work_directory_open_or_closed}/${job_directory}/${unique_center_id}/${txt_or_bin}
processed_directory=${local_work_directory_open_or_closed}/${job_directory}/processed/${txt_or_bin}
mkdir -p ${work_directory} ${processed_directory}
touch ${processed_directory}/dummy.tmp
if test -s ${work_directory}/pid.txt; then
  running=`cat ${work_directory}/pid.txt | xargs -r ps ho 'pid comm args' | grep -F " $0 " | grep -F " ${unique_center_id} " | grep -F " ${txt_or_bin} " | wc -l`
else
  running=0
fi
if test ${running} -eq 0; then
  clone &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  wait ${pid}
fi
