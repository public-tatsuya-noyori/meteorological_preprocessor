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
bandwidth_limit_k_bytes_per_s=0
cutoff=16M
end_yyyymmddhhmm=0
out=0
pubsub_index_directory=4PubSub
search_index_directory=4Search
start_yyyymmddhhmm=0
suffix=.`id -un`.`date -u +"%Y%m%d%H%M%S%N"`
timeout=8s
parallel=64
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--debug_shell" ) set -evx;shift;;
    "--end" ) end_yyyymmddhhmm=$2;shift;shift;;
    "--help" ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--debug_shell] [--parallel the_number_of_parallel_transfer] [--start yyyymmddhhmm] [--end yyyymmddhhmm] [--out] local_work_directory rclone_remote_bucket priority keyword_pattern/inclusive_pattern_file [exclusive_pattern_file]"; exit 0;;
    "--out" ) out=1;shift;;
    "--parallel" ) parallel=$2;shift;shift;;
    "--start" ) start_yyyymmddhhmm=$2;shift;shift;;
  esac
done
if test -z $4; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
work_directory=${local_work_directory}
set +e
rclone_remote_bucket=`echo $2 | grep -F ':'`
priority=`echo $3 | grep "^p[1-9]$"`
set -e
if test -z "${rclone_remote_bucket}"; then
  echo "ERROR: $2 is not rclone_remote:bucket." >&2
  exit 199
fi
if test -z ${priority}; then
  echo "ERROR: $3 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
keyword_pattern=''
inclusive_pattern_file=''
if test -f $4; then
  inclusive_pattern_file=$4
else
  keyword_pattern=$4
fi
exclusive_pattern_file=''
if test -n $5; then
  exclusive_pattern_file=$5
fi
if test -n "${start_yyyymmddhhmm}"; then
  set +e
  start_yyyymmddhhmm=`echo "${start_yyyymmddhhmm}" | grep -E "^([0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]|0)$"`
  set -e
  if test -z ${start_yyyymmddhhmm}; then
    echo "ERROR: start_yyyymmddhhmm is not yyyymmddhhmm." >&2
    exit 199
  fi
  set +e
  start_yyyymmddhhmm=`expr 0 + ${start_yyyymmddhhmm}`
  set -e
fi
if test -n "${end_yyyymmddhhmm}"; then
  set +e
  end_yyyymmddhhmm=`echo "${end_yyyymmddhhmm}" | grep -E "^([0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]|0)$"`
  set -e
  if test -z ${end_yyyymmddhhmm}; then
    echo "ERROR: end_yyyymmddhhmm is not yyyymmddhhmm." >&2
    exit 199
  fi
  set +e
  end_yyyymmddhhmm=`expr 0 + ${end_yyyymmddhhmm}`
  set -e
fi
if test -n "${work_directory}"; then
  mkdir -p "${work_directory}"
fi
if test -f ${work_directory}/search_index_dir_list.tmp${suffix} -o -f ${work_directory}/search_index_list.tmp${suffix} -o -f ${work_directory}/search_index.tmp${suffix} -o -f ${work_directory}/search_file.tmp${suffix}; then
  echo "ERROR: exist${work_directory}/search_index_dir_list.tmp${suffix} or ${work_directory}/search_index_list.tmp${suffix} or ${work_directory}/search_index.tmp${suffix} or ${work_directory}/search_file.tmp${suffix}." >&2
  exit 199
fi
cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
set +e
rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority} > ${work_directory}/search_index_dir_list.tmp${suffix}
exit_code=$?
set -e
if test ${exit_code} -eq 0; then
  cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
else
  cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
  echo "ERROR: can not get index directory list from ${rclone_remote_bucket}/${search_index_directory}/${priority}." >&2
  rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
  exit ${exit_code}
fi
if test ${end_yyyymmddhhmm} -eq 0; then
  if test ${start_yyyymmddhhmm} -eq 0; then
    for index_directory in `cat ${work_directory}/search_index_dir_list.tmp${suffix}`; do
      yyyymmddhh=`echo ${index_directory} | cut -c1-10`
      set +e
      yyyymmddhh=`expr 0 + ${yyyymmddhh}`
      rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh} > ${work_directory}/search_index_list.tmp${suffix}
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
      else
        cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
        echo "ERROR: can not get index file list from ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}." >&2
        rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
        exit ${exit_code}
      fi
      for index_file in `cat ${work_directory}/search_index_list.tmp${suffix}`; do
        set +e
        rclone copyto --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file} ${work_directory}/search_index.tmp${suffix}
        exit_code=$?
        set -e
        if test ${exit_code} -eq 0; then
          cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
        else
          cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
          echo "ERROR: can not get index file from ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file}." >&2
          rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
          exit ${exit_code}
        fi
        set +e
        if test -n "${exclusive_pattern_file}"; then
          grep -v -E -f ${exclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/search_file.tmp${suffix}
        elif test -n "${inclusive_pattern_file}"; then
          grep -E -f ${inclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
        else
          grep -E ${keyword_pattern} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
        fi
        set -e
        if test -s ${work_directory}/search_file.tmp${suffix}; then
          if test ${out} -eq 0; then
            cat ${work_directory}/search_file.tmp${suffix}
          else
            set +e
            rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --checksum --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/search_file.tmp${suffix} --immutable --local-no-set-modtime --log-level DEBUG --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-traverse --retries 3 --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${work_directory}
            exit_code=$?
            set -e
            if test ${exit_code} -ne 0; then
              echo "ERROR: can not get file from ${rclone_remote_bucket} ${priority}." >&2
              rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
              exit ${exit_code}
            fi
          fi
        fi
      done
    done
    set +e
    rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${work_directory}/search_index_list.tmp${suffix}
    exit_code=$?
    set -e
    if test ${exit_code} -eq 0; then
      cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
    else
      cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
      echo "ERROR: can not get index list from ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >&2
      rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
      exit ${exit_code}
    fi
    for index_file in `cat ${work_directory}/search_index_list.tmp${suffix}`; do
      set +e
      rclone copyto --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file} ${work_directory}/search_index.tmp${suffix}
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
      else
        cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
        echo "ERROR: can not get index file from ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file}." >&2
        rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
        exit ${exit_code}
      fi
      set +e
      if test -n "${exclusive_pattern_file}"; then
        grep -v -E -f ${exclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/search_file.tmp${suffix}
      elif test -n "${inclusive_pattern_file}"; then
        grep -E -f ${inclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
      else
        grep -E ${keyword_pattern} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
      fi
      set -e
      if test -s ${work_directory}/search_file.tmp${suffix}; then
        if test ${out} -eq 0; then
          cat ${work_directory}/search_file.tmp${suffix}
        else
          set +e
          rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --checksum --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/search_file.tmp${suffix} --immutable --local-no-set-modtime --log-level DEBUG --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-traverse --retries 3 --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${work_directory}
          exit_code=$?
          set -e
          if test ${exit_code} -ne 0; then
            echo "ERROR: can not get file from ${rclone_remote_bucket} ${priority}." >&2
            rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
            exit ${exit_code}
          fi
        fi
      fi
    done
  else
    for index_directory in `cat ${work_directory}/search_index_dir_list.tmp${suffix}`; do
      yyyymmddhh=`echo ${index_directory} | cut -c1-10`
      set +e
      yyyymmddhh00=`expr 100 \* ${yyyymmddhh}`
      set -e
      if test ${yyyymmddhh00} -ge ${start_yyyymmddhhmm}; then
        set +e
        rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh} > ${work_directory}/search_index_list.tmp${suffix}
        exit_code=$?
        set -e
        if test ${exit_code} -eq 0; then
          cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
        else
          cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
          echo "ERROR: can not get index file list from ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}." >&2
          rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
          exit ${exit_code}
        fi
        for index_file in `cat ${work_directory}/search_index_list.tmp${suffix}`; do
          mm=`echo ${index_file} | cut -c1-2`
          set +e
          mm=`expr 0 + ${mm}`
          yyyymmddhhmm=`expr ${yyyymmddhh00} + ${mm}`
          set -e
          if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm}; then
            set +e
            rclone copyto --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file} ${work_directory}/search_index.tmp${suffix}
            exit_code=$?
            set -e
            if test ${exit_code} -eq 0; then
              cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
            else
              cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
              echo "ERROR: can not get index file from ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file}." >&2
              rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
              exit ${exit_code}
            fi
            set +e
            if test -n "${exclusive_pattern_file}"; then
              grep -v -E -f ${exclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/search_file.tmp${suffix}
            elif test -n "${inclusive_pattern_file}"; then
              grep -E -f ${inclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
            else
              grep -E ${keyword_pattern} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
            fi
            set -e
            if test -s ${work_directory}/search_file.tmp${suffix}; then
              if test ${out} -eq 0; then
                cat ${work_directory}/search_file.tmp${suffix}
              else
                set +e
                rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --checksum --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/search_file.tmp${suffix} --immutable --local-no-set-modtime --log-level DEBUG --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-traverse --retries 3 --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${work_directory}
                exit_code=$?
                set -e
                if test ${exit_code} -ne 0; then
                  echo "ERROR: can not get file from ${rclone_remote_bucket} ${priority}." >&2
                  rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
                  exit ${exit_code}
                fi
              fi
            fi
          fi
        done
      fi
    done
    set +e
    rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${work_directory}/search_index_list.tmp${suffix}
    exit_code=$?
    set -e
    if test ${exit_code} -eq 0; then
      cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
    else
      cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
      echo "ERROR: can not get index list from ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >&2
      rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
      exit ${exit_code}
    fi
    for index_file in `cat ${work_directory}/search_index_list.tmp${suffix}`; do
      yyyymmddhhmm=`echo ${index_file} | cut -c1-12`
      set +e
      yyyymmddhhmm=`expr 0 + ${yyyymmddhhmm}`
      set -e
      if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm}; then
        set +e
        rclone copyto --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file} ${work_directory}/search_index.tmp${suffix}
        exit_code=$?
        set -e
        if test ${exit_code} -eq 0; then
          cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
        else
          cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
          echo "ERROR: can not get index file from ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file}." >&2
          rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
          exit ${exit_code}
        fi
        set +e
        if test -n "${exclusive_pattern_file}"; then
          grep -v -E -f ${exclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/search_file.tmp${suffix}
        elif test -n "${inclusive_pattern_file}"; then
          grep -E -f ${inclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
        else
          grep -E ${keyword_pattern} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
        fi
        set -e
        if test -s ${work_directory}/search_file.tmp${suffix}; then
          if test ${out} -eq 0; then
            cat ${work_directory}/search_file.tmp${suffix}
          else
            set +e
            rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --checksum --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/search_file.tmp${suffix} --immutable --local-no-set-modtime --log-level DEBUG --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-traverse --retries 3 --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${work_directory}
            exit_code=$?
            set -e
            if test ${exit_code} -ne 0; then
              echo "ERROR: can not get file from ${rclone_remote_bucket} ${priority}." >&2
              rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
              exit ${exit_code}
            fi
          fi
        fi
      fi
    done
  fi
else
  for index_directory in `cat ${work_directory}/search_index_dir_list.tmp${suffix}`; do
    yyyymmddhh=`echo ${index_directory} | cut -c1-10`
    set +e
    yyyymmddhh00=`expr 100 \* ${yyyymmddhh}`
    set -e
    if test ${yyyymmddhh00} -ge ${start_yyyymmddhhmm} -a ${yyyymmddhh00} -le ${end_yyyymmddhhmm}; then
      set +e
      rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh} > ${work_directory}/search_index_list.tmp${suffix}
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
      else
        cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
        echo "ERROR: can not get index file list from ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}." >&2
        rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
        exit ${exit_code}
      fi
      for index_file in `cat ${work_directory}/search_index_list.tmp${suffix}`; do
        mm=`echo ${index_file} | cut -c1-2`
        set +e
        mm=`expr 0 + ${mm}`
        yyyymmddhhmm=`expr ${yyyymmddhh00} + ${mm}`
        set -e
        if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm} -a ${yyyymmddhhmm} -le ${end_yyyymmddhhmm}; then
          set +e
          rclone copyto --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file} ${work_directory}/search_index.tmp${suffix}
          exit_code=$?
          set -e
          if test ${exit_code} -eq 0; then
            cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
          else
            cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
            echo "ERROR: can not get index file from ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file}." >&2
            rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
            exit ${exit_code}
          fi
          set +e
          if test -n "${exclusive_pattern_file}"; then
            grep -v -E -f ${exclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/search_file.tmp${suffix}
          elif test -n "${inclusive_pattern_file}"; then
            grep -E -f ${inclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
          else
            grep -E ${keyword_pattern} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
          fi
          set -e
          if test -s ${work_directory}/search_file.tmp${suffix}; then
            if test ${out} -eq 0; then
              cat ${work_directory}/search_file.tmp${suffix}
            else
              set +e
              rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --checksum --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/search_file.tmp${suffix} --immutable --local-no-set-modtime --log-level DEBUG --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-traverse --retries 3 --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${work_directory}
              exit_code=$?
              set -e
              if test ${exit_code} -ne 0; then
                echo "ERROR: can not get file from ${rclone_remote_bucket} ${priority}." >&2
                rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
                exit ${exit_code}
              fi
            fi
          fi
        fi
      done
    fi
  done
  set +e
  rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --contimeout ${timeout} --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority} > ${work_directory}/search_index_list.tmp${suffix}
  exit_code=$?
  set -e
  if test ${exit_code} -eq 0; then
    cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
  else
    cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
    echo "ERROR: can not get index list from ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}." >&2
    rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
    exit ${exit_code}
  fi
  for index_file in `cat ${work_directory}/search_index_list.tmp${suffix}`; do
    yyyymmddhhmm=`echo ${index_file} | cut -c1-12`
    set +e
    yyyymmddhhmm=`expr 0 + ${yyyymmddhhmm}`
    set -e
    if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm} -a ${yyyymmddhhmm} -le ${end_yyyymmddhhmm}; then
      set +e
      rclone copyto --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/${priority}_err_log.tmp${suffix} --low-level-retries 3 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file} ${work_directory}/search_index.tmp${suffix}
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        cp /dev/null ${work_directory}/${priority}_err_log.tmp${suffix}
      else
        cat ${work_directory}/${priority}_err_log.tmp${suffix} >&2
        echo "ERROR: can not get index file from ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file}." >&2
        rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
        exit ${exit_code}
      fi
      set +e
      if test -n "${exclusive_pattern_file}"; then
        grep -v -E -f ${exclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/search_file.tmp${suffix}
      elif test -n "${inclusive_pattern_file}"; then
        grep -E -f ${inclusive_pattern_file} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
      else
        grep -E ${keyword_pattern} ${work_directory}/search_index.tmp${suffix} > ${work_directory}/search_file.tmp${suffix}
      fi
      set -e
      if test -s ${work_directory}/search_file.tmp${suffix}; then
        if test ${out} -eq 0; then
          cat ${work_directory}/search_file.tmp${suffix}
        else
          set +e
          rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --checksum --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${work_directory}/search_file.tmp${suffix} --immutable --local-no-set-modtime --log-level DEBUG --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-traverse --retries 3 --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${work_directory}
          exit_code=$?
          set -e
          if test ${exit_code} -ne 0; then
            echo "ERROR: can not get file from ${rclone_remote_bucket} ${priority}." >&2
            rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
            exit ${exit_code}
          fi
        fi
      fi
    fi
  done
fi
rm -f ${work_directory}/search_index_dir_list.tmp${suffix} ${work_directory}/search_index_list.tmp${suffix} ${work_directory}/search_index.tmp${suffix} ${work_directory}/search_file.tmp${suffix} ${work_directory}/${priority}_err_log.tmp${suffix}
