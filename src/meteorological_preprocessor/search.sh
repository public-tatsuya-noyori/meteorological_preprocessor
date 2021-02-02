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
    "--bnadwidth_limit") shift;bandwidth_limit_k_bytes_per_s=$1;shift;;
    "--debug_shell" ) set -evx;shift;;
    "--end" ) end_yyyymmddhhmm=$2;shift;shift;;
    "--help" ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--debug_shell] [--start yyyymmddhhmm] [--end yyyymmddhhmm] [--out] local_work_directory rclone_remote_bucket priority keyword_pattern/inclusive_pattern_file [exclusive_pattern_file]"; exit 0;;
    "--out" ) out=1;shift;;
    "--start" ) start_yyyymmddhhmm=$2;shift;shift;;
  esac
done
if test -z $4; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
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
if test -n "${local_work_directory}"; then
  mkdir -p "${local_work_directory}"
fi
if test -f ${local_work_directory}/search_index.tmp${suffix} -o -f ${local_work_directory}/search_file.tmp${suffix}; then
  echo "ERROR: exist ${local_work_directory}/search_index.tmp${suffix} or ${local_work_directory}/search_file.tmp${suffix}." >&2
  exit 199
fi
if test ${end_yyyymmddhhmm} -eq 0; then
  if test ${start_yyyymmddhhmm} -eq 0; then
    for index_directory in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}`; do
      yyyymmddhh=`echo ${index_directory} | cut -c1-10`
      set +e
      yyyymmddhh=`expr 0 + ${yyyymmddhh}`
      set -e
      for index_file in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}`; do
        rclone copyto --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file} ${local_work_directory}/search_index.tmp${suffix}
        set +e
        if test -n "${exclusive_pattern_file}"; then
          grep -v -E -f ${exclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${local_work_directory}/search_file.tmp${suffix}
        elif test -n "${inclusive_pattern_file}"; then
          grep -E -f ${inclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
        else
          grep -E ${keyword_pattern} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
        fi
        set -e
        if test -s ${local_work_directory}/search_file.tmp${suffix}; then
          if test ${out} -eq 0; then
            cat ${local_work_directory}/search_file.tmp${suffix}
          else
            rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${local_work_directory}/search_file.tmp${suffix} --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${local_work_directory}
          fi
        fi
      done
    done
    for index_file in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}`; do
      rclone copyto --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file} ${local_work_directory}/search_index.tmp${suffix}
      set +e
      if test -n "${exclusive_pattern_file}"; then
        grep -v -E -f ${exclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${local_work_directory}/search_file.tmp${suffix}
      elif test -n "${inclusive_pattern_file}"; then
        grep -E -f ${inclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
      else
        grep -E ${keyword_pattern} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
      fi
      set -e
      if test -s ${local_work_directory}/search_file.tmp${suffix}; then
        if test ${out} -eq 0; then
          cat ${local_work_directory}/search_file.tmp${suffix}
        else
          rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${local_work_directory}/search_file.tmp${suffix} --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${local_work_directory}
        fi
      fi
    done
  else
    for index_directory in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}`; do
      yyyymmddhh=`echo ${index_directory} | cut -c1-10`
      set +e
      yyyymmddhh00=`expr 100 \* ${yyyymmddhh}`
      set -e
      if test ${yyyymmddhh00} -ge ${start_yyyymmddhhmm}; then
        for index_file in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}`; do
          mm=`echo ${index_file} | cut -c1-2`
          set +e
          mm=`expr 0 + ${mm}`
          yyyymmddhhmm=`expr ${yyyymmddhh00} + ${mm}`
          set -e
          if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm}; then
            rclone copyto --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file} ${local_work_directory}/search_index.tmp${suffix}
            set +e
            if test -n "${exclusive_pattern_file}"; then
              grep -v -E -f ${exclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${local_work_directory}/search_file.tmp${suffix}
            elif test -n "${inclusive_pattern_file}"; then
              grep -E -f ${inclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
            else
              grep -E ${keyword_pattern} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
            fi
            set -e
            if test -s ${local_work_directory}/search_file.tmp${suffix}; then
              if test ${out} -eq 0; then
                cat ${local_work_directory}/search_file.tmp${suffix}
              else
                rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${local_work_directory}/search_file.tmp${suffix} --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${local_work_directory}
              fi
            fi
          fi
        done
      fi
    done
    for index_file in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}`; do
      yyyymmddhhmm=`echo ${index_file} | cut -c1-12`
      set +e
      yyyymmddhhmm=`expr 0 + ${yyyymmddhhmm}`
      set -e
      if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm}; then
        rclone copyto --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file} ${local_work_directory}/search_index.tmp${suffix}
        set +e
        if test -n "${exclusive_pattern_file}"; then
          grep -v -E -f ${exclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${local_work_directory}/search_file.tmp${suffix}
        elif test -n "${inclusive_pattern_file}"; then
          grep -E -f ${inclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
        else
          grep -E ${keyword_pattern} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
        fi
        set -e
        if test -s ${local_work_directory}/search_file.tmp${suffix}; then
          if test ${out} -eq 0; then
            cat ${local_work_directory}/search_file.tmp${suffix}
          else
            rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${local_work_directory}/search_file.tmp${suffix} --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${local_work_directory}
          fi
        fi
      fi
    done
  fi
else
  for index_directory in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}`; do
    yyyymmddhh=`echo ${index_directory} | cut -c1-10`
    set +e
    yyyymmddhh00=`expr 100 \* ${yyyymmddhh}`
    set -e
    if test ${yyyymmddhh00} -ge ${start_yyyymmddhhmm} -a ${yyyymmddhh00} -le ${end_yyyymmddhhmm}; then
      for index_file in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}`; do
        mm=`echo ${index_file} | cut -c1-2`
        set +e
        mm=`expr 0 + ${mm}`
        yyyymmddhhmm=`expr ${yyyymmddhh00} + ${mm}`
        set -e
        if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm} -a ${yyyymmddhhmm} -le ${end_yyyymmddhhmm}; then
          rclone copyto --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file} ${local_work_directory}/search_index.tmp${suffix}
          set +e
          if test -n "${exclusive_pattern_file}"; then
            grep -v -E -f ${exclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${local_work_directory}/search_file.tmp${suffix}
          elif test -n "${inclusive_pattern_file}"; then
            grep -E -f ${inclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
          else
            grep -E ${keyword_pattern} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
          fi
          set -e
          if test -s ${local_work_directory}/search_file.tmp${suffix}; then
            if test ${out} -eq 0; then
              cat ${local_work_directory}/search_file.tmp${suffix}
            else
              rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${local_work_directory}/search_file.tmp${suffix} --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${local_work_directory}
            fi
          fi
        fi
      done
    fi
  done
  for index_file in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}`; do
    yyyymmddhhmm=`echo ${index_file} | cut -c1-12`
    set +e
    yyyymmddhhmm=`expr 0 + ${yyyymmddhhmm}`
    set -e
    if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm} -a ${yyyymmddhhmm} -le ${end_yyyymmddhhmm}; then
      rclone copyto --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file} ${local_work_directory}/search_index.tmp${suffix}
      set +e
      if test -n "${exclusive_pattern_file}"; then
        grep -v -E -f ${exclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} | grep -E -f ${inclusive_pattern_file} > ${local_work_directory}/search_file.tmp${suffix}
      elif test -n "${inclusive_pattern_file}"; then
        grep -E -f ${inclusive_pattern_file} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
      else
        grep -E ${keyword_pattern} ${local_work_directory}/search_index.tmp${suffix} > ${local_work_directory}/search_file.tmp${suffix}
      fi
      set -e
      if test -s ${local_work_directory}/search_file.tmp${suffix}; then
        if test ${out} -eq 0; then
          cat ${local_work_directory}/search_file.tmp${suffix}
        else
          rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checkers ${parallel} --contimeout ${timeout} --cutoff-mode=cautious --files-from-raw ${local_work_directory}/search_file.tmp${suffix} --local-no-set-modtime --log-level INFO --low-level-retries 3 --multi-thread-cutoff ${cutoff} --multi-thread-streams ${parallel} --no-check-dest --no-traverse --retries 1 --size-only --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${local_work_directory}
        fi
      fi
    fi
  done
fi
rm -f ${local_work_directory}/search_index.tmp${suffix} ${local_work_directory}/search_file.tmp${suffix}
