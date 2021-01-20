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
pubsub_index_directory=4PubSub
search_index_directory=4Search
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--debug_shell" ) set -evx;shift;;
    "--help" ) echo "$0 [--debug_shell] rclone_remote_bucket priority keyword_pattern [start_yyyymmddhhmm] [end_yyyymmddhhmm]"; exit 0;;
  esac
done
if test -z $3; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
set +e
rclone_remote_bucket=`echo $1 | grep ':'`
priority=`echo $2 | grep "^p[1-9]$"`
set -e
if test -z "${rclone_remote_bucket}"; then
  echo "ERROR: $1 is not rclone_remote:bucket." >&2
  exit 199
fi
if test -z ${priority}; then
  echo "ERROR: $2 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
keyword_pattern=$3
start_yyyymmddhhmm=0
if test -n "$4"; then
  start_yyyymmddhhmm=`echo $4 | grep "^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$"`
  if test -z ${start_yyyymmddhhmm}; then
    echo "ERROR: $4 is not yyyymmddhh." >&2
    exit 199
  fi
  set +e
  start_yyyymmddhhmm=`expr 0 + $4`
  set -e
fi
end_yyyymmddhhmm=0
if test -n "$5"; then
  end_yyyymmddhhmm=`echo $5 | grep "^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$"`
  if test -z ${end_yyyymmddhhmm}; then
    echo "ERROR: $5 is not yyyymmddhh." >&2
    exit 199
  fi
  set +e
  end_yyyymmddhhmm=`expr 0 + $5`
  set -e
fi
if test ${end_yyyymmddhhmm} -eq 0; then
  if test ${start_yyyymmddhhmm} -eq 0; then
    for index_directory in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}`; do
      yyyymmddhh=`echo ${index_directory} | cut -c1-10`
      set +e
      yyyymmddhh=`expr 0 + ${yyyymmddhh}`
      set -e
      rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh} | xargs -r -n 1 -I {} rclone cat --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/{} | grep -E ${keyword_pattern}
    done
    for index_file in `rclone lsf --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}`; do
      rclone cat --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file} | grep -E ${keyword_pattern}
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
            rclone cat --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file} | grep -E ${keyword_pattern}
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
        rclone cat --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file} | grep -E ${keyword_pattern}
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
          rclone cat --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${priority}/${yyyymmddhh}/${index_file} | grep -E ${keyword_pattern}
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
      rclone cat --contimeout ${timeout} --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 1 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${priority}/${index_file} | grep -E ${keyword_pattern}
    fi
  done
fi
