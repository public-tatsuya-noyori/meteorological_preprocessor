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
for arg in "$@"; do
  case "${arg}" in
    "--debug" ) set -evx;shift;;
    "--help" ) echo "$0 rclone_remote_bucket priority keyword [end_published_yyyymmddhh] [start_published_yyyymmddhh]"; exit 0;;
  esac
done
if test -z $3; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
rclone_remote_bucket=$1
priority=`echo $2 | grep "^p[1-9]$"`
if test -z ${priority}; then
  echo "ERROR: $2 is not p1 or p2 or p3 or p4 or p5 or p6 or p7 or p8 or p9." >&2
  exit 199
fi
keyword=$3
end_yyyymmddhh=0
if test -n "$4"; then
  end_yyyymmddhh=`echo $4 | grep "^[0-9]\+$"`
  if test -z ${end_yyyymmddhh}; then
    echo "ERROR: $4 is not integer." >&2
    exit 199
  elif test $4 -le 0; then
    echo "ERROR: $4 is not more than 1." >&2
    exit 199
  fi
  end_yyyymmddhh=$4
fi
start_yyyymmddhh=0
if test -n "$5"; then
  start_yyyymmddhh=`echo $5 | grep "^[0-9]\+$"`
  if test -z ${start_yyyymmddhh}; then
    echo "ERROR: $5 is not integer." >&2
    exit 199
  elif test $5 -le 0; then
    echo "ERROR: $5 is not more than 1." >&2
    exit 199
  fi
  start_yyyymmddhh=$5
fi
if test ${start_yyyymmddhh} -eq 0; then
  if test ${end_yyyymmddhh} -eq 0; then
    for index_directory in `rclone lsf ${rclone_remote_bucket}/4Search/${priority} | tac`; do
      yyyymmddhh=`echo ${index_directory} | cut -c1-10`
      yyyymmddhh=`expr 0 + ${yyyymmddhh}`
      rclone lsf ${rclone_remote_bucket}/4Search/${priority}/${yyyymmddhh} | tac | xargs -r -n 1 -I {} rclone cat ${rclone_remote_bucket}/4Search/${priority}/${yyyymmddhh}/{} | grep ${keyword} | tac
    done
  else
    for index_directory in `rclone lsf ${rclone_remote_bucket}/4Search/${priority} | tac`; do
      yyyymmddhh=`echo ${index_directory} | cut -c1-10`
      yyyymmddhh=`expr 0 + ${yyyymmddhh}`
      if test ${yyyymmddhh} -le ${end_yyyymmddhh}; then
        rclone lsf ${rclone_remote_bucket}/4Search/${priority}/${yyyymmddhh} | tac | xargs -r -n 1 -I {} rclone cat ${rclone_remote_bucket}/4Search/${priority}/${yyyymmddhh}/{} | grep ${keyword} | tac
      fi
    done
  fi
else
  for index_directory in `rclone lsf ${rclone_remote_bucket}/4Search/${priority} | tac`; do
    yyyymmddhh=`echo ${index_directory} | cut -c1-10`
    yyyymmddhh=`expr 0 + ${yyyymmddhh}`
    if test ${yyyymmddhh} -le ${end_yyyymmddhh} -a ${yyyymmddhh} -ge ${start_yyyymmddhh}; then
      rclone lsf ${rclone_remote_bucket}/4Search/${priority}/${yyyymmddhh} | tac | xargs -r -n 1 -I {} rclone cat ${rclone_remote_bucket}/4Search/${priority}/${yyyymmddhh}/{} | grep ${keyword} | tac
    fi
  done
fi
