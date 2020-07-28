#!/bin/bash
#
# Copyright 2020 Japan Meteorological Agency.
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
    '--help' ) echo "$0 sub_name rclone_remote bucket priority_name_pattern local_dir parallel access [include_pattern] [exclude_pattern]"; exit 0;;
  esac
done
if test $# -lt `expr 7`; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
sub_name=$1
rclone_remote=$2
bucket=$3
priority_name_pattern=$4
local_dir=$5
parallel=$6
access=$7
include_pattern=''
if test $# -ge 8; then
  include_pattern=$8
fi
exclude_pattern=''
if test $# -ge 9; then
  exclude_pattern=$8
fi
sub_datetime=`date -u "+%Y%m%d%H%M%S"`
mkdir -p ${local_dir}/${access}/4Sub_log/${sub_name}
for priority_name in `rclone --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/4Sub_log/${sub_name}/${sub_datetime}.log lsf --max-depth 1 ${rclone_remote}:${bucket}/4PubSub | grep -E "${priority_name_pattern}" | grep -v '^ *$' | sort -u`; do
  if test ! -d ${local_dir}/${access}/4PubSub/${priority_name}; then
    mkdir -p ${local_dir}/${access}/4PubSub/${priority_name}
    latest_created=`rclone --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/4Sub_log/${sub_name}/${sub_datetime}.log lsf --max-depth 1 ${rclone_remote}:${bucket}/4PubSub/${priority_name} | tail -1`
    rclone --ignore-checksum --immutable --no-gzip-encoding --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level ERROR --log-file ${local_dir}/${access}/4Sub_log/${sub_name}/${sub_datetime}.log copy ${rclone_remote}:${bucket}/4PubSub/${priority_name}/${latest_created} ${local_dir}/${access}/4PubSub/${priority_name}
  fi
  local_latest_created=`ls -1 ${local_dir}/${access}/4PubSub/${priority_name} | grep -v ^.*\.tmp$ | tail -1`
  for newly_created in `rclone --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/4Sub_log/${sub_name}/${sub_datetime}.log lsf --max-depth 1 ${rclone_remote}:${bucket}/4PubSub/${priority_name} | grep ${local_latest_created} -A 100000 | sed -e '1d' | grep -v '^ *$' | sort -u`; do
    unsub_num=1
    while test ${unsub_num} -ne 0; do
      now=`date -u "+%Y%m%d%H%M%S"`
      rclone --ignore-checksum --immutable --no-gzip-encoding --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level DEBUG --log-file ${local_dir}/${access}/4Sub_log/${sub_name}/${sub_datetime}_${now}_index.log copyto ${rclone_remote}:${bucket}/4PubSub/${priority_name}/${newly_created} ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp
      unsub_num=`grep ERROR ${local_dir}/${access}/4Sub_log/${sub_name}/${sub_datetime}_${now}_index.log | wc -l`
    done
    if test -n "${include_pattern}"; then
      if test -n "${exclude_pattern}"; then
        match_list=`cat ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp | grep -v -E "${exclude_pattern}" | grep -E "${include_pattern}" | uniq`
	echo "${match_list}" > ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp
      else
        match_list=`cat ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp | grep -E "${include_pattern}" | uniq`
	echo "${match_list}" > ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp
      fi
    fi
    unsub_num=1
    while test ${unsub_num} -ne 0; do
      now=`date -u "+%Y%m%d%H%M%S"`
      rclone --ignore-checksum --immutable --no-gzip-encoding --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level DEBUG --log-file ${local_dir}/${access}/4Sub_log/${sub_name}/${sub_datetime}_${now}.log copy --files-from-raw ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp ${rclone_remote}:${bucket} ${local_dir}/${access}
      unsub_num=`grep ERROR ${local_dir}/${access}/4Sub_log/${sub_name}/${sub_datetime}_${now}.log | wc -l`
    done
    mv -f ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}
  done
done
