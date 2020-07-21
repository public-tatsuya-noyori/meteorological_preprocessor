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
set -evx
for arg in "$@"; do
  case "${arg}" in
    '--help' ) echo "$0 clone_name src_rclone_remote src_bucket priority_name_pattern dst_rclone_remote dst_bucket local_dir parallel access [include_pattern] [exclude_pattern]"; exit 0;;
  esac
done
if test $# -lt `expr 9 + ${opt_num}`; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
clone_name=$1
src_rclone_remote=$2
src_bucket=$3
priority_name_pattern=$4
dst_rclone_remote=$5
dst_bucket=$6
local_dir=$7
parallel=$8
access=$9
include_pattern=''
if test $# -ge 10; then
  include_pattern=$10
fi
exclude_pattern=''
if test $# -ge 11; then
  exclude_pattern=$11
fi
clone_datetime=`date -u "+%Y%m%d%H%M%S"`
mkdir -p ${local_dir}/${access}/4Clone_log/${clone_name}
for priority_name in `rclone --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}.log lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub | grep -E "${priority_name_pattern}" | grep -v '^ *$' | sort -u`; do
  if test ! -d ${local_dir}/${access}/4PubSub/${priority_name}; then
    mkdir -p ${local_dir}/${access}/4PubSub/${priority_name}
    latest_created=`rclone --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}.log lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name} | tail -1`
    rclone --ignore-checksum --ignore-existing --no-gzip-encoding --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}.log copy ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name}/${latest_created} ${local_dir}/${access}/4PubSub/${priority_name}
  fi
  local_latest_created=`ls -1 ${local_dir}/${access}/4PubSub/${priority_name} | grep -v ^.*\.tmp$ | tail -1`
  for newly_created in `rclone --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}.log lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name} | grep ${local_latest_created} -A 100000 | sed -e '1d' | grep -v '^ *$' | sort -u`; do
    unclone_num=1
    while test ${unclone_num} -ne 0; do
      now=`date -u "+%Y%m%d%H%M%S"`
      rclone --ignore-checksum --ignore-existing --no-gzip-encoding --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_sub.log copyto ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name}/${newly_created} ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp
      unclone_num=`grep ERROR ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_sub.log | wc -l`
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
    unclone_num=1
    while test ${unclone_num} -ne 0; do
      now=`date -u "+%Y%m%d%H%M%S"`
      rclone --ignore-checksum --ignore-existing --no-gzip-encoding --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --s3-upload-concurrency ${parallel} --s3-upload-cutoff 0 --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}.log copy --files-from-raw ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp ${src_rclone_remote}:${src_bucket} ${dst_rclone_remote}:${dst_bucket}
      unclone_num=`grep ERROR ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}.log | wc -l`
    done
    mv -f ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}
    unclone_num=1
    retry_num=0
    while test ${unclone_num} -ne 0; do
      now=`date -u "+%Y%m%d%H%M%S"`
      set +e
      retry_num=`expr 1 + ${retry_num}`
      rclone --ignore-checksum --immutable --no-gzip-encoding --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --s3-upload-concurrency ${parallel} --s3-upload-cutoff 0 --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_pub.log copyto ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created} ${dst_rclone_remote}:${dst_bucket}/4PubSub/${priority_name}/${now}.txt
      unclone_num=`grep ERROR ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_pub.log | wc -l`
      set -e
      if test ${unclone_num} -gt 0 -a ${retry_num} -gt 4; then
        exit 1
      fi
    done
  done
done
