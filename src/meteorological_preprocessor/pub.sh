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
update=0
for arg in "$@"; do
  case "${arg}" in
    '--help' ) echo "$0 raw_list_file local_dir rclone_remote bucket priority_name parallel access"; exit 0;;
  esac
done
if test $# -lt 7; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
raw_list_file=$1
if test ! -f "${raw_list_file}"; then
  echo "ERROR: ${raw_list_file} is not a file."
  exit 199
fi
local_dir=$2
rclone_remote=$3
bucket=$4
priority_name=$5
parallel=$6
access=$7
pub_datetime=`date -u "+%Y%m%d%H%M%S"`
mkdir -p ${local_dir}/${access}/4PubSub/${priority_name}
mkdir -p ${local_dir}/${access}/4Pub_log/${priority_name}
grep ^${local_dir}/${access}/ ${raw_list_file} | sed -e "s%^${local_dir}/${access}/%/%g" | grep -v '^ *$' | uniq > ${local_dir}/${access}/4PubSub/${priority_name}/${pub_datetime}.txt
if test -s ${local_dir}/${access}/4PubSub/${priority_name}/${pub_datetime}.txt; then
  unpub_num=1
  while test ${unpub_num} -ne 0; do
    now=`date -u "+%Y%m%d%H%M%S"`
    rclone --ignore-checksum --update --use-server-modtime --no-gzip-encoding --no-traverse --size-only --stats 0 --timeout 1m --transfers ${parallel} --s3-upload-concurrency ${parallel} --s3-upload-cutoff 0 --log-level ERROR --log-file ${local_dir}/${access}/4Pub_log/${priority_name}/${pub_datetime}_${now}.log copy --files-from-raw ${local_dir}/${access}/4PubSub/${priority_name}/${pub_datetime}.txt ${local_dir}/${access} ${rclone_remote}:${bucket}
    unpub_num=`grep ERROR ${local_dir}/${access}/4Pub_log/${priority_name}/${pub_datetime}_${now}.log | wc -l`
  done
  unpub_num=1
  retry_num=0
  while test ${unpub_num} -ne 0; do
    now=`date -u "+%Y%m%d%H%M%S"`
    set +e
    retry_num=`expr 1 + ${retry_num}`
    rclone --ignore-checksum --immutable --no-gzip-encoding --no-traverse --size-only --stats 0 --timeout 1m --transfers ${parallel} --s3-upload-concurrency ${parallel} --s3-upload-cutoff 0 --log-level ERROR --log-file ${local_dir}/${access}/4Pub_log/${priority_name}/${pub_datetime}_${now}_index.log copy ${local_dir}/${access}/4PubSub/${priority_name}/${pub_datetime}.txt ${rclone_remote}:${bucket}/4PubSub/${priority_name}
    unpub_num=`grep ERROR ${local_dir}/${access}/4Pub_log/${priority_name}/${pub_datetime}_${now}_index.log | wc -l`
    set -e
    if test ${unpub_num} -gt 0 -a ${retry_num} -gt 4; then
      exit 1
    fi
  done
fi
