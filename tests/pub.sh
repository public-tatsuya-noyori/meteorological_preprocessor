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
open=1
opt_num=0
for arg in "$@"; do
  case "${arg}" in
    '--closed' ) open=0; ope_num=1;;
    '--help' ) echo "pub.sh raw_list_file local_dir rclone_remote bucket pubsub_name parallel [--closed]"; exit 0;;
  esac
done
if test $# -lt `expr 6 + ${opt_num}`; then
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
pubsub_name=$5
parallel=$6
if test ${open} -eq 1; then
  acl='open'
else
  acl='closed'
fi
pub_datetime=`date -u "+%Y%m%d%H%M%S"`
mkdir -p ${local_dir}/${acl}/4PubSub/${pubsub_name}
mkdir -p ${local_dir}/${acl}/4Pub_log/${pubsub_name}
grep ^${local_dir}/${acl}/ ${raw_list_file} | sed -e "s%^${local_dir}/${acl}/%/%g" | grep -v '^ *$' | sort -u > ${local_dir}/${acl}/4PubSub/${pubsub_name}/${pub_datetime}.txt
if test -s ${local_dir}/${acl}/4PubSub/${pubsub_name}/${pub_datetime}.txt; then
  unpub_num=1
  while test ${unpub_num} -ne 0; do
    now=`date -u "+%Y%m%d%H%M%S"`
    rclone --ignore-checksum --ignore-existing --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --s3-upload-concurrency ${parallel} --s3-upload-cutoff 0 --log-level ERROR --log-file ${local_dir}/${acl}/4Pub_log/${pubsub_name}/${pub_datetime}_${now}.log copy --files-from-raw ${local_dir}/${acl}/4PubSub/${pubsub_name}/${pub_datetime}.txt ${local_dir}/${acl} ${rclone_remote}:${bucket}
    unpub_num=`grep ERROR ${local_dir}/${acl}/4Pub_log/${pubsub_name}/${pub_datetime}_${now}.log | wc -l`
  done
  unpub_num=1
  while test ${unpub_num} -ne 0; do
    now=`date -u "+%Y%m%d%H%M%S"`
    rclone --ignore-checksum --ignore-existing --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --s3-upload-concurrency ${parallel} --s3-upload-cutoff 0 --log-level ERROR --log-file ${local_dir}/${acl}/4Pub_log/${pubsub_name}/${pub_datetime}_${now}_index.log copy ${local_dir}/${acl}/4PubSub/${pubsub_name}/${pub_datetime}.txt ${rclone_remote}:${bucket}/4PubSub/${pubsub_name}
    unpub_num=`grep ERROR ${local_dir}/${acl}/4Pub_log/${pubsub_name}/${pub_datetime}_${now}_index.log | wc -l`
  done
fi
