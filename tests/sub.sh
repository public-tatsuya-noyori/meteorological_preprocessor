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
    '--help' ) echo "sub.sh sub_name rclone_remote bucket pubsub_name_pattern local_dir parallel [--closed]"; exit 0;;
  esac
done
if test $# -lt `expr 6 + ${opt_num}`; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
sub_name=$1
rclone_remote=$2
bucket=$3
pubsub_name_pattern=$4
local_dir=$5
parallel=$6
if test ${open} -eq 1; then
  acl='open'
else
  acl='closed'
fi
sub_datetime=`date -u "+%Y%m%d%H%M%S"`
mkdir -p ${local_dir}/${acl}/4Sub_log/${sub_name}
for pubsub_name in `rclone --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${acl}/4Sub_log/${sub_name}/${sub_datetime}.log lsf --max-depth 1 ${rclone_remote}:${bucket}/4PubSub | grep ${pubsub_name_pattern} | grep -v '^ *$' | sort -u`; do
  if test ! -d ${local_dir}/${acl}/4PubSub/${pubsub_name}; then
    mkdir -p ${local_dir}/${acl}/4PubSub/${pubsub_name}
    latest_created=`rclone --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${acl}/4Sub_log/${sub_name}/${sub_datetime}.log lsf --max-depth 1 ${rclone_remote}:${bucket}/4PubSub/${pubsub_name} | tail -1`
    rclone --ignore-checksum --ignore-existing --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level ERROR --log-file ${local_dir}/${acl}/4Sub_log/${sub_name}/${sub_datetime}.log copy ${rclone_remote}:${bucket}/4PubSub/${pubsub_name}/${latest_created} ${local_dir}/${acl}/4PubSub/${pubsub_name}
  fi
  local_latest_created=`ls -1 ${local_dir}/${acl}/4PubSub/${pubsub_name} | tail -1`
  for newly_created in `rclone --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${acl}/4Sub_log/${sub_name}/${sub_datetime}.log lsf --max-depth 1 ${rclone_remote}:${bucket}/4PubSub/${pubsub_name} | grep ${local_latest_created} -A 100000 | sed -e '1d' | grep -v '^ *$' | sort -u`; do
  unsub_num=1
  while test ${unsub_num} -ne 0; do
    rclone --ignore-checksum --ignore-existing --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level DEBUG --log-file ${local_dir}/${acl}/4Sub_log/${sub_name}/${sub_datetime}_index.log copyto ${rclone_remote}:${bucket}/4PubSub/${pubsub_name}/${newly_created} ${local_dir}/${acl}/4PubSub/${pubsub_name}/${newly_created}.tmp
    unsub_num=`grep ERROR ${local_dir}/${acl}/4Sub_log/${sub_name}/${sub_datetime}_index.log | wc -l`
  done
  unsub_num=1
  while test ${unsub_num} -ne 0; do
    rclone --ignore-checksum --ignore-existing --no-traverse --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level DEBUG --log-file ${local_dir}/${acl}/4Sub_log/${sub_name}/${sub_datetime}.log copy --files-from-raw ${local_dir}/${acl}/4PubSub/${pubsub_name}/${newly_created}.tmp ${rclone_remote}:${bucket} ${local_dir}/${acl}
    unsub_num=`grep ERROR ${local_dir}/${acl}/4Sub_log/${sub_name}/${sub_datetime}.log | wc -l`
  done
  mv -f ${local_dir}/${acl}/4PubSub/${pubsub_name}/${newly_created}.tmp ${local_dir}/${acl}/4PubSub/${pubsub_name}/${newly_created}
done
