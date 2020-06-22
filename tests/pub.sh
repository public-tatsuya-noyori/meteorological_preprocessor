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
    '--help' ) echo "pub.sh raw_list_file_dir local_dir rclone_remote bucket pub_num [--closed]"; exit 0;;
  esac
done
if test $# -lt `expr 5 + ${opt_num}`; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
raw_list_file_dir=$1
if test ! -d "${raw_list_file_dir}"; then
  echo "ERROR: ${raw_list_file_dir} is not a directory."
  exit 199
fi
local_dir=$2
rclone_remote=$3
bucket=$4
pub_num=$5
if test ${open} -eq 1; then
    acl='open'
else
    acl='closed'
fi
for raw_list_file in `ls -1 ${raw_list_file_dir} | grep -v tmp`; do
  now=`date "+%Y%m%d%H%M%S"`
  mkdir -p ${local_dir}/${acl}/4PubSub/${pub_num}
  mkdir -p ${local_dir}/${acl}/4PubSub_log/${pub_num}
  grep ^${local_dir}/${acl}/ ${raw_list_file_dir}/${raw_list_file} | sed -e "s%^${local_dir}/${acl}/%/%g" | grep -v '^ *$' | sort -u > ${local_dir}/${acl}/4PubSub/${pub_num}/${now}.txt
  if test -s ${local_dir}/${acl}/4PubSub/${pub_num}/${now}.txt; then
    unpub_num=1
    while test ${unpub_num} -ne 0; do
      rm -f ${local_dir}/${acl}/4PubSub_log/${pub_num}/${now}.log
      rclone --ignore-checksum --ignore-existing --no-update-modtime --no-traverse --stats 0 --timeout 1m --transfers 8 --s3-upload-cutoff 0 --log-level INFO --log-file ${local_dir}/${acl}/4PubSub_log/${pub_num}/${now}.log copy --files-from-raw ${local_dir}/${acl}/4PubSub/${pub_num}/${now}.txt ${local_dir}/${acl} ${rclone_remote}:${bucket}
      unpub_num=`grep ERROR ${local_dir}/${acl}/4PubSub_log/${pub_num}/${now}.log | wc -l`
    done
    unpub_num=1
    while test ${unpub_num} -ne 0; do
      rm -f ${local_dir}/${acl}/4PubSub_log/${pub_num}/${now}_index.log
      rclone --ignore-checksum --ignore-existing --no-update-modtime --no-traverse --stats 0 --timeout 1m --transfers 8 --s3-upload-cutoff 0 --log-level INFO --log-file ${local_dir}/${acl}/4PubSub_log/${pub_num}/${now}_index.log copy ${local_dir}/${acl}/4PubSub/${pub_num}/${now}.txt ${rclone_remote}:${bucket}/4PubSub/${pub_num}
      unpub_num=`grep ERROR ${local_dir}/${acl}/4PubSub_log/${pub_num}/${now}_index.log | wc -l`
    done
    rm -f ${raw_list_file_dir}/${raw_list_file}
  fi
done
