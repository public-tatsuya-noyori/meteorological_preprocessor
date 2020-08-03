#!/bin/sh
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
    '--help' ) echo "$0 sub_name rclone_remote bucket priority_name_pattern local_dir parallel access [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
  esac
done
if test $# -lt 7; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
sub_name=$1
rclone_remote=$2
bucket=$3
priority_name_pattern=$4
local_dir=$5
set +e
parallel=`echo "$6" | grep '^[0-9]\+$'`
set -e
if test -z "${parallel}"; then
  echo "ERROR: $6 is not integer."
  exit 199
elif test $6 -le 0; then
  echo "ERROR: $6 is not more than 1."
  exit 199
fi
access=$7
inclusive_pattern_file=''
if test $# -ge 8; then
  inclusive_pattern_file=$8
fi
exclusive_pattern_file=''
if test $# -ge 9; then
  exclusive_pattern_file=$9
fi
sub_datetime=`date -u "+%Y%m%d%H%M%S"`
for priority_name in `rclone --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${rclone_remote}:${bucket}/4PubSub | grep -E "${priority_name_pattern}" | uniq`; do
  mkdir -p ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log
  if test ! -f ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/index.txt; then
    rclone --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${rclone_remote}:${bucket}/4PubSub/${priority_name} > ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/index.txt
    exit 0
  fi
  rclone --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${rclone_remote}:${bucket}/4PubSub/${priority_name} > ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/index.txt.new
  for newly_created in `diff ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/index.txt ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/index.txt.new | grep '>' | cut -c3-`; do
    now=`date -u "+%Y%m%d%H%M%S"`
    set +e
    rclone --update --use-server-modtime --no-gzip-encoding --no-traverse --size-only --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}_index.log copyto ${rclone_remote}:${bucket}/4PubSub/${priority_name}/${newly_created} ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp
    unsub_num=`grep ERROR ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}_index.log | wc -l`
    if test ${unsub_num} -ne 0; then
      cat ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}_index.log
      rm -f ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}_index.log
      exit 199
    fi
    set -e
    rm -f ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}_index.log
    if test -n "${inclusive_pattern_file}"; then
      if test -n "${exclusive_pattern_file}"; then
        cat ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} | uniq > ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp.new
        mv -f ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp.new ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp
      else
        cat ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp | grep -E -f ${inclusive_pattern_file} | uniq > ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp.new
        mv -f ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp.new ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp
      fi
    fi
    if test -s ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp; then
      now=`date -u "+%Y%m%d%H%M%S"`
      set +e
      rclone --update --use-server-modtime --no-traverse --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level INFO --log-file ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}.log copy --files-from-raw ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp ${rclone_remote}:${bucket} ${local_dir}/${access}
      unsub_num=`grep ERROR ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}.log | wc -l`
      if test ${unsub_num} -ne 0; then
        grep ERROR ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}.log
        rm -f ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}.log
        exit 199
      fi
      mv -f ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}
      sed -e "s|^.* INFO *: *\(.*\) *: Copied .*$|${local_dir}/${access}/\1|g" ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}.log
      set -e
      rm -f ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/log/${sub_datetime}_${now}.log
    fi
  done
  mv -f ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/index.txt.new ${local_dir}/${access}/4Sub/${priority_name}/${sub_name}/index.txt
done
