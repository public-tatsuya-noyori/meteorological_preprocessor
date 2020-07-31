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
    '--help' ) echo "$0 clone_name src_rclone_remote src_bucket priority_name_pattern dst_rclone_remote dst_bucket local_dir parallel access [inclusive_pattern_file] [exclusive_pattern_file]"; exit 0;;
  esac
done
if test $# -lt 9; then
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
access=$9
inclusive_pattern_file=''
if test $# -ge 10; then
  inclusive_pattern_file=$10
fi
exclusive_pattern_file=''
if test $# -ge 11; then
  exclusive_pattern_file=$11
fi
clone_datetime=`date -u "+%Y%m%d%H%M%S"`
mkdir -p ${local_dir}/${access}/4Clone_log/${clone_name}
for priority_name in `rclone --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub | grep -E "${priority_name_pattern}" | grep -v '^ *$' | uniq`; do
  if test ! -d ${local_dir}/${access}/4PubSub/${priority_name}; then
    latest_created=`rclone --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name} | tail -1`
    rclone --update --use-server-modtime --no-gzip-encoding --no-traverse --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}.log copy ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name}/${latest_created} ${local_dir}/${access}/4PubSub/${priority_name}
  fi
  local_latest_created=`ls -1 ${local_dir}/${access}/4PubSub/${priority_name} | grep -v '^.*\.tmp$' | tail -1`
  for newly_created in `rclone --stats 0 --timeout 1m --log-level ERROR lsf --max-depth 1 ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name} | grep ${local_latest_created} -A 100000 | sed -e '1d' | grep -v '^ *$' | uniq`; do
    now=`date -u "+%Y%m%d%H%M%S"`
    set +e
    rclone --update --use-server-modtime --no-traverse --size-only --stats 0 --timeout 1m --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_sub.log copyto ${src_rclone_remote}:${src_bucket}/4PubSub/${priority_name}/${newly_created} ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp
    unclone_num=`grep ERROR ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_sub.log | wc -l`
    if test ${unclone_num} -ne 0; then
      cat ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_sub.log
      rm -f ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_sub.log
      exit 199
    fi
    set -e
    rm -f ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_sub.log
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
      rclone --ignore-checksum --update --use-server-modtime --no-gzip-encoding --no-traverse --size-only --stats 0 --timeout 1m --transfers ${parallel} --s3-upload-concurrency ${parallel} --s3-upload-cutoff 0 --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}.log copy --files-from-raw ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp ${src_rclone_remote}:${src_bucket} ${dst_rclone_remote}:${dst_bucket}
      unclone_num=`grep ERROR ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}.log | wc -l`
      if test ${unclone_num} -ne 0; then
        cat ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}.log
        rm -f ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}.log
        exit 199
      fi
      set -e
      rm -f ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}.log
      unclone_num=1
      retry_num=0
      while test ${unclone_num} -ne 0; do
        now=`date -u "+%Y%m%d%H%M%S"`
        set +e
        rclone --ignore-checksum --immutable --no-traverse --size-only --stats 0 --timeout 1m --s3-upload-cutoff 0 --log-level ERROR --log-file ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_pub.log copyto ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp ${dst_rclone_remote}:${dst_bucket}/4PubSub/${priority_name}/${now}.txt
        unclone_num=`grep ERROR ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_pub.log | wc -l`
        if test ${unclone_num} -ne 0; then
          if test ${retry_num} -ge 8; then
            cat ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_pub.log
            rm -f ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_pub.log
            exit 199
          else
            sleep 1
          fi
          retry_num=`expr 1 + ${retry_num}`
        fi
        set -e
        rm -f ${local_dir}/${access}/4Clone_log/${clone_name}/${clone_datetime}_${now}_index_pub.log
      done
      mv -f ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}.tmp ${local_dir}/${access}/4PubSub/${priority_name}/${newly_created}
    fi
  done
done
