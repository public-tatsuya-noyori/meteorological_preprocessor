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
#set -evx
sh_name=bufr_to_arrow.sh
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
if test -s bufr_to_arrow/pid.txt; then
  running=`cat bufr_to_arrow/pid.txt | xargs ps -f --no-headers | grep " $0" | wc -l`
else
  mkdir -p bufr_to_arrow/out_list
  ls -1 cache_s/4Sub/sub_iij12oip3/p3_processed > bufr_to_arrow/previous_list.txt
  running=0
fi
if test ${running} -eq 0; then
  {
    cp /dev/null bufr_to_arrow/out_list.tmp
    ls -1 cache_s/4Sub/sub_iij12oip3/p3_processed > bufr_to_arrow/current_list.txt
    for i in `diff bufr_to_arrow/previous_list.txt bufr_to_arrow/current_list.txt | grep '>' | cut -c3- | uniq`; do
      grep /surface/ cache_s/4Sub/sub_iij12oip3/p3_processed/${i} | sed -e 's|^|cache_s|g' > bufr_to_arrow/in.tmp
      ./met_pre_bufr_to_arrow.py RJTD bufr_to_arrow/in.tmp cache_bufr_to_arrow 1>> bufr_to_arrow/out_list.tmp 2>> log/met_pre_bufr_to_arrow.py.log
    done
    if test -s bufr_to_arrow/out_list.tmp; then
      grep -v ecCodes bufr_to_arrow/out_list.tmp | grep -v '^ *$' > bufr_to_arrow/out_list/`date -u +"%Y%m%d%H%M%S"`.txt
    fi
    mv -f bufr_to_arrow/current_list.txt bufr_to_arrow/previous_list.txt
  } &
  pid=$!
  echo ${pid} > bufr_to_arrow/pid.txt
  wait ${pid}
fi
