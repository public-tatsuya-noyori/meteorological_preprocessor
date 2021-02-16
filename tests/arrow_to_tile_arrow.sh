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
sh_name=arrow_to_tile_arrow.sh
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
if test -s tile_arrow/pid.txt; then
  running=`cat tile_arrow/pid.txt | xargs ps -f --no-headers | grep " $0" | wc -l`
else
  mkdir -p tile_arrow
  running=0
fi
if test ${running} -eq 0; then
  now=`date -u "+%Y%m%d%H%M%S"`
  {
    for i in `ls -1 bufr_to_arrow/out_list|grep -v '\.tmp$'|uniq`; do
      cat bufr_to_arrow/out_list/${i} >> tile_arrow/${now}.txt.tmp
      rm -f bufr_to_arrow/out_list/${i}
    done
    if test -s tile_arrow/${now}.txt.tmp; then
      ./met_pre_arrow_to_tile_arrow.py tile_arrow/${now}.txt.tmp cache_tile_arrow/RJTD/tile_arrow_dataset 1 1>>tile_arrow/${now}.txt.tmp2 2>>log/met_pre_arrow_to_tile_arrow.py.log
      if test -s tile_arrow/${now}.txt.tmp2; then
        grep -v ecCodes tile_arrow/${now}.txt.tmp2 > tile_arrow/${now}.txt
      fi
    fi
    rm -f tile_arrow/${now}.txt.tmp
    rm -f tile_arrow/${now}.txt.tmp2
  } &
  pid=$!
  echo ${pid} > tile_arrow/pid.txt
  wait ${pid}
fi
