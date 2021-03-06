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
if test -s pid/${sh_name}.txt; then
  running=`cat pid/${sh_name}.txt | xargs ps -f --no-headers | grep " $0" | wc -l`
else
  mkdir -p pid
  running=0
fi
if test ${running} -eq 0; then
  now=`date -u "+%Y%m%d%H%M%S"`
  {
    for i in `ls -1 sub_arrow_synop|grep -v '\.tmp$'|uniq`; do
      cat sub_arrow_synop/${i} >> tile_arrow/${now}.txt.tmp
      rm -f sub_arrow_synop/${i}
    done
    if test -s tile_arrow/${now}.txt.tmp; then
      ./pub.sh --pub_dir_list_index cache_bufr_to_arrow bufr_synop_arrow_p7 tile_arrow/${now}.txt.tmp wasabi japan.meteorological.agency.open.data p7 8 2>>log/pub.sh.bufr_synop_arrow.log
      echo "start"  >>log/met_pre_arrow_to_tile_arrow.py.log
      date  >>log/met_pre_arrow_to_tile_arrow.py.log
      ./met_pre_arrow_to_tile_arrow.py tile_arrow/${now}.txt.tmp cache_tile_arrow/RJTD/tile_arrow_dataset 1 1>>tile_arrow/${now}.txt.tmp2 2>>log/met_pre_arrow_to_tile_arrow.py.log
      echo "end"  >>log/met_pre_arrow_to_tile_arrow.py.log
      date  >>log/met_pre_arrow_to_tile_arrow.py.log
      if test -s tile_arrow/${now}.txt.tmp2; then
        grep -v ecCodes tile_arrow/${now}.txt.tmp2 > tile_arrow/${now}.txt
      fi
    fi
    rm -f tile_arrow/${now}.txt.tmp
    rm -f tile_arrow/${now}.txt.tmp2
  } &
  pid=$!
  echo ${pid} > pid/${sh_name}.txt
  wait ${pid}
fi
