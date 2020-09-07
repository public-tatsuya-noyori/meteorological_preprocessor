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
sh_name=pub_tile_arrow.sh
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
if test -s pid/${sh_name}.txt; then
  running=`cat pid/${sh_name}.txt | xargs ps -f --no-headers | grep " $0 " | wc -l`
else
  mkdir -p pid
  running=0
fi
if test ${running} -eq 0; then
  {
    for i in `ls -1 tile_arrow|grep -v '\.tmp$'|uniq`; do
      ./pub.sh --cron --rm_list_file cache_tile_arrow tile_arrow tile_arrow/${i} wasabi japan.meteorological.agency.open.data.aws.js.s3.explorer p9 8 2>>log/pub.sh.tile_arrow.log
    done
  } &
  pid=$!
  echo ${pid} > pid/${sh_name}.txt
  wait ${pid}
fi
