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
mkdir -p tile_arrow/out_list tile_arrow/log
if test -s tile_arrow/pid.txt; then
  running=`cat tile_arrow/pid.txt | xargs ps -f --no-headers | grep " $0" | wc -l`
else
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
      ./met_pre_arrow_to_tile_arrow.py tile_arrow/${now}.txt.tmp cache_tile_arrow/RJTD/tile_arrow_dataset 0 1>>tile_arrow/${now}.txt.tmp2 2>>log/met_pre_arrow_to_tile_arrow.py.log
      if test -s tile_arrow/${now}.txt.tmp2; then
        grep -v ecCodes tile_arrow/${now}.txt.tmp2 | sed -e "s|^cache_tile_arrow/|/|g" > tile_arrow/out_list/${now}.txt
      fi
    fi
    rm -f tile_arrow/${now}.txt.tmp
    rm -f tile_arrow/${now}.txt.tmp2
    for i in `ls -1 tile_arrow/out_list|grep -v '\.tmp$'|uniq`; do
      set +e
      rclone copy --checkers 256 --checksum --contimeout 8s --cutoff-mode=cautious --files-from-raw tile_arrow/out_list/${i} --log-file tile_arrow/log/${i}.log --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-chunk-size 8M --s3-upload-concurrency 64 --stats 0 --timeout 8s --transfers 64 cache_tile_arrow iij1:japan.meteorological.agency.1.site
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        rm -f tile_arrow/out_list/${i} tile_arrow/log/${i}.log
      fi
    done
  } &
  pid=$!
  echo ${pid} > tile_arrow/pid.txt
  wait ${pid}
fi
