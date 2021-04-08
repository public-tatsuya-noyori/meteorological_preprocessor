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
alias python='/usr/bin/python3'
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
set -e
if test $1 = 'p1'; then
  priority=p1
  parallel=16
  format=Alphanumeric
  category=Warning
elif test $1 = 'p2'; then
  priority=p2
  parallel=16
  format=Alphanumeric
  category='!Warning'
elif test $1 = 'p2_crex'; then
  priority=p2_crex
  parallel=4
  format=CREX
  category='!Warning'
elif test $1 = 'p3'; then
  priority=p3
  parallel=16
  format=BUFR
  category='!Satellite'
elif test $1 = 'p4'; then
  priority=p4
  parallel=16
  format=BUFR
  category='Satellite'
elif test $1 = 'p5'; then
  priority=p5
  parallel=16
  format=GRIB
  category=''
fi
if test -s download_${priority}/pid.txt; then
  running=`cat download_${priority}/pid.txt | xargs ps -f --no-headers | grep " $0 " | grep " ${priority}" | wc -l`
else
  mkdir -p download_${priority}/cached
  running=0
fi
if test ${running} -eq 0; then

{
rm -f download_${priority}/created.txt download_${priority}/aria2c.log download_${priority}/get_list_stdout.txt
if test -s download_${priority}/etag.txt; then
  etag=`cat download_${priority}//etag.txt`
  since="If-None-Match: ${etag}"
  aria2c --check-certificate=false -j 1 -s 1 -x 1 --header "${since}" --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l download_${priority}/aria2c.log -o download_${priority}/created.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Type=${format}&Access=Open&Category=${category}" >> download_${priority}/get_list_stdout.txt
else
  aria2c --check-certificate=false -j 1 -s 1 -x 1 --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l download_${priority}/aria2c.log -o download_${priority}/created.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Type=${format}&Access=Open&Category=${category}" >> download_${priority}/get_list_stdout.txt
fi
if test ! -s download_${priority}/aria2c.log; then
  exit 0
fi
grep "ETag:" download_${priority}/aria2c.log | tail -1 | cut -d' ' -f2 > download_${priority}/etag.txt
if test -s download_${priority}/created.txt; then
  if test ! -s download_${priority}/created.txt; then
    exit 0
  fi
  now=`date -u "+%Y%m%d%H%M%S"`
  created_num=`cat download_${priority}/created.txt | wc -l`
  while test ${created_num} -gt 0; do
    rm -rf download_${priority}/downloaded download_${priority}/aria2c.log download_${priority}/get_file_stdout.txt
    mkdir -p download_${priority}/downloaded
    aria2c --check-certificate=false -j ${parallel} -s ${parallel} -x ${parallel} --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=error -l download_${priority}/aria2c.log -i download_${priority}/created.txt -d download_${priority}/downloaded >> download_${priority}/get_file_stdout.txt
    ./met_pre_batch_to_cache.py RJTD download_${priority}/downloaded download_${priority}/checksum.feather cache_o 1>> download_${priority}/cached/${now}.txt.tmp 2>> download_${priority}/met_pre_batch_to_cache.log
    grep -F '[ERROR]' download_${priority}/aria2c.log | grep 'URI=' | sed -e 's/^.*URI=//g' | grep -v '^ *$' | sort -u > download_${priority}/created.txt
    if test -s download_${priority}/created.txt; then
      created_num=`cat download_${priority}/created.txt | wc -l`
    else
      created_num=0
    fi
  done
  if test -s download_${priority}/cached/${now}.txt.tmp; then
    cat download_${priority}/cached/${now}.txt.tmp | grep -v "/A_P" | grep -v ecCodes | uniq > download_${priority}/cached/${now}.txt
    if test ! -s download_${priority}/cached/${now}.txt; then
      rm -f download_${priority}/cached/${now}.txt
    fi
  fi
  rm -f download_${priority}/cached/${now}.txt.tmp
fi
} &
pid=$!
echo ${pid} > download_${priority}/pid.txt
wait ${pid}

fi
