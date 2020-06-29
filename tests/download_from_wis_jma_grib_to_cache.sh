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
mkdir -p wis_jma_nwfc/cached
parallel=8
is_pre=1
if test -f wis_jma_nwfc/etag.txt; then
  etag=`cat wis_jma_nwfc/etag.txt`
else
  is_pre=0
  cp /dev/null wis_jma_nwfc/etag.txt
fi
rm -f wis_jma_nwfc/created.txt wis_jma_nwfc/aria2c.log wis_jma_nwfc/get_list_stdout.txt
if test ${is_pre} -eq 0; then
  aria2c --check-certificate=false -j 1 -s 1 -x 1 --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l wis_jma_nwfc/aria2c.log -o wis_jma_nwfc/created.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Access=Open&Category=!Warning" >> wis_jma_nwfc/get_list_stdout.txt
else
  since="If-None-Match: ${etag}"
  aria2c --check-certificate=false -j 1 -s 1 -x 1 --header "${since}" --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l wis_jma_nwfc/aria2c.log -o wis_jma_nwfc/created.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Access=Open&Category=!Warning" >> wis_jma_nwfc/get_list_stdout.txt
fi
err_num=`grep -F '[ERROR]' wis_jma_nwfc/aria2c.log | wc -l`
if test ${err_num} -eq 0; then
  grep "ETag:" wis_jma_nwfc/aria2c.log | tail -1 | cut -d' ' -f2 > wis_jma_nwfc/etag.txt
  if test ${is_pre} -eq 1 -a -s wis_jma_nwfc/created.txt; then
    grep -E '/(Alphanumeric|BUFR)/' wis_jma_nwfc/created.txt > wis_jma_nwfc/tmp_created.txt
    mv -f wis_jma_nwfc/tmp_created.txt wis_jma_nwfc/created.txt 
    now=`date "+%Y%m%d%H%M%S"`
    created_num=`cat wis_jma_nwfc/created.txt | wc -l`
    while test ${created_num} -ne 0; do
      rm -rf wis_jma_nwfc/downloaded wis_jma_nwfc/aria2c.log wis_jma_nwfc/get_file_stdout.txt
      mkdir -p wis_jma_nwfc/downloaded
      set +e
      aria2c --check-certificate=false -j ${parallel} -s ${parallel} -x ${parallel} --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=error -l wis_jma_nwfc/aria2c.log -i wis_jma_nwfc/created.txt -d wis_jma_nwfc/downloaded >> wis_jma_nwfc/get_file_stdout.txt
      set -e
      met_pre_batch_to_cache RJTD wis_jma_nwfc/downloaded cache 1>> wis_jma_nwfc/cached/${now}.txt.tmp 2>> wis_jma_nwfc/met_pre_batch_to_cache.log
      grep -F '[ERROR]' wis_jma_nwfc/aria2c.log | grep 'URI=' | sed -e 's/^.*URI=//g' | grep -v '^ *$' | sort -u > wis_jma_nwfc/created.txt
      if test -s wis_jma_nwfc/created.txt; then
        sleep 15
      fi
      created_num=`cat wis_jma_nwfc/created.txt | wc -l`
    done
    mv -f wis_jma_nwfc/cached/${now}.txt.tmp wis_jma_nwfc/cached/${now}.txt
  fi
fi
