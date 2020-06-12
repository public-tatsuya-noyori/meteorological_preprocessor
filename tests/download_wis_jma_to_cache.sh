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
parallel=4
set -e
mkdir -p work.tmp
cd work.tmp
is_pre=1
if test -f wis_jma_etag.txt; then
    etag=`cat wis_jma_etag.txt`
else
    is_pre=0
    cp /dev/null wis_jma_etag.txt
fi
rm -f wis_jma_updated.txt
rm -f wis_jma_log.txt
rm -f wis_jma_get1.txt
if test ${is_pre} -eq 0; then
    aria2c -j 1 -s 1 -x 1 --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l wis_jma_log.txt -o wis_jma_updated.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Access=Open" >> wis_jma_get1.txt
else
    since="If-None-Match: ${etag}"
    aria2c -j 1 -s 1 -x 1 --header "${since}" --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l wis_jma_log.txt -o wis_jma_updated.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Access=Open" >> wis_jma_get1.txt
fi
err_num=`grep -F '[ERROR]' wis_jma_log.txt | wc -l`
if test ${err_num} -eq 0; then
    grep "ETag:" wis_jma_log.txt | tail -1 | cut -d' ' -f2 > wis_jma_etag.txt
    if test ${is_pre} -eq 1; then
        updated_num=`cat wis_jma_updated.txt | wc -l`
        mkdir -p cache
        rm -rf wis_jma
        rm -f wis_jma_get2.txt
        while test ${updated_num} -ne 0; do
            rm -f wis_jma_log.txt
            mkdir -p wis_jma
	    set +e
            aria2c -j ${parallel} -s ${parallel} -x ${parallel} --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=error -l wis_jma_log.txt -i wis_jma_updated.txt -d wis_jma >> wis_jma_get2.txt
	    set -e
            met_pre_batch_to_cache RJTD wis_jma cache
            grep -F '[ERROR]' wis_jma_log.txt | grep 'URI=' | sed -e 's/^.*URI=//g' | grep -v '^ *$' | sort -u > wis_jma_updated.txt
            updated_num=`cat wis_jma_updated.txt | wc -l`
            rm -rf wis_jma
        done
    fi
fi
