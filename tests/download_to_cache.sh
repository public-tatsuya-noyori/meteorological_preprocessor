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
is_download_wis_jma_to_cache_running=`touch download_wis_jma_to_cache.pid && cat download_wis_jma_to_cache.pid | xargs -I{} ps --no-headers -q {} | wc -l`
if test ${is_download_wis_jma_to_cache_running} -eq 0; then
    ./download_wis_jma_to_cache.sh > download_wis_jma_to_cache_list_file.txt &
    echo $! > download_wis_jma_to_cache.pid
fi
#is_download_noaa_batch_to_cache_running=`touch download_noaa_batch_to_cache.pid && cat download_noaa_batch_to_cache.pid | xargs -I{} ps --no-headers -q {} | wc -l`
#if test ${is_download_noaa_batch_to_cache_running} -eq 0; then
#    ./download_noaa_batch_to_cache.sh > download_noaa_batch_to_cache_list_file.txt &
#    echo $! > download_noaa_batch_to_cache.pid
#fi
is_upload_to_object_storage_running=`touch upload_to_object_storage.pid && cat upload_to_object_storage.pid  | xargs -I{} ps --no-headers -q {} | wc -l`
if test ${is_upload_to_object_storage_running} -eq 0; then
    mkdir -p work.tmp/cache/open/0.created_URLs
    now=`date "+%Y%m%d%H%M%S"`
    if test -s work.tmp/wis_jma_to_cache.txt; then
        grep ^cache/open work.tmp/wis_jma_to_cache.txt | sed -e 's%^cache/open%https://ap1.dag.iij.gio.com/japan-meteorological-agency-oepn-data%g' > work.tmp/cache/open/0.created_URLs/system_A_${now}.txt
        ./rclone --ignore-checksum --ignore-existing --no-update-modtime --no-traverse --max-age 1d --max-size 1G --stats 0 --timeout 1m --transfers 4 copy work.tmp/cache/open iij1:japan-meteorological-agency-open-data --log-level DEBUG --log-file upload_to_object_storage.log &
        echo $! > upload_to_object_storage.pid
    fi
fi
