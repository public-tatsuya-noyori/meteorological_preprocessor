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
set -ex
is_download_wis_jma_to_cache_running=`touch download_wis_jma_to_cache.pid && cat download_wis_jma_to_cache.pid | xargs -n1 -I{} ps -h -q {} | wc -l`
if test ${is_download_wis_jma_to_cache_running} -eq 0; then
    ./download_wis_jma_to_cache.sh > download_wis_jma_to_cache_list_file.txt &
    echo $! > download_wis_jma_to_cache.pid
fi
is_download_noaa_batch_to_cache_running=`touch download_noaa_batch_to_cache.pid && cat download_noaa_batch_to_cache.pid | xargs -n1 -I{} ps -h -q {} | wc -l`
if test ${is_download_noaa_batch_to_cache_running} -eq 0; then
    ./download_noaa_batch_to_cache.sh > download_noaa_batch_to_cache_list_file.txt &
    echo $! > download_noaa_batch_to_cache.pid
fi
is_upload_to_object_storage_running=`touch upload_to_object_storage.pid && cat upload_to_object_storage.pid  | xargs -n1 -I{} ps -h -q {} | wc -l`
if test ${is_upload_to_object_storage_running} -eq 0; then
    ./rclone --ignore-checksum --ignore-existing --max-age 1d --max-size 1G --no-update-modtime --stats 0 --timeout 1m --transfers 32 --s3-upload-concurrency 32 --use-server-modtime -u copy work.tmp/cache/open iij1:japan-meteorological-agency-open-data --log-level ERROR --log-file upload_to_object_storage.log &
    echo $! > upload_to_object_storage.pid
fi
is_delete_object_storage_running=`touch delete_object_storage.pid && cat delete_object_storage.pid  | xargs -n1 -I{} ps -h -q {} | wc -l`
if test ${is_delete_object_storage_running} -eq 0; then
    ./rclone delete --min-age 1d --use-server-modtime iij1:japan-meteorological-agency-open-data/ --log-level ERROR --log-file delete_object_storage.log &
    echo $! > delete_object_storage.pid
fi
