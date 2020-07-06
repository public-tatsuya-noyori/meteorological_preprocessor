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
mkdir -p wis_jma_urgent/cached
is_download_from_wis_jma_urgent_an_bufr_to_cache_running=`touch download_from_wis_jma_urgent_an_bufr_to_cache.pid && cat download_from_wis_jma_urgent_an_bufr_to_cache.pid | xargs -I{} ps --no-headers -q {} | wc -l`
if test ${is_download_from_wis_jma_urgent_an_bufr_to_cache_running} -eq 0; then
  ./download_from_wis_jma_urgent_an_bufr_to_cache.sh >> wis_jma_urgent/download_from_wis_jma_urgent_an_bufr_to_cache.log
  for raw_list_file in `ls -1 wis_jma_urgent/cached`; do
    ./pub.sh wis_jma_urgent/cached/${raw_list_file} cache iij1 japan-meteorological-agency-open-data JMA_1 8 >> wis_jma_urgent/pub.log && rm -f wis_jma_urgent/cached/${raw_list_file} &
  done &
  echo $! > download_from_wis_jma_urgent_an_bufr_to_cache.pid
fi
