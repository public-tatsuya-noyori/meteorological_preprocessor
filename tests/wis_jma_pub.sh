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
mkdir -p wis_jma/cached
is_download_from_wis_jma_an_bufr_to_cache_running=`touch download_from_wis_jma_an_bufr_to_cache.pid && cat download_from_wis_jma_an_bufr_to_cache.pid | xargs -I{} ps --no-headers -q {} | wc -l`
if test ${is_download_from_wis_jma_an_bufr_to_cache_running} -eq 0; then
  ./download_from_wis_jma_an_bufr_to_cache.sh >> wis_jma/download_from_wis_jma_an_bufr_to_cache.log &
  echo $! > download_from_wis_jma_an_bufr_to_cache.pid
fi
is_pub_wis_jma_an_bufr_running=`touch pub_wis_jma_an_bufr.pid && cat pub_wis_jma_an_bufr.pid | xargs -I{} ps --no-headers -q {} | wc -l`
if test ${is_pub_wis_jma_an_bufr_running} -eq 0; then
  ./pub.sh wis_jma/cached cache iij1 japan-meteorological-agency-open-data JMA_1 8 >> wis_jma/pub.log & 
  echo $! > pub_wis_jma_an_bufr.pid
fi
