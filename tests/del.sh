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
for arg in "$@"; do
  case "${arg}" in
    '--help' ) echo "del.sh num_days_ago rclone_remote bucket parallel"; exit 0;;
  esac
done
if test $# -lt 4; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
num_days_ago=$1
rclone_remote=$2
bucket=$3
parallel=$4
rclone --ignore-checksum --no-gzip-encoding --no-update-modtime --size-only --stats 0 --timeout 1m --transfers ${parallel} --log-level ERROR delete --min-age "${num_days_ago}d" --rmdirs ${rclone_remote}:${bucket}
