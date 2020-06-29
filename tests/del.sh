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
open=1
opt_num=0
for arg in "$@"; do
  case "${arg}" in
    '--help' ) echo "del_mc.sh num_days_ago rclone_remote bucket"; exit 0;;
  esac
done
if test $# -lt `expr 6 + ${opt_num}`; then
  echo -e "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information."
  exit 199
fi
date=`date +"%Y%m%d" --date "$1 days ago"`
rclone_remote=$2
bucket=$3
mc rm --recursive --force --insecure --older-than $1d ${rclone_remote}/${bucket}
