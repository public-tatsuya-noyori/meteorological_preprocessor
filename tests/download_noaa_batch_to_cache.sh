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
dir_list='https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sflnd/DS.metar/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sflnd/DS.sclim/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sflnd/DS.synop/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sflnd/DS.tafst/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sfmar/DS.dbuoy/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sfmar/DS.ships/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sluan/DS.airep/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sluan/DS.airmet/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sluan/DS.amdar/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sluan/DS.pirep/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sluan/DS.recco/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.sluan/DS.sigmt/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.vsndn/DS.dropw/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.vsndn/DS.prflr/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.vsndn/DS.raobf/
https://tgftp.nws.noaa.gov/SL.us008001/DF.an/DC.vsndn/DS.raobs/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bb/DC.radar/DS.carib/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bb/DC.sluan/DS.recco/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.intl/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.mos/DS.mavjs/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.mos/DS.mexjs/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.sfsat/DS.altika/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.sfsat/DS.gssd/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.sfsat/DS.hdw/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.sfsat/DS.qscat/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.sfsat/DS.swind/
https://tgftp.nws.noaa.gov/SL.us008001/DF.bf/DC.tacbf/
https://tgftp.nws.noaa.gov/SL.us008001/DF.c5/DC.bathy/'
is_pre=1
if test -f noaa_pre_file_time_list.txt; then
    if test -f noaa_file_time_list.txt; then
        mv -f noaa_file_time_list.txt noaa_pre_file_time_list.txt
    fi
else
    is_pre=0
    cp /dev/null noaa_pre_file_time_list.txt
fi
rm -f noaa_file_time_list.txt
rm -f noaa_log.txt
rm -f noaa_get1.txt
for dir in `echo "${dir_list}"`; do
    rm -f noaa_tmp.txt
    aria2c -j 1 -s 1 -x 1 --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=error -l noaa_log.txt -o noaa_tmp.txt "${dir}" >> noaa_get1.txt
    cat noaa_tmp.txt | grep -E '\.(txt|bin)</a></td>' | sed -e "s%^.*href=\"%${dir}%g" | grep -v '^ *$' | sort -u >> noaa_file_time_list.txt
done
err_num=`grep -F '[ERROR]' noaa_log.txt | wc -l`
if test ${is_pre} -eq 1 -a ${err_num} -eq 0; then
    diff <(sort -u noaa_pre_file_time_list.txt) <(sort -u noaa_file_time_list.txt) | grep '^>' | cut -c3- | sed -e 's/".*//g' | grep -v '^ *$' | sort -u > noaa_updated.txt
    updated_num=`cat noaa_updated.txt | wc -l`
    mkdir -p cache
    rm -rf batch_noaa
    rm -f noaa_get2.txt
    while test ${updated_num} -ne 0; do
        rm noaa_log.txt
        mkdir -p batch_noaa
	set +e
        aria2c -j ${parallel} -s ${parallel} -x ${parallel} --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=error -l noaa_log.txt -i noaa_updated.txt -d batch_noaa >> noaa_get2.txt
	set -e
        met_pre_batch_to_cache RJTD batch_noaa cache
        grep -F '[ERROR]' noaa_log.txt | grep 'URI=' | sed -e 's/^.*URI=//g' | grep -v '^ *$' | sort -u > noaa_updated.txt
        updated_num=`cat noaa_updated.txt | wc -l`
        rm -rf batch_noaa
    done
fi
