#!/bin/sh
alias python='/usr/bin/python3'
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
set -e
rclone_remote_bucket_list=$2
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
  priority=p2
  parallel=16
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
elif test $1 = 'p4'; then
  priority=p5
  parallel=16
  format=GRIB
  category=''
fi
if test -s download_${priority}/pid.txt; then
  running=`cat download_${priority}/pid.txt | xargs ps -f --no-headers | grep " $0 " | grep " ${priority} " | wc -l`
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
  cat download_${priority}/created.txt | grep -v "/A_ISXX[0-9][0-9]EUSR" | grep -v "/A_P" | sort -u > download_${priority}/created.txt.tmp
  mv -f download_${priority}/created.txt.tmp download_${priority}/created.txt
  if ! test -s download_${priority}/created.txt; then
    exit 0
  fi
  now=`date -u "+%Y%m%d%H%M%S"`
  created_num=`cat download_${priority}/created.txt | wc -l`
  while test ${created_num} -gt 0; do
    rm -rf download_${priority}/downloaded download_${priority}/aria2c.log download_${priority}/get_file_stdout.txt
    mkdir -p download_${priority}/downloaded
    aria2c --check-certificate=false -j ${parallel} -s ${parallel} -x ${parallel} --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=error -l download_${priority}/aria2c.log -i download_${priority}/created.txt -d download_${priority}/downloaded >> download_${priority}/get_file_stdout.txt
    ./met_pre_batch_to_cache.py RJTD download_${priority}/downloaded cache_o download_${priority}/checksum.feather 1>> download_${priority}/cached/${now}.txt.tmp 2>> download_${priority}/met_pre_batch_to_cache.log
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
for i in `ls -1 download_${priority}/cached/*|grep -v '\.tmp$'|uniq`;do ./pub.sh --rm_input_index_file cache_o minio1open ${priority} ${i} "${rclone_remote_bucket_list}" 16;done
} &
pid=$!
echo ${pid} > download_${priority}/pid.txt
wait ${pid}

fi
