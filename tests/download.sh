#!/bin/sh
alias python='/usr/bin/python3'
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
set -e
if test $1 = 'p1'; then
  priority=p1
  parallel=1
  format=Alphanumeric
  category=Warning
elif test $1 = 'p2'; then
  priority=p2
  parallel=1
  format=Alphanumeric
  category=''
elif test $1 = 'p2_crex'; then
  priority=p2_crex
  parallel=1
  format=CREX
  category=''
elif test $1 = 'p3'; then
  priority=p3
  parallel=16
  format=BUFR
  category=''
elif test $1 = 'p4'; then
  priority=p4
  parallel=16
  format=GRIB
  category=''
fi
regions=$2
center=''
if test $2 = '1'; then
  region1='Africa'
  region2='Europe'
  center='AAAA'
elif test $2 = '2'; then
  region1='Asia'
  region2='South-West Pacific'
  center='BBBB'
elif test $2 = '3'; then
  region1='North America, Central America and the Caribbean'
  region2='South America'
  center='CCCC'
fi
closed=''
user=''
passwd=''
if test "$3" = 'closed'; then
  closed='_closed'
  user=`head -1 wis_user_passwd.txt`
  passwd=`tail -1 wis_user_passwd.txt`
fi
if test -s download_${priority}_${regions}${closed}/pid.txt; then
  running=`cat download_${priority}_${regions}${closed}/pid.txt | xargs ps -f --no-headers | grep " $0 " | grep " ${priority}" | wc -l`
else
  mkdir -p download_${priority}_${regions}${closed}/cached
  running=0
fi
touch download_${priority}_${regions}${closed}/cached/dummy.tmp
if test ! -f download_${priority}_${regions}${closed}/checksum.arrow; then
  cp checksum.arrow download_${priority}_${regions}${closed}/
fi
if test ${running} -eq 0; then
{
rm -f download_${priority}_${regions}${closed}/created.txt download_${priority}_${regions}${closed}/aria2c.log download_${priority}_${regions}${closed}/get_list_stdout.txt
if test -s download_${priority}_${regions}${closed}/etag.txt; then
  etag=`cat download_${priority}_${regions}${closed}//etag.txt`
  since="If-None-Match: ${etag}"
  if test -n "${closed}"; then
    aria2c --check-certificate=false -j 1 -s 1 -x 1 --header "${since}" --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l download_${priority}_${regions}${closed}/aria2c.log -o download_${priority}_${regions}${closed}/created.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Type=${format}&Access=Closed&Category=${category}&Region=${region1}&Region=${region2}" >> download_${priority}_${regions}${closed}/get_list_stdout.txt
  else
    aria2c --http-user=${user} --http-passwd=${passwd} --check-certificate=false -j 1 -s 1 -x 1 --header "${since}" --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l download_${priority}_${regions}${closed}/aria2c.log -o download_${priority}_${regions}${closed}/created.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Type=${format}&Access=Open&Category=${category}&Region=${region1}&Region=${region2}" >> download_${priority}_${regions}${closed}/get_list_stdout.txt
  fi
else
  if test -n "${closed}"; then
    aria2c --check-certificate=false -j 1 -s 1 -x 1 --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l download_${priority}_${regions}${closed}/aria2c.log -o download_${priority}_${regions}${closed}/created.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Type=${format}&Access=Closed&Category=${category}&Region=${region1}&Region=${region2}" >> download_${priority}_${regions}${closed}/get_list_stdout.txt
  else
    aria2c --http-user=${user} --http-passwd=${passwd} --check-certificate=false -j 1 -s 1 -x 1 --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=info -l download_${priority}_${regions}${closed}/aria2c.log -o download_${priority}_${regions}${closed}/created.txt "https://www.wis-jma.go.jp/data/syn?ContentType=Text&Type=${format}&Access=Open&Category=${category}&Region=${region1}&Region=${region2}" >> download_${priority}_${regions}${closed}/get_list_stdout.txt
  fi
fi
if test ! -s download_${priority}_${regions}${closed}/aria2c.log; then
  exit 0
fi
grep "ETag:" download_${priority}_${regions}${closed}/aria2c.log | tail -1 | cut -d' ' -f2 > download_${priority}_${regions}${closed}/etag.txt
if test -s download_${priority}_${regions}${closed}/created.txt; then
  cat download_${priority}_${regions}${closed}/created.txt | grep -v "/A_P" | sort -u > download_${priority}_${regions}${closed}/created.txt.tmp
  mv -f download_${priority}_${regions}${closed}/created.txt.tmp download_${priority}_${regions}${closed}/created.txt
  if test ! -s download_${priority}_${regions}${closed}/created.txt; then
    exit 0
  fi
  now=`date -u "+%Y%m%d%H%M%S"`
  created_num=`cat download_${priority}_${regions}${closed}/created.txt | wc -l`
  while test ${created_num} -gt 0; do
    rm -rf download_${priority}_${regions}${closed}/downloaded download_${priority}_${regions}${closed}/aria2c.log download_${priority}_${regions}${closed}/get_file_stdout.txt
    mkdir -p download_${priority}_${regions}${closed}/downloaded
    if test -n "${closed}"; then
      aria2c --check-certificate=false -j ${parallel} -s ${parallel} -x ${parallel} --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=error -l download_${priority}_${regions}${closed}/aria2c.log -i download_${priority}_${regions}${closed}/created.txt -d download_${priority}_${regions}${closed}/downloaded >> download_${priority}_${regions}${closed}/get_file_stdout.txt
    else
      aria2c --http-user=${user} --http-passwd=${passwd} --check-certificate=false -j ${parallel} -s ${parallel} -x ${parallel} --header 'Cache-Control: no-cache' --auto-file-renaming=false --allow-overwrite=false --log-level=error -l download_${priority}_${regions}${closed}/aria2c.log -i download_${priority}_${regions}${closed}/created.txt -d download_${priority}_${regions}${closed}/downloaded >> download_${priority}_${regions}${closed}/get_file_stdout.txt
    fi
    ./met_pre_batch_to_cache.py ${center} download_${priority}_${regions}${closed}/downloaded cache_${regions}${closed} download_${priority}_${regions}${closed}/checksum.arrow 1>> download_${priority}_${regions}${closed}/cached/${now}.txt.tmp 2>> download_${priority}_${regions}${closed}/met_pre_batch_to_cache.log
    grep -F '[ERROR]' download_${priority}_${regions}${closed}/aria2c.log | grep 'URI=' | sed -e 's/^.*URI=//g' | grep -v '^ *$' | sort -u > download_${priority}_${regions}${closed}/created.txt
    if test -s download_${priority}_${regions}${closed}/created.txt; then
      created_num=`cat download_${priority}_${regions}${closed}/created.txt | wc -l`
    else
      created_num=0
    fi
  done
  if test -s download_${priority}_${regions}${closed}/cached/${now}.txt.tmp; then
    cat download_${priority}_${regions}${closed}/cached/${now}.txt.tmp | grep -v ecCodes | uniq > download_${priority}_${regions}${closed}/cached/${now}.txt
    if test ! -s download_${priority}_${regions}${closed}/cached/${now}.txt; then
      rm -f download_${priority}_${regions}${closed}/cached/${now}.txt
    fi
  fi
  rm -f download_${priority}_${regions}${closed}/cached/${now}.txt.tmp
fi
} &
pid=$!
echo ${pid} > download_${priority}_${regions}${closed}/pid.txt
wait ${pid}

fi
