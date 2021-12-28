#!/bin/bash
set -e
now=`date -u "+%Y%m%d%H%M%S"`
cp /dev/null arrow_to_dataset/minute/${now}.txt.tmp
cp /dev/null arrow_to_dataset/day/${now}.txt.tmp
for i in `ls -1 cache_s/sub_bufr_to_arrow/* | grep txt$`; do
  cat ${i} >> arrow_to_dataset/minute/${now}.txt.tmp
  cat ${i} >> arrow_to_dataset/day/${now}.txt.tmp
  rm -f ${i}
done
mv -f arrow_to_dataset/minute/${now}.txt.tmp arrow_to_dataset/minute/${now}.txt
mv -f arrow_to_dataset/day/${now}.txt.tmp arrow_to_dataset/day/${now}.txt
