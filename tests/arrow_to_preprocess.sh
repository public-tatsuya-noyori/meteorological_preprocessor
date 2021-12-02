#!/bin/bash
set -e
now=`date -u "+%Y%m%d%H%M%S"`
cp /dev/null arrow_to_dataset/minute/${now}.txt
cp /dev/null arrow_to_dataset/day/${now}.txt
for i in `ls -1 cache_s/sub_bufr_to_arrow/* | grep txt$`; do
  cat ${i} >> arrow_to_dataset/minute/${now}.txt
  cat ${i} >> arrow_to_dataset/day/${now}.txt
  rm -f ${i}
done 
