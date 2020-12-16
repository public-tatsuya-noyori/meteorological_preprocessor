#!/bin/bash
pri=$1
for i in `rclone lsf wasabi:japan.meteorological.agency.open.data.eu.central.1/4PubSub/${pri}`; do
  pd=`echo ${i} | sed -e 's/^\([0-9][0-9][0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)\.txt$/\1-\2-\3 \4:\5:\6/'`
  pu=`date --date "${pd}" "+%s"`
  j=`rclone cat wasabi:japan.meteorological.agency.open.data.eu.central.1/4PubSub/${pri}/${i} | tail -1`
  cd=`basename ${j} | cut -d'_' -f5 | cut -c1-14 | sed -e 's/^\([0-9][0-9][0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)$/\1-\2-\3 \4:\5:\6/'` 
  cu=`date --date "${cd}" "+%s"`
  k=`expr ${pu} - ${cu}`
  echo ${i} ${k}
done
