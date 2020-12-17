#!/bin/bash
pri=$1
for i in `rclone lsf wasabi:japan.meteorological.agency.open.data.eu.central.1/4PubSub/${pri}`; do
  j=`rclone cat wasabi:japan.meteorological.agency.open.data.eu.central.1/4PubSub/${pri}/${i} | wc -l`
  echo ${i} ${j}
done
