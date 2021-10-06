#!/bin/sh
set -e
sh_name=pub_tile_arrow.sh
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
if test -s pid/${sh_name}.txt; then
  running=`cat pid/${sh_name}.txt | xargs ps -f --no-headers | grep " $0" | wc -l`
else
  mkdir -p pid
  running=0
fi
if test ${running} -eq 0; then
  {
    cp /dev/null tile_arrow/pub.txt.tmp
    cp /dev/null tile_arrow/pub.txt2.tmp
    for i in `ls -1 tile_arrow|grep -v '\.tmp$'|uniq`; do
      cat tile_arrow/${i} >> tile_arrow/pub.txt.tmp
      rm -f tile_arrow/${i}
    done
    if test -s tile_arrow/pub.txt.tmp; then
      cat tile_arrow/pub.txt.tmp | sort | uniq > tile_arrow/pub.txt2.tmp 
    fi
    if test -s tile_arrow/pub.txt2.tmp; then
      ./pub.sh --cron  --rm_list_file --pub_dir_list_index cache_tile_arrow tile_arrow tile_arrow/pub.txt2.tmp wasabi japan.meteorological.agency.open.data p8 8 2>>log/pub.sh.tile_arrow.log
    fi
  } &
  pid=$!
  echo ${pid} > pid/${sh_name}.txt
  wait ${pid}
fi
