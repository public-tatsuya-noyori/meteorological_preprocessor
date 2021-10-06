#!/bin/sh
set -e
sh_name=arrow_to_tile_arrow.sh
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
mkdir -p tile_arrow/out_list tile_arrow/log
if test -s tile_arrow/pid.txt; then
  running=`cat tile_arrow/pid.txt | xargs ps -f --no-headers | grep " $0" | wc -l`
else
  running=0
fi
if test ${running} -eq 0; then
  now=`date -u "+%Y%m%d%H%M%S"`
  {
    for i in `ls -1 bufr_to_arrow/out_list|grep -v '\.tmp$'|uniq`; do
      cat bufr_to_arrow/out_list/${i} >> tile_arrow/${now}.txt.tmp
      rm -f bufr_to_arrow/out_list/${i}
    done
    if test -s tile_arrow/${now}.txt.tmp; then
      ./met_pre_arrow_to_tile_arrow.py tile_arrow/${now}.txt.tmp cache_tile_arrow/RJTD/tile_arrow_dataset 0 1>>tile_arrow/${now}.txt.tmp2 2>>log/met_pre_arrow_to_tile_arrow.py.log
      if test -s tile_arrow/${now}.txt.tmp2; then
        grep -v ecCodes tile_arrow/${now}.txt.tmp2 | sed -e "s|^cache_tile_arrow/|/|g" > tile_arrow/out_list/${now}.txt
      fi
    fi
    rm -f tile_arrow/${now}.txt.tmp
    rm -f tile_arrow/${now}.txt.tmp2
    for i in `ls -1 tile_arrow/out_list|grep -v '\.tmp$'|uniq`; do
      set +e
      rclone copy --checkers 64 --checksum --contimeout 8s --cutoff-mode=cautious --files-from-raw tile_arrow/out_list/${i} --log-file tile_arrow/log/${i}.log --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-chunk-size 8M --s3-upload-concurrency 64 --stats 0 --timeout 8s --transfers 64 cache_tile_arrow iij1:japan.meteorological.agency.1.site
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        rm -f tile_arrow/out_list/${i} tile_arrow/log/${i}.log
      fi
    done
  } &
  pid=$!
  echo ${pid} > tile_arrow/pid.txt
  wait ${pid}
fi
