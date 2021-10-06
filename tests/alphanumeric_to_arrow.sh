#!/bin/sh
set -e
sh_name=alphanumeric_to_arrow.sh
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
if test -s alphanumeric_to_arrow/pid.txt; then
  running=`cat alphanumeric_to_arrow/pid.txt | xargs ps -f --no-headers | grep " $0" | wc -l`
else
  mkdir -p alphanumeric_to_arrow/out_list
#  ls -1 cache_s/4Sub/iij12oo/p2/processed > alphanumeric_to_arrow/previous_list.txt
  ls -1 download_p2/cached | grep -v '\.tmp' > alphanumeric_to_arrow/previous_list.txt
  running=0
fi
if test ${running} -eq 0; then
  {
    cp /dev/null alphanumeric_to_arrow/out_list.tmp
#    ls -1 cache_s/4Sub/iij12oo/p2/processed > alphanumeric_to_arrow/current_list.txt
    ls -1 download_p2/cached | grep -v '\.tmp' > alphanumeric_to_arrow/current_list.txt
    for i in `diff alphanumeric_to_arrow/previous_list.txt alphanumeric_to_arrow/current_list.txt | grep '>' | cut -c3- | uniq`; do
#      grep -E "(/synop/|/ship/|/synop_mobil/)" cache_s/4Sub/iij12oo/p2/processed/${i} | sed -e 's|^|cache_s|g' > alphanumeric_to_arrow/in.tmp
      grep -E "(/synop/|/ship/|/synop_mobil/)" download_p2/cached/${i} > alphanumeric_to_arrow/in.tmp
      ./met_pre_alphanumeric_to_arrow.py --debug RJTD alphanumeric_to_arrow/in.tmp cache_alphanumeric_to_arrow 1>> alphanumeric_to_arrow/out_list.tmp 2>> log/met_pre_alphanumeric_to_arrow.py.log
#
      rm -f download_p2/cached/${i}
#
    done
    if test -s alphanumeric_to_arrow/out_list.tmp; then
      grep -v ecCodes alphanumeric_to_arrow/out_list.tmp | grep -v '^ *$' > alphanumeric_to_arrow/out_list/`date -u +"%Y%m%d%H%M%S"`.txt
    fi
    mv -f alphanumeric_to_arrow/current_list.txt alphanumeric_to_arrow/previous_list.txt
  } &
  pid=$!
  echo ${pid} > alphanumeric_to_arrow/pid.txt
  wait ${pid}
fi
