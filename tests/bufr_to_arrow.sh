#!/bin/sh
set -e
sh_name=bufr_to_arrow.sh
export PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin
if test -s bufr_to_arrow/pid.txt; then
  running=`cat bufr_to_arrow/pid.txt | xargs ps -f --no-headers | grep " $0" | wc -l`
else
  mkdir -p bufr_to_arrow/out_list
#  ls -1 cache_s/4Sub/iij12oo/p3/processed > bufr_to_arrow/previous_list.txt
  ls -1 download_p3/cached | grep -v '\.tmp' > bufr_to_arrow/previous_list.txt
  running=0
fi
if test ${running} -eq 0; then
  {
    cp /dev/null bufr_to_arrow/out_list.tmp
#    ls -1 cache_s/4Sub/iij12oo/p3/processed > bufr_to_arrow/current_list.txt
    ls -1  download_p3/cached | grep -v '\.tmp' > bufr_to_arrow/current_list.txt
    for i in `diff bufr_to_arrow/previous_list.txt bufr_to_arrow/current_list.txt | grep '>' | cut -c3- | uniq`; do
#      grep -E "(/surface/|/upper_air/)" cache_s/4Sub/iij12oo/p3/processed/${i} | sed -e 's|^|cache_s|g' > bufr_to_arrow/in.tmp
      grep -E "(/surface/|/upper_air/)" download_p3/cached/${i} > bufr_to_arrow/in.tmp
      ./met_pre_bufr_to_arrow.py --debug RJTD bufr_to_arrow/in.tmp cache_bufr_to_arrow 1>> bufr_to_arrow/out_list.tmp 2>> log/met_pre_bufr_to_arrow.py.log
#
      rm -f download_p3/cached/${i}
#
    done
    if test -s bufr_to_arrow/out_list.tmp; then
      grep -v ecCodes bufr_to_arrow/out_list.tmp | grep -v '^ *$' > bufr_to_arrow/out_list/`date -u +"%Y%m%d%H%M%S"`.txt
    fi
    mv -f bufr_to_arrow/current_list.txt bufr_to_arrow/previous_list.txt
  } &
  pid=$!
  echo ${pid} > bufr_to_arrow/pid.txt
  wait ${pid}
fi
