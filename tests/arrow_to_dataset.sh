#!/bin/bash
set -e

make_dataset(){
  cp /dev/null ${work_directory}/input.txt
  for i in `ls -1 cache_s/sub_bufr_to_arrow/*.txt`; do
    cat ${i} >> ${work_directory}/input.txt
    rm -f ${i}
  done 
  ./met_pre_arrow_to_dataset.py --debug ${work_directory}/input.txt ${work_directory} 1>${work_directory}/output.txt 2>>${work_directory}/0_dataset_stderr.log
  cat ${work_directory}/output.txt | xargs -r -n 64 -P 16 gzip -f
  cat ${work_directory}/output.txt | xargs -r -n 1 -P 16 -I {} mv -f {}.gz {}
  sed -e "s|${parent_work_directory}/||g" ${work_directory}/output.txt > ${work_directory}/raw.txt
  timeout -k 3 300 rclone copy --header-upload 'Content-Encoding: gzip' --checksum --files-from-raw ${work_directory}/raw.txt --log-file ${work_directory}/rclone.log --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --transfers 16 ${parent_work_directory} ${rclone_remote_bucket}
  cat ${work_directory}/output.txt | xargs -r -n 1 -P 16 -I {} mv -f {} {}.gz
  cat ${work_directory}/output.txt | sed -e 's|$|.gz|' | xargs -r -n 64 -P 16 gunzip -f
}

rclone_remote_bucket=minio:center-bb-cloud-b-region-c-open-main
parent_work_directory=arrow_to_dataset
work_directory=${parent_work_directory}/0_dataset
mkdir -p ${work_directory}
if test -s ${work_directory}/pid.txt; then
  running=`cat ${work_directory}/pid.txt | xargs -r ps ho 'pid comm args' | grep -F " $0" | wc -l`
else
  running=0
fi
if test ${running} -eq 0; then
  make_dataset &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  set +e
  wait ${pid}
  ec=$?
  set -e
  rm ${work_directory}/pid.txt
fi
