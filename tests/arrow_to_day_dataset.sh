#!/bin/bash
set -e

make_dataset(){
  cp /dev/null ${work_directory}/${name}_input.txt
  for i in `ls -1 ${work_directory}/${name}/*|grep txt$`; do
    cat ${i} >> ${work_directory}/${name}_input.txt
    rm -f ${i}
  done 
  ./met_pre_arrow_to_${name}_dataset.py --debug ${work_directory}/${name}_input.txt ${work_directory} 1>${work_directory}/${name}_output.txt 2>>${work_directory}/${name}_stderr.log
  cat ${work_directory}/${name}_output.txt | xargs -r -n 64 -P 16 gzip -f
  cat ${work_directory}/${name}_output.txt | xargs -r -n 1 -P 16 -I {} mv -f {}.gz {}
  sed -e "s|${work_directory}/||g" ${work_directory}/${name}_output.txt > ${work_directory}/${name}_raw.txt
  timeout -k 3 300 rclone copy --header-upload 'Content-Encoding: gzip' --checksum --contimeout 8s --files-from-raw ${work_directory}/${name}_raw.txt --log-file ${work_directory}/${name}_rclone.log --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s --transfers 16 ${work_directory} ${rclone_remote_bucket}
  gzip -f ${work_directory}/${name}_raw.txt
  now=`date -u "+%Y%m%d%H%M%S"`
  timeout -k 3 50 rclone copyto --header-upload 'Content-Encoding: gzip' --checksum --contimeout 8s --log-file ${work_directory}/${name}_rclone.log --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s --transfers 1 ${work_directory}/${name}_raw.txt.gz ${rclone_remote_bucket}/4PubSub/${dataset_directory}/${now}.txt
  cat ${work_directory}/${name}_output.txt | xargs -r -n 1 -P 16 -I {} mv -f {} {}.gz
  cat ${work_directory}/${name}_output.txt | sed -e 's|$|.gz|' | xargs -r -n 64 -P 16 gunzip -f
}

rclone_remote_bucket=minio:aa-open-dataset
work_directory=arrow_to_dataset
name=day
dataset_directory=1DayDataset
mkdir -p ${work_directory}
if test -s ${work_directory}/${name}_pid.txt; then
  running=`cat ${work_directory}/${name}_pid.txt | xargs -r ps ho 'pid comm args' | grep -F " $0" | wc -l`
else
  running=0
fi
if test ${running} -eq 0; then
  make_dataset &
  pid=$!
  echo ${pid} > ${work_directory}/${name}_pid.txt
  set +e
  wait ${pid}
  ec=$?
  set -e
  rm ${work_directory}/${name}_pid.txt
fi
