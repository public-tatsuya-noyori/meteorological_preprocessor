#!/bin/bash
set -e

make_dataset(){
  cp /dev/null ${work_directory}/${name}_input.txt
  for i in `ls -1 ${input_directory1}/* ${input_directory2}/* | grep txt$`; do
    cat ${i} >> ${work_directory}/${name}_input.txt
    rm -f ${i}
  done 
  ./met_pre_arrow_to_dataset.py --debug ${work_directory}/${name}_input.txt ${work_directory}/${name} 1>${work_directory}/${name}_output.txt 2>>${work_directory}/${name}_stderr.log
  if test -s ${work_directory}/${name}_output.txt; then
    cat ${work_directory}/${name}_output.txt | xargs -r -n 64 -P 16 gzip -f
    cat ${work_directory}/${name}_output.txt | xargs -r -n 1 -P 16 -I {} mv -f {}.gz {}
    sed -e "s|${work_directory}/||g" ${work_directory}/${name}_output.txt > ${work_directory}/${name}_raw.txt
    timeout -k 3 300 rclone copy --header-upload 'Content-Encoding: gzip' --checksum --contimeout 8s --files-from-raw ${work_directory}/${name}_raw.txt --log-file ${work_directory}/${name}_rclone.log --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s --transfers 16 ${work_directory} ${rclone_remote_bucket}
    gzip -f ${work_directory}/${name}_raw.txt
    now=`date -u "+%Y%m%d%H%M%S"`
    timeout -k 3 50 rclone copyto --header-upload 'Content-Encoding: gzip' --checksum --contimeout 8s --log-file ${work_directory}/${name}_rclone.log --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s --transfers 1 ${work_directory}/${name}_raw.txt.gz ${rclone_remote_bucket}/4PubSub/${dataset_directory}/${now}.txt
    cat ${work_directory}/${name}_output.txt | xargs -r -n 1 -P 16 -I {} mv -f {} {}.gz
    cat ${work_directory}/${name}_output.txt | sed -e 's|$|.gz|' | xargs -r -n 64 -P 16 gunzip -f
  fi
}

input_directory1=cache_tile_dataset/sub_bufr_to_tile_dataset
input_directory2=cache_tile_dataset_satellite/sub_bufr_to_tile_dataset
rclone_remote_bucket=minio:aa-open-dataset
work_directory=cache_4all_dataset
name=4All
dataset_directory=4all_arrow
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
