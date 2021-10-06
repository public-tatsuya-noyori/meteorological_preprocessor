function handler () {
  set +o pipefail
  source_rclone_remote_bucket_main_sub='jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub'
  source_center_id=jma
  extension_type=bin
  function=clone_jma_bin_sub_function
  main_sub_num=2
  the_number_of_execution_within_timeout=11
  parallel=16
  export HOME="/tmp"
  export PATH="$HOME/aws-cli/bin:$HOME/rclone/bin:$PATH"
  account=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^account | cut -d= -f2 | sed -e 's| ||g'`
  region=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^region_main_sub | cut -d= -f2 | sed -e 's| ||g' | cut -d';' -f${main_sub_num}`
  running=`aws stepfunctions list-executions --state-machine-arn arn:aws:states:${region}:${account}:stateMachine:${function} --max-item ${the_number_of_execution_within_timeout} | grep '"status": "' | grep RUNNING | wc -l`
  if test ${running} -gt 1; then
    return 0
  fi
  rclone_remote_bucket_main_sub=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^rclone_remote_bucket_main_sub | cut -d= -f2 | sed -e 's| ||g'`
  center_id=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^center_id | cut -d= -f2 | sed -e 's| ||g'`
  clone_directory=4Clone
  clone_work_directory=$HOME/${clone_directory}
  pub_clone_directory=4PubClone
  pub_clone_work_directory=$HOME/${pub_clone_directory}
  destination_tar_index_directory=4TarIndex
  tar_index_directory=4Clone_TarIndex
  tar_index_work_directory=$HOME/${tar_index_directory}
  IFS=$'\n'
  rc=0
  mkdir -p ${clone_work_directory}/${source_center_id}/${extension_type}
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    rm -rf ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
    mkdir -p ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
    cp /dev/null ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt
    for destination_rclone_remote_bucket in `echo "${rclone_remote_bucket_main_sub}" | tr ';' '\n'`; do
      set +e
      timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/ | grep -E '^[0-9]+\.txt' | sed -e "s|$|/${destination_rclone_remote_bucket}|g" >> ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        rc=${exit_code}
      fi
    done
    if test -s ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt; then
      remote_bucket_former_index=`sort -u ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt | tail -1 | sed -e "s|\([^/]\+\)/\([^/]\+\)|\2/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/\1|"`
      remote_bucket=`echo ${remote_bucket_former_index} | cut -d/ -f1`
      former_index=`echo ${remote_bucket_former_index} | cut -d/ -f2-`
      echo ${former_index} > ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/raw.txt
      set +e
      timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/raw.txt --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout 8s ${remote_bucket} ${pub_clone_work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        return 255
      fi
      if test -f ${pub_clone_work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${former_index}; then
        mv -f ${pub_clone_work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${former_index} ${pub_clone_work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/4PubSub_index.txt
      fi
    fi
  done
  rm -rf ${tar_index_work_directory}/tar/${extension_type}
  mkdir -p ${tar_index_work_directory}/tar/${extension_type}
  cp /dev/null ${tar_index_work_directory}/tar/${extension_type}/list.txt
  for destination_rclone_remote_bucket in `echo "${rclone_remote_bucket_main_sub}" | tr ';' '\n'`; do
    set +e
    timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/ | grep -E '^[0-9]+\.tar' | sed -e "s|$|/${destination_rclone_remote_bucket}|g" >> ${tar_index_work_directory}/tar/${extension_type}/list.txt
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      rc=${exit_code}
    fi
  done
  if test -s ${tar_index_work_directory}/tar/${extension_type}/list.txt; then
    mkdir -p ${pub_clone_work_directory}/${pub_clone_directory}/processed/${extension_type}
    remote_bucket_tar_file=`sort -u ${tar_index_work_directory}/tar/${extension_type}/list.txt | tail -1 | sed -e "s|\([^/]\+\)/\([^/]\+\)|\2/${destination_tar_index_directory}/tar/${extension_type}/\1|"`
    remote_bucket=`echo ${remote_bucket_tar_file} | cut -d/ -f1`
    tar_file=`echo ${remote_bucket_tar_file} | cut -d/ -f2-`
    echo ${tar_file} > ${tar_index_work_directory}/tar/${extension_type}/raw.txt
    set +e
    timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${tar_index_work_directory}/tar/${extension_type}/raw.txt --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout 8s ${remote_bucket} ${pub_clone_work_directory}/${pub_clone_directory}/processed/${extension_type}
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      return 255
    fi
    if test -f ${pub_clone_work_directory}/${pub_clone_directory}/processed/${extension_type}/${tar_file}; then
      mv -f ${pub_clone_work_directory}/${pub_clone_directory}/processed/${extension_type}/${tar_file} ${pub_clone_work_directory}/${pub_clone_directory}/processed/${extension_type}/former.tar
      cwd=`pwd`
      cd ${pub_clone_work_directory}/${pub_clone_directory}/processed/${extension_type}
      tar -xf former.tar
      rm -rf former.tar
      cd ${cwd}
    fi
  fi
  set +e
  ./clone.sh --no_check_pid --config rclone.conf --parallel ${parallel} ${pub_clone_work_directory} ${source_center_id} ${extension_type} ${source_rclone_remote_bucket_main_sub} "${rclone_remote_bucket_main_sub}" inclusive_pattern.txt exclusive_pattern.txt
  exit_code=$?
  set -e
  if test ${exit_code} -ne 0; then
    rc=${exit_code}
  fi
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    for destination_rclone_remote_bucket in `echo "${rclone_remote_bucket_main_sub}" | tr ';' '\n'`; do
      now=`date -u "+%Y%m%d%H%M%S"`
      set +e
      timeout -k 3 30 rclone copyto --checksum --config rclone.conf --contimeout 8s --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${pub_clone_work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/4PubSub_index.txt ${destination_rclone_remote_bucket}/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${now}.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        rc=${exit_code}
      fi
      for index_file in `timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/ | grep -E '^[0-9]+\.txt' | head -n -2`; do
        set +e
        timeout -k 3 30 rclone delete --config rclone.conf --contimeout 8s --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${index_file}
        set -e
      done
    done
  done
  return ${rc}
}
