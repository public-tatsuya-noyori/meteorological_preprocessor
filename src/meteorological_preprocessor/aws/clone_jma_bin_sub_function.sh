function handler () {
  set +o pipefail
  source_rclone_remote_bucket_main_sub='jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub'
  source_center_id=jma
  extension_type=bin
  function=clone_jma_bin_sub_function
  main_sub_num=2
  the_number_of_execution_within_timeout=11
  rclone_timeout=480
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
  tar_index_directory=4Clone_TarIndex
  tar_index_work_directory=$HOME/${tar_index_directory}
  IFS=$'\n'
  rc=0
  mkdir -p ${clone_work_directory}/${source_center_id}/${extension_type}
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    rm -rf ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
    mkdir -p ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
    for destination_rclone_remote_bucket in `echo "${rclone_remote_bucket_main_sub}" | tr ';' '\n'`; do
      destination_rclone_remote_bucket_file=`echo ${destination_rclone_remote_bucket} | tr ':' '_'`
      set +e
      timeout -k 3 60 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/ | sed -e "s|$|/${destination_rclone_remote_bucket}|g" | grep -E '^[0-9]+\.txt' > ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt
      exit_code=$?
      set -e
    done
    if test -s ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt; then
      former_index=`sort ${clone_work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt | tail -1 | sed -e "s|\([^/]\+\)/\([^/]\+\)|\2/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/\1|"`
      set +e
      timeout -k 3 ${rclone_timeout} rclone copyto --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${former_index} ${pub_clone_work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/4PubSub_index.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        return 255
      fi
    fi
  done
  for destination_rclone_remote_bucket in `echo "${rclone_remote_bucket_main_sub}" | tr ';' '\n'`; do
    destination_rclone_remote_bucket_directory=`echo ${destination_rclone_remote_bucket} | tr ':' '_'`
    mkdir -p ${tar_index_work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}
    set +e
    timeout -k 3 60 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/tar/${extension_type}/ > ${tar_index_work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      continue
    fi
    if test -s ${tar_index_work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt; then
      mkdir -p ${pub_clone_work_directory}/${pub_clone_directory}/processed
      tar_file=`tail -1 ${tar_index_work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt`
      set +e
      timeout -k 3 ${rclone_timeout} rclone copyto --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/tar/${extension_type}/${tar_file} ${pub_clone_work_directory}/${pub_clone_directory}/processed/
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        return 255
      fi
      tar -xf ${pub_clone_work_directory}/${pub_clone_directory}/processed/${tar_file}
      rm -f ${pub_clone_work_directory}/${pub_clone_directory}/processed/${tar_file}
    fi
  done
  set +e
  ./clone.sh --no_check_pid --config rclone.conf --parallel 16 ${pub_clone_work_directory} ${source_center_id} ${extension_type} ${source_rclone_remote_bucket_main_sub} "${rclone_remote_bucket_main_sub}" inclusive_pattern.txt exclusive_pattern.txt
  set -e
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    for destination_rclone_remote_bucket in `echo "${rclone_remote_bucket_main_sub}" | tr ';' '\n'`; do
      now=`date -u "+%Y%m%d%H%M%S"`
      set +e
      timeout -k 3 ${rclone_timeout} rclone copyto --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${pub_clone_work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/4PubSub_index.txt ${destination_rclone_remote_bucket}/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${now}.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        continue
      fi
      for index_file in `timeout -k 3 60 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/ | grep -E '^[0-9]+\.txt' | head -n -3`; do
        set +e
        timeout -k 3 60 rclone delete --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${index_file}
        set -e
      done
    done
  done
  return ${rc}
}
