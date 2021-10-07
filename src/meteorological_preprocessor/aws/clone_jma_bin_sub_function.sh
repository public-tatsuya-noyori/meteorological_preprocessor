function handler () {
  set +o pipefail
  source_rclone_remote_bucket_main_sub='jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub'
  source_center_id=jma
  extension_type=bin
  function=clone_jma_bin_sub_function
  main_sub_num=2
  the_number_of_execution_within_timeout=11
  parallel=24
  export HOME="/tmp"
  export PATH="$HOME/aws-cli/bin:$HOME/rclone/bin:$PATH"
  account=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^account | cut -d= -f2 | sed -e 's| ||g'`
  region=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^region_main_sub | cut -d= -f2 | sed -e 's| ||g' | cut -d';' -f${main_sub_num}`
  running=`aws stepfunctions list-executions --state-machine-arn arn:aws:states:${region}:${account}:stateMachine:${function} --max-item ${the_number_of_execution_within_timeout} | grep '"status": "' | grep RUNNING | wc -l`
  if test ${running} -gt 1; then
    echo "INFO: A former ${function} is running. ${function} ends." >&2
    return 0
  fi
  rclone_remote_bucket_main_sub=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^rclone_remote_bucket_main_sub | cut -d= -f2 | sed -e 's| ||g'`
  center_id=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^center_id | cut -d= -f2 | sed -e 's| ||g'`
  destination_clone_directory=4Clone
  destination_tar_index_directory=4TarIndex
  pub_clone_directory=4PubClone
  work_directory=$HOME/${function}
  IFS=$'\n'
  rc=0
  is_destination_alive=0
  rm -rf ${work_directory}
  mkdir -p ${work_directory}
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    mkdir -p ${work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
    is_destination_alive=0
    for destination_rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
      cp /dev/null ${work_directory}/err_log.tmp
      set +e
      timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/ | grep -E '^[0-9]+\.txt' | sed -e "s|$|/${destination_rclone_remote_bucket}|g" >> ${work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        cat ${work_directory}/err_log.tmp >&2
        echo "WARNING: ${function} can not get a list of ${destination_rclone_remote_bucket}/${destination_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/." >&2
        rc=${exit_code}
      else
        is_destination_alive=1
      fi
    done
    if test ${is_destination_alive} -eq 0; then
      echo "ERROR: ${rclone_remote_bucket_main_sub} is not alive." >&2
      return 255
    fi
    if test -s ${work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt; then
      remote_bucket_former_index=`sort -u ${work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/list.txt | tail -1 | sed -e "s|\([^/]\+\)/\([^/]\+\)|\2/${destination_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/\1|"`
      remote_bucket=`echo ${remote_bucket_former_index} | cut -d/ -f1`
      former_index=`echo ${remote_bucket_former_index} | cut -d/ -f2-`
      echo ${former_index} > ${work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/raw.txt
      cp /dev/null ${work_directory}/err_log.tmp
      set +e
#      timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/raw.txt --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout 8s ${remote_bucket} ${work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
      timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${work_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/raw.txt --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout 8s ${remote_bucket} ${work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        cat ${work_directory}/err_log.tmp >&2
        echo "ERROR: ${function} can not get a file of ${remote_bucket_former_index}." >&2
        return ${exit_code}
      fi
      if test -f ${work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${former_index}; then
        mv -f ${work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${former_index} ${work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/4PubSub_index.txt
      fi
    fi
  done
  mkdir -p ${work_directory}/tar/${extension_type}
  is_destination_alive=0
  for destination_rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/ | grep -E '^[0-9]+\.tar' | sed -e "s|$|/${destination_rclone_remote_bucket}|g" >> ${work_directory}/tar/${extension_type}/list.txt
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      cat ${work_directory}/err_log.tmp >&2
      echo "WARNING: ${function} can not get a list of ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/." >&2
      rc=${exit_code}
    fi
  done
  if test ${is_destination_alive} -eq 0; then
    echo "ERROR: ${rclone_remote_bucket_main_sub} is not alive." >&2
    return 255
  fi
  if test -s ${work_directory}/tar/${extension_type}/list.txt; then
    mkdir -p ${work_directory}/${pub_clone_directory}/processed/${extension_type}
    remote_bucket_tar_file=`sort -u ${work_directory}/tar/${extension_type}/list.txt | tail -1 | sed -e "s|\([^/]\+\)/\([^/]\+\)|\2/${destination_tar_index_directory}/tar/${extension_type}/\1|"`
    remote_bucket=`echo ${remote_bucket_tar_file} | cut -d/ -f1`
    tar_file=`echo ${remote_bucket_tar_file} | cut -d/ -f2-`
    echo ${tar_file} > ${work_directory}/tar/${extension_type}/raw.txt
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
#    timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${work_directory}/tar/${extension_type}/raw.txt --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout 8s ${remote_bucket} ${work_directory}/${pub_clone_directory}/processed/${extension_type}
    timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${work_directory}/tar/${extension_type}/raw.txt --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout 8s ${remote_bucket} ${work_directory}/${pub_clone_directory}/processed/${extension_type}
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      cat ${work_directory}/err_log.tmp >&2
      echo "ERROR: ${function} can not get a file of ${remote_bucket_tar_file}." >&2
      return ${exit_code}
    fi
    if test -f ${work_directory}/${pub_clone_directory}/processed/${extension_type}/${tar_file}; then
      mv -f ${work_directory}/${pub_clone_directory}/processed/${extension_type}/${tar_file} ${work_directory}/${pub_clone_directory}/processed/${extension_type}/former.tar
      cwd=`pwd`
      cd ${work_directory}/${pub_clone_directory}/processed/${extension_type}
      tar -xf former.tar
      cd ${cwd}
    fi
  fi
  set +e
  ./clone.sh --no_check_pid --config rclone.conf --parallel ${parallel} ${work_directory} ${source_center_id} ${extension_type} ${source_rclone_remote_bucket_main_sub} ${rclone_remote_bucket_main_sub} inclusive_pattern.txt exclusive_pattern.txt
  exit_code=$?
  set -e
  if test ${exit_code} -ne 0; then
    echo "ERROR: ${function} can not clone ${source_rclone_remote_bucket_main_sub} ${rclone_remote_bucket_main_sub}" 
    rc=${exit_code}
  fi
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    for destination_rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
      now=`date -u "+%Y%m%d%H%M%S"`
      cp /dev/null ${work_directory}/err_log.tmp
      set +e
      timeout -k 3 30 rclone copyto --checksum --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${work_directory}/${pub_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/4PubSub_index.txt ${destination_rclone_remote_bucket}/${destination_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${now}.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        cat ${work_directory}/err_log.tmp >&2
        echo "ERROR: ${function} can not pub a file of ${destination_rclone_remote_bucket}/${destination_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${now}.txt." >&2
        rc=${exit_code}
      fi
      for index_file in `timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/ | grep -E '^[0-9]+\.txt' | head -n -2`; do
        cp /dev/null ${work_directory}/err_log.tmp
        set +e
        timeout -k 3 30 rclone delete --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${index_file}
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          cat ${work_directory}/err_log.tmp >&2
          echo "INFO: ${function} can not delete a file of ${destination_rclone_remote_bucket}/${destination_clone_directory}/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${index_file}" >&2
        fi
      done
    done
  done
  return ${rc}
}
