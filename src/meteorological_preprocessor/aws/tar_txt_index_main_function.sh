function handler () {
  set +o pipefail
  extension_type=txt
  function=tar_txt_index_main_function
  main_sub_num=1
  the_number_of_execution_within_timeout=11
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
  destination_tar_index_directory=4TarIndex
  sub_directory=4Sub
  work_directory=$HOME/${function}
  IFS=$'\n'
  rc=0
  rm -rf ${work_directory}
  mkdir -p ${work_directory}
  is_destination_alive=0
  for destination_rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    destination_rclone_remote_bucket_directory=`echo ${destination_rclone_remote_bucket} | tr ':' '_'`
    mkdir -p ${work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/ > ${work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt
    exit_code=$?
    set -e
    if test ${exit_code} -eq 0; then
      is_destination_alive=1
    else
      cat ${work_directory}/err_log.tmp >&2
      echo "WARNING: ${function} can not get a list of ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/." >&2
      rc=${exit_code}
    fi
    if test -s ${work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt; then
      mkdir -p ${work_directory}/${sub_directory}/processed/${extension_type}
      tar_file=`sort -u ${work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt | grep -E '^[0-9]+\.tar' | tail -1`
      echo ${destination_tar_index_directory}/tar/${extension_type}/${tar_file} > ${work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/raw.txt
      cp /dev/null ${work_directory}/err_log.tmp
      set +e
#      timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/raw.txt --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout 8s ${destination_rclone_remote_bucket} ${work_directory}/${sub_directory}/processed/${extension_type}
      timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/raw.txt --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout 8s ${destination_rclone_remote_bucket} ${work_directory}/${sub_directory}/processed/${extension_type}
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        cat ${work_directory}/err_log.tmp >&2
        echo "ERROR: ${function} can not get a file of ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/${tar_file}." >&2
        return ${exit_code}
      fi
      if test -f ${work_directory}/${sub_directory}/processed/${extension_type}/${destination_tar_index_directory}/tar/${extension_type}/${tar_file}; then
        mv -f ${work_directory}/${sub_directory}/processed/${extension_type}/${destination_tar_index_directory}/tar/${extension_type}/${tar_file} ${work_directory}/${sub_directory}/processed/${extension_type}/former.tar
        cwd=`pwd`
        cd ${work_directory}/${sub_directory}/processed/${extension_type}
        tar -xf former.tar
        cd ${cwd}
      fi
    fi
  done
  if test ${is_destination_alive} -eq 0; then
    echo "ERROR: ${rclone_remote_bucket_main_sub} is not alive." >&2
    return 255
  fi
  is_destination_alive=0
  for destination_rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    destination_rclone_remote_bucket_directory=`echo ${destination_rclone_remote_bucket} | tr ':' '_'`
    mkdir -p ${work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/index/${extension_type}/ > ${work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt
    exit_code=$?
    set -e
    if test ${exit_code} -eq 0; then
      is_destination_alive=1
    else
      cat ${work_directory}/err_log.tmp >&2
      echo "WARNING: ${function} can not get a list of ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/index/${extension_type}/." >&2
      rc=${exit_code}
    fi
    if test -s ${work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt; then
      mkdir -p ${work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}
      index_file=`sort -u ${work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt | grep -E '^[0-9]+\.txt' | tail -1`
      echo ${destination_tar_index_directory}/index/${extension_type}/${index_file} > ${work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}/raw.txt
      cp /dev/null ${work_directory}/err_log.tmp
      set +e
#      timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}/raw.txt --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout 8s ${destination_rclone_remote_bucket} ${work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}
      timeout -k 3 30 rclone copy --checksum --config rclone.conf --contimeout 8s --files-from-raw ${work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}/raw.txt --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --stats 0 --timeout 8s ${destination_rclone_remote_bucket} ${work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}
      set -e
      if test ${exit_code} -ne 0; then
        cat ${work_directory}/err_log.tmp >&2
        echo "ERROR: ${function} can not get a file of ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/index/${extension_type}/${index_file}." >&2
        return ${exit_code}
      fi
      if test -f ${work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}/${destination_tar_index_directory}/index/${extension_type}/${index_file}; then
        mv -f ${work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}/${destination_tar_index_directory}/index/${extension_type}/${index_file} ${work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}/4PubSub_index.txt
      fi
    fi
  done
  if test ${is_destination_alive} -eq 0; then
    echo "ERROR: ${rclone_remote_bucket_main_sub} is not alive." >&2
    return 255
  fi
  set +e
  ./sub.sh --no_check_pid --config rclone.conf --index_only ${work_directory} ${center_id} ${extension_type} ${rclone_remote_bucket_main_sub} inclusive_pattern.txt exclusive_pattern.txt
  exit_code=$?
  set -e
  if test ${exit_code} -ne 0; then
    echo "ERROR: ${function} can not subscribe ${rclone_remote_bucket_main_sub}" 
    rc=${exit_code}
  fi
  gz_index_count=`find ${work_directory}/${sub_directory}/processed/${extension_type} -regextype posix-egrep -regex "^${work_directory}/${sub_directory}/processed/${extension_type}/[0-9]{14}_.*\.txt.gz$" -type f 2>/dev/null | wc -l`
  is_tar=0
  now=`date -u "+%Y%m%d%H%M%S"`
  if test ${gz_index_count} -gt 0; then
    cwd=`pwd`
    cd ${work_directory}/${sub_directory}/processed/${extension_type}
    tar -cf ${now}.tar *.txt.gz
    cd ${cwd}
    is_tar=1
  fi
  for destination_rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    destination_rclone_remote_bucket_directory=`echo ${destination_rclone_remote_bucket} | tr ':' '_'`
    if test ${is_tar} -ne 0; then
      cp /dev/null ${work_directory}/err_log.tmp
      set +e
      timeout -k 3 30 rclone copyto --checksum --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${work_directory}/${sub_directory}/processed/${extension_type}/${now}.tar ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/${now}.tar
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        cat ${work_directory}/err_log.tmp >&2
        echo "ERROR: ${function} can not pub a file of ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/${now}.tar." >&2
        rc=${exit_code}
      fi
      for tar_file in `timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/ | grep -E '^[0-9]+\.tar' | head -n -2`; do
        cp /dev/null ${work_directory}/err_log.tmp
        set +e
        timeout -k 3 30 rclone delete --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/${tar_file}
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          cat ${work_directory}/err_log.tmp >&2
          echo "INFO: ${function} can not delete a file of ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/tar/${extension_type}/${tar_file}" >&2
        fi
      done
    fi
    if test -s ${work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}/4PubSub_index.txt; then
      cp /dev/null ${work_directory}/err_log.tmp
      set +e
      timeout -k 3 30 rclone copyto --checksum --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-check-dest --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}/4PubSub_index.txt ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/index/${extension_type}/${now}.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        cat ${work_directory}/err_log.tmp >&2
        echo "ERROR: ${function} can not pub a file of ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/index/${extension_type}/${now}.txt." >&2
        rc=${exit_code}
      fi
      for index_file in `timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/index/${extension_type}/ | grep -E '^[0-9]+\.txt' | head -n -2`; do
        cp /dev/null ${work_directory}/err_log.tmp
        set +e
        timeout -k 3 30 rclone delete --config rclone.conf --contimeout 8s --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/index/${extension_type}/${index_file}
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          cat ${work_directory}/err_log.tmp >&2
          echo "INFO: ${function} can not delete a file of ${destination_rclone_remote_bucket}/${destination_tar_index_directory}/index/${extension_type}/${index_file}" >&2
        fi
      done
    fi
  done
  return ${rc}
}
