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
    return 0
  fi
  rclone_remote_bucket_main_sub=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^rclone_remote_bucket_main_sub | cut -d= -f2 | sed -e 's| ||g'`
  center_id=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^center_id | cut -d= -f2 | sed -e 's| ||g'`
  tar_index_directory=4TarIndex
  tar_index_work_directory=$HOME/${tar_index_directory}
  sub_directory=4Sub
  sub_work_directory=$HOME/${sub_directory}
  IFS=$'\n'
  rc=0
  rm -rf ${sub_work_directory}/${sub_directory}/processed/${extension_type}
  rm -rf ${sub_work_directory}/${sub_directory}/${center_id}/${extension_type}
  for destination_rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    destination_rclone_remote_bucket_directory=`echo ${destination_rclone_remote_bucket} | tr ':' '_'`
    rm -rf ${tar_index_work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}
    mkdir -p ${tar_index_work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}
    set +e
    timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/tar/${extension_type}/ > ${tar_index_work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      continue
    fi
    if test -s ${tar_index_work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt; then
      mkdir -p ${sub_work_directory}/${sub_directory}/processed/${extension_type}
      tar_file=`sort -u ${tar_index_work_directory}/tar/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt | grep -E '^[0-9]+\.tar' | tail -1`
      timeout -k 3 30 rclone copyto --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/tar/${extension_type}/${tar_file} ${sub_work_directory}/${sub_directory}/processed/${extension_type}/${tar_file}
      cwd=`pwd`
      cd ${sub_work_directory}/${sub_directory}/processed/${extension_type}
      tar -xf ${tar_file}
      rm -f ${tar_file}
      cd ${cwd}
    fi
    rm -rf ${tar_index_work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}
    mkdir -p ${tar_index_work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}
    set +e
    timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/index/${extension_type}/ > ${tar_index_work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      continue
    fi
    if test -s ${tar_index_work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt; then
      mkdir -p ${sub_work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}
      index_file=`sort -u ${tar_index_work_directory}/index/${extension_type}/${destination_rclone_remote_bucket_directory}/list.txt | grep -E '^[0-9]+\.txt' | tail -1`
      timeout -k 3 30 rclone copyto --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/index/${extension_type}/${index_file} ${sub_work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}/4PubSub_index.txt
    fi
  done
  set +e
  ./sub.sh --no_check_pid --config rclone.conf --index_only ${sub_work_directory} ${center_id} ${extension_type} "${rclone_remote_bucket_main_sub}" inclusive_pattern.txt exclusive_pattern.txt
  set -e
  gz_index_count=`find ${sub_work_directory}/${sub_directory}/processed/${extension_type} -regextype posix-egrep -regex "^${sub_work_directory}/${sub_directory}/processed/${extension_type}/[0-9]{14}_.*\.txt.gz$" -type f 2>/dev/null | wc -l`
  is_tar=0
  now=`date -u "+%Y%m%d%H%M%S"`
  if test ${gz_index_count} -gt 0; then
    cwd=`pwd`
    cd ${sub_work_directory}/${sub_directory}/processed/${extension_type}
    tar -cf ${now}.tar *.txt.gz
    cd ${cwd}
    is_tar=1
  fi
  for destination_rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    destination_rclone_remote_bucket_directory=`echo ${destination_rclone_remote_bucket} | tr ':' '_'`
    if test ${is_tar} -ne 0; then
      set +e
      timeout -k 3 30 rclone copyto --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${sub_work_directory}/${sub_directory}/processed/${extension_type}/${now}.tar ${destination_rclone_remote_bucket}/${tar_index_directory}/tar/${extension_type}/${now}.tar
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        continue
      fi
      for tar_file in `timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/tar/${extension_type}/ | grep -E '^[0-9]+\.tar' | head -n -3`; do
        set +e
        timeout -k 3 30 rclone delete --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/tar/${extension_type}/${tar_file}
        set -e
      done
    fi
    if test -s ${sub_work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}/4PubSub_index.txt; then
      for destination_rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
        set +e
        timeout -k 3 30 rclone copyto --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${sub_work_directory}/${sub_directory}/${center_id}/${extension_type}/${destination_rclone_remote_bucket_directory}/4PubSub_index.txt ${destination_rclone_remote_bucket}/${tar_index_directory}/index/${extension_type}/${now}.txt
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          continue
        fi
        for index_file in `timeout -k 3 30 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/index/${extension_type}/ | grep -E '^[0-9]+\.txt' | head -n -3`; do
          set +e
          timeout -k 3 30 rclone delete --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/${tar_index_directory}/index/${extension_type}/${index_file}
          set -e
        done
      done
    fi
  done
  return ${rc}
}
