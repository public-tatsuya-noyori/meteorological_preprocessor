function handler () {
  set +euo pipefail
  source_rclone_remote_bucket_main_sub='jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub'
  destination_rclone_remote_bucket_main_sub=''
  source_center_id=jma
  extension_type=txt
  function=clone_jma_txt_function
  the_number_of_execution_within_timeout=11
  set -e
#  export HOME="/tmp"
  export PATH="$HOME/aws-cli/bin:$HOME/rclone/bin:$PATH"
  region=`cat $HOME/.aws/config | grep ^region | cut -d= -f2 | sed -e 's| ||g'`
  account=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^account | cut -d= -f2 | sed -e 's| ||g'`
#  running=`aws stepfunctions list-executions --state-machine-arn arn:aws:states:${region}:${account}:stateMachine:${function} --max-item ${the_number_of_execution_within_timeout} | grep '"status": "' | grep RUNNING | wc -l`
#  if test ${running} -ne 0; then
#    return 0
#  fi
  center_id=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^center_id | cut -d= -f2 | sed -e 's| ||g'`
  main_sub=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^main_sub | cut -d= -f2 | sed -e 's| ||g'`
  bucket=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^bucket | cut -d= -f2 | sed -e 's| ||g'`
  IFS=$'\n'
  rc=0
  if test -z "${destination_rclone_remote_bucket_main_sub}"; then
    destination_rclone_remote_bucket_main_sub=${center_id}_${main_sub}:${bucket}
  fi
  mkdir -p /tmp/4Clone/${source_center_id}/${extension_type}
  former_index_count=0
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    cp /dev/null /tmp/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
    for destination_rclone_remote_bucket in `echo ${destination_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
      destination_rclone_remote_bucket_file=`echo ${destination_rclone_remote_bucket} | tr ':' '_'`
      set +e
      timeout -k 3 60 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/ | sed -e "s|$|/${destination_rclone_remote_bucket}|g" | grep -E '^[0-9]+\.txt' > /tmp/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
      exit_code=$?
      set -e
    done
    if test -s /tmp/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}; then
      former_index=`sort /tmp/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory} | tail -1 | sed -e "s|\([^/]\+\)/\([^/]\+\)|\2/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/\1|"`
      rclone copyto --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s ${former_index} /tmp/pub_clone_work/4PubClone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/4PubSub_index.txt
      former_index_count=`expr 1 + ${former_index_count}`
    fi
  done
  source_rclone_remote_count=`echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n' | wc -l`
  if test ${source_rclone_remote_count} -ne ${former_index_count}; then
    return 255
  fi
  ./clone.sh --no_check_pid --config rclone.conf /tmp/pub_clone_work ${source_center_id} ${extension_type} ${source_rclone_remote_bucket_main_sub} ${destination_rclone_remote_bucket_main_sub} inclusive_pattern.txt exclusive_pattern.txt
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    for destination_rclone_remote_bucket in `echo ${destination_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
      now=`date -u "+%Y%m%d%H%M%S"`
      rclone copyto --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout 8s /tmp/pub_clone_work/4PubClone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/4PubSub_index.txt ${destination_rclone_remote_bucket}/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${now}.txt
      set +e
      timeout -k 3 60 rclone lsf --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/ | grep -E '^[0-9]+\.txt' > /tmp/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        for index_file in `head -n -1 /tmp/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}`; do
          set +e
          rclone delete --config rclone.conf --contimeout 8s --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout 8s ${destination_rclone_remote_bucket}/4Clone/${source_center_id}/${extension_type}/${source_rclone_remote_bucket_directory}/${index_file}
          set -e
        done
      fi
    done
  done
}
