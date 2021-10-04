function handler () {
  set +o pipefail
  function=move_4PubSub_4Search_main_function
  main_sub_num=1
  the_number_of_execution_within_timeout=5
  export HOME="/tmp"
  export PATH="$HOME/aws-cli/bin:$HOME/rclone/bin:$PATH"
  account=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^account | cut -d= -f2 | sed -e 's| ||g'`
  region=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^region_main_sub | cut -d= -f2 | sed -e 's| ||g' | cut -d';' -f${main_sub_num}`
  running=`aws stepfunctions list-executions --state-machine-arn arn:aws:states:${region}:${account}:stateMachine:${function} --max-item ${the_number_of_execution_within_timeout} | grep '"status": "' | grep RUNNING | wc -l`
  if test ${running} -gt 1; then
    return 0
  fi
  rclone_remote_bucket=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^rclone_remote_bucket_main_sub | cut -d= -f2 | sed -e 's| ||g' | cut -d';' -f${main_sub_num}`
  center_id=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^center_id | cut -d= -f2 | sed -e 's| ||g'`
  pub_clone_directory=4PubClone
  pub_clone_work_directory=$HOME/${pub_clone_directory}
  IFS=$'\n'
  rc=0
  commands_args="./move_4PubSub_4Search.sh --no_check_pid --config rclone.conf ${pub_clone_work_directory} ${center_id} txt ${rclone_remote_bucket}
./move_4PubSub_4Search.sh --no_check_pid --config rclone.conf ${pub_clone_work_directory} ${center_id} bin ${rclone_remote_bucket}"
  for command_args in `echo "${commands_args}"`; do
    echo ${command_args[@]} | xargs -r -I {} sh -c "{}" &
    pids+=($!)
  done
  for pid in ${pids[@]}; do
    set +e
    wait ${pid}
    cpec=$?
    set -e
    if test ${cpec} -ne 0; then
      rc=${cpec}
    fi
  done
  return ${rc}
}
