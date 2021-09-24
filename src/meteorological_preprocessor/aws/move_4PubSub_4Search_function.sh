function handler () {
  set +euo pipefail
  function=move_4PubSub_4Search_function
  the_number_of_execution_within_timeout=5
  set -e
  export HOME="/tmp"
  export PATH="$HOME/aws-cli/bin:$HOME/rclone/bin:$PATH"
  region=`cat $HOME/.aws/config | grep ^region | cut -d= -f2 | sed -e 's| ||g'`
  account=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^account | cut -d= -f2 | sed -e 's| ||g'`
  running=`aws stepfunctions list-executions --state-machine-arn arn:aws:states:${region}:${account}:stateMachine:${function} --max-item ${the_number_of_execution_within_timeout} | grep '"status": "' | grep RUNNING | wc -l`
  if test ${running} -ne 0; then
    return 0
  fi
  center_id=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^center_id | cut -d= -f2 | sed -e 's| ||g'`
  main_sub=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^main_sub | cut -d= -f2 | sed -e 's| ||g'`
  bucket=`cat $HOME/.config/rclone/my_remote_bucket.txt | grep ^bucket | cut -d= -f2 | sed -e 's| ||g'`
  IFS=$'\n'
  rc=0
  commands_args="./move_4PubSub_4Search.sh --no_check_pid --config rclone.conf /tmp/pub_clone_work ${center_id} txt ${center_id}_${main_sub}:${bucket}
./move_4PubSub_4Search.sh --no_check_pid --config rclone.conf /tmp/pub_clone_work ${center_id} bin ${center_id}_${main_sub}:${bucket}"
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
