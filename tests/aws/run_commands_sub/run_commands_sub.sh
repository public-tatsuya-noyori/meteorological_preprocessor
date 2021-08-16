function handler () {
  EVENT_DATA=$1
  set -e
  IFS=$'\n'
  layer_name=object_storage
  export PATH="/opt/${layer_name}:$PATH"
  rc=0
  commands_args_file=commands_args.txt
  for command_args in `cat ${commands_args_file}`; do
    echo ${command_args[@]} | xargs -I {} sh -c "{}" &
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