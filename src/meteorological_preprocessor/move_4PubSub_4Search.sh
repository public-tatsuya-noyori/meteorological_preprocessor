#!/bin/bash
set -e
IFS=$'\n'
move_4PubSub_4Search() {
  cp /dev/null ${work_directory}/err_log.tmp
  set +e
  timeout -k 3 30 rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/ > ${work_directory}/${pubsub_index_directory}_index.tmp
  exit_code=$?
  set -e
  if test ${exit_code} -ne 0; then
    cat ${work_directory}/err_log.tmp >&2
    echo "ERROR: can not get a list of index file on ${rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}." >&2
    return ${exit_code}
  fi
  for index_file in `head -n -1 ${work_directory}/${pubsub_index_directory}_index.tmp | grep -v -E "^(${move_index_date_hour_minute_pattern})[0-9][0-9]_.*\.txt\.gz$"`; do
    index_file_date_hour=`echo ${index_file} | cut -c1-10`
    index_file_minute_second_extension=`echo ${index_file} | cut -c11-`
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 30 rclone moveto --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --checksum --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/${index_file} ${rclone_remote_bucket}/${search_index_directory}/${extension_type}/${index_file_date_hour}/${index_file_minute_second_extension}
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      cat ${work_directory}/err_log.tmp >&2
      echo "ERROR: can not move ${rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/${index_file} ${rclone_remote_bucket}/${search_index_directory}/${extension_type}/${index_file_date_hour}/${index_file_minute_second_extension}." >&2
      return ${exit_code}
    fi
  done
  return ${exit_code}
}
bandwidth_limit_k_bytes_per_s=0
config=$HOME/.config/rclone/rclone.conf
datetime=`date -u "+%Y%m%d%H%M%S"`
datetime_date=`echo ${datetime} | cut -c1-8`
datetime_hour=`echo ${datetime} | cut -c9-10`
datetime_minute=`echo ${datetime} | cut -c11-12`
ec=0
job_directory=4Move
move_index_date_hour_minute_pattern=${datetime_date}${datetime_hour}${datetime_minute}
move_index_minute=2
for minute_count in `seq ${move_index_minute}`; do
  move_index_date_hour_minute_pattern="${move_index_date_hour_minute_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:${datetime_minute} ${minute_count} minute ago" "+%Y%m%d%H%M"`"|"`date -u -d "${datetime_date} ${datetime_hour}:${datetime_minute} ${minute_count} minute" "+%Y%m%d%H%M"`
done
no_check_pid=0
pubsub_index_directory=4PubSub
search_index_directory=4Search
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--bnadwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--config") config=$2;shift;shift;;
    "--help" ) echo "$0 [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--config config_file] [--no_check_pid] local_work_directory unique_center_id_main_or_sub extension_type rclone_remote_bucket"; exit 0;;
    "--no_check_pid" ) no_check_pid=1;shift;;
  esac
done
if test -z $4; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
set -u
local_work_directory=$1
unique_center_id_main_or_sub=$2
set +e
extension_type=`echo $3 | grep -E '^(txt|bin)$'`
rclone_remote_bucket=`echo $4 | grep -F ':'`
set -e
if test -z "${extension_type}"; then
  echo "ERROR: $3 is not txt or bin." >&2
  exit 199
fi
if test -z "${rclone_remote_bucket}"; then
  echo "ERROR: $4 is not rclone_remote:bucket." >&2
  exit 199
fi
work_directory=${local_work_directory}/${job_directory}/${unique_center_id_main_or_sub}/${extension_type}
mkdir -p ${work_directory}
if test -s ${work_directory}/pid.txt; then
  if test ${no_check_pid} -eq 0; then
    running=`cat ${work_directory}/pid.txt | xargs -r ps ho 'pid comm args' | grep -F " $0 " | grep -F " ${unique_center_id_main_or_sub} " | grep -F " ${extension_type} " | wc -l`
  else
    exit 0
  fi
else
  running=0
fi
if test ${running} -eq 0; then
  move_4PubSub_4Search &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  set +e
  wait ${pid}
  ec=$?
  set -e
  rm ${work_directory}/pid.txt
fi
exit ${ec}
