#!/bin/bash
set -e
IFS=$'\n'
publish(){
  if test ${extension_type} = 'txt'; then
    non_extension_count=`grep -v '^ *$' ${input_index_file} | grep -v \.${extension_type}$ | wc -l`
    if test ${non_extension_count} -ne 0; then
      echo "ERROR: Do not include non-.${extension_type} file on ${input_index_file}." >&2
      return 199
    fi
  fi
  not_matched_prefix_count=`grep -v ^${local_work_directory}/ ${input_index_file} | wc -l`
  if test ${not_matched_prefix_count} -ne 0; then
    echo "ERROR: can not match ^${local_work_directory}/ on ${input_index_file}." >&2
    return 199
  fi
  set +e
  grep -v '^ *$' ${input_index_file} | xargs -r -n 1 -P ${parallel} test -f
  not_exist_file=$?
  set -e
  if test ${not_exist_file} -ne 0; then
    echo "ERROR: not exist file on ${input_index_file}." >&2
    return 199
  fi
  grep -v '^ *$' ${input_index_file} | sed -e "s|^${local_work_directory}/||g" -e "s|$|.gz|g" | sort -u > ${work_directory}/newly_created_file.tmp
  non_inclusive_pattern_count=`grep -v '^ *$' ${work_directory}/newly_created_file.tmp | grep -v -E -f ${inclusive_pattern_file} | wc -l`
  if test ${non_inclusive_pattern_count} -ne 0; then
    echo "ERROR: Do not include non-inclusive pattern file on ${input_index_file} with .gz." >&2
    return 199
  fi
  non_exclusive_pattern_count=`grep -v '^ *$' ${work_directory}/newly_created_file.tmp | grep -E -f ${exclusive_pattern_file} | wc -l`
  if test ${non_exclusive_pattern_count} -ne 0; then
    echo "ERROR: Do not include non-exclusive pattern file on ${input_index_file} with .gz." >&2
    return 199
  fi
  cp /dev/null ${work_directory}/all_processed_file.txt
  cp /dev/null ${work_directory}/filtered_newly_created_file.tmp
  set +e
  find ${processed_directory} -regextype posix-egrep -regex "^${processed_directory}/[0-9]{14}_[^/]*\.txt.gz$" -type f 2>/dev/null | xargs -r zcat > ${work_directory}/all_processed_file.txt 2>/dev/null
  grep -v -F -f ${work_directory}/all_processed_file.txt ${work_directory}/newly_created_file.tmp > ${work_directory}/filtered_newly_created_file.tmp
  set -e
  already_publishd_file_count=`grep -F -f ${work_directory}/all_processed_file.txt ${work_directory}/newly_created_file.tmp | wc -l`
  if test ${already_publishd_file_count} -ne 0; then
    echo "ERROR: exist already published file on ${input_index_file}." >&2
    return 199
  fi
  cat ${work_directory}/filtered_newly_created_file.tmp | sed -e "s|^|${local_work_directory}/|g" -e "s|\.gz$||g" | xargs -r -n 64 -P ${parallel} gzip -f
  for destination_rclone_remote_bucket in `echo ${destination_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 30 rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${destination_rclone_remote_bucket}/${pubsub_index_directory}/ > /dev/null
    exit_code=$?
    set -e
    if test ${exit_code} -eq 0; then
      break
    else
      cat ${work_directory}/err_log.tmp >&2
      echo "WARNING: can not access on ${destination_rclone_remote_bucket}." >&2
    fi
  done
  if test ${exit_code} -ne 0; then
    cat ${work_directory}/filtered_newly_created_file.tmp | sed -e "s|^|${local_work_directory}/|g" | xargs -r -n 64 -P ${parallel} gunzip -f
    echo "ERROR: can not access on ${destination_rclone_remote_bucket_main_sub}." >&2
    return ${exit_code}
  fi
  cp /dev/null ${work_directory}/processed_file.txt
  if test -s ${work_directory}/filtered_newly_created_file.tmp; then
    cp /dev/null ${work_directory}/info_log.tmp
    set +e
    timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --checksum --contimeout ${timeout} --files-from-raw ${work_directory}/filtered_newly_created_file.tmp --log-file ${work_directory}/info_log.tmp --log-level DEBUG --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} --transfers ${parallel} ${local_work_directory} ${destination_rclone_remote_bucket}
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      set +e
      grep -F ERROR ${work_directory}/info_log.tmp >&2
      set -e
      cat ${work_directory}/filtered_newly_created_file.tmp | sed -e "s|^|${local_work_directory}/|g" | xargs -r -n 64 -P ${parallel} gunzip -f
      echo "ERROR: can not put ${extension_type} files on ${destination_rclone_remote_bucket}." >&2
      return ${exit_code}
    fi
    grep -E "^(.* DEBUG *: *[^ ]* *:.* Unchanged skipping.*|.* INFO *: *[^ ]* *:.* Copied .*)$" ${work_directory}/info_log.tmp | sed -e "s|^.* DEBUG *: *\([^ ]*\) *:.* Unchanged skipping.*$|/\1|g" -e "s|^.* INFO *: *\([^ ]*\) *:.* Copied .*$|/\1|g" -e 's|^/||g' | grep -v '^ *$' | sort -u > ${work_directory}/processed_file.txt
  fi
  if test -s ${work_directory}/processed_file.txt; then
    for retry_count in `seq ${retry_num}`; do
      cp /dev/null ${work_directory}/err_log.tmp
      rm -rf ${work_directory}/prepare
      mkdir ${work_directory}/prepare
      now=`date -u "+%Y%m%d%H%M%S"`
      cp ${work_directory}/processed_file.txt ${work_directory}/prepare/${now}_${unique_center_id}.txt
      gzip -f ${work_directory}/prepare/${now}_${unique_center_id}.txt
      set +e
      timeout -k 3 30 rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --checksum --contimeout ${timeout} --immutable --log-file ${work_directory}/err_log.tmp --low-level-retries 1 --no-traverse --quiet --retries 1 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} ${work_directory}/prepare/ ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        mv ${work_directory}/prepare/${now}_${unique_center_id}.txt.gz ${processed_directory}/
        break
      else
        sleep 1
      fi
    done
    if test ${exit_code} -ne 0; then
      cat ${work_directory}/err_log.tmp >&2
      cat ${work_directory}/filtered_newly_created_file.tmp | sed -e "s|^|${local_work_directory}/|g" | xargs -r -n 64 -P ${parallel} gunzip -f
      echo "ERROR: can not put ${now}.txt on ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/." >&2
      return ${exit_code}
    fi
    if test ${gunzip_at_end} -eq 1; then
      cat ${work_directory}/filtered_newly_created_file.tmp | sed -e "s|^|${local_work_directory}/|g" | xargs -r -n 64 -P ${parallel} gunzip -f
    fi
  fi
  if test ${exit_code} -eq 0; then
    find ${processed_directory} -regextype posix-egrep -regex "^${processed_directory}/[0-9]{14}_${unique_center_id}\.txt.gz$" -type f -mmin +${delete_index_minute} 2>/dev/null | xargs -r rm -f
    if test ${delete_input_index_file} -eq 1; then
      rm -f ${input_index_file}
    fi
  fi
  return ${exit_code}
}
bandwidth_limit_k_bytes_per_s=0
config=$HOME/.config/rclone/rclone.conf
delete_index_minute=480
delete_input_index_file=0
ec=0
gunzip_at_end=0
job_directory=4PubClone
no_check_pid=0
parallel=4
pubsub_index_directory=4PubSub
rclone_timeout=480
retry_num=4
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--bandwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--config") config=$2;shift;shift;;
    "--delete_index_minute" ) delete_index_minute=$2;shift;shift;;
    "--delete_input_index_file" ) delete_input_index_file=1;shift;;
    "--gunzip_at_end" ) gunzip_at_end=1;shift;;
    "--help" ) echo "$0 [--bandwidth_limit bandwidth_limit_k_bytes_per_s] [--config config_file] [--delete_index_minute delete_index_minute] [--delete_input_index_file] [--gunzip_at_end] [--no_check_pid] [--parallel the_number_of_parallel_transfer] [--timeout rclone_timeout] local_work_directory unique_center_id extension_type input_index_file 'destination_rclone_remote_bucket_main[;destination_rclone_remote_bucket_sub]' inclusive_pattern_file exclusive_pattern_file"; exit 0;;
    "--no_check_pid" ) no_check_pid=1;shift;;
    "--parallel" ) parallel=$2;shift;shift;;
    "--timeout" ) rclone_timeout=$2;shift;shift;;
  esac
done
if test -z $7; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
set -u
local_work_directory=$1
unique_center_id=$2
set +e
extension_type=`echo $3 | grep -E '^(txt|bin)$'`
input_index_file=$4
destination_rclone_remote_bucket_main_sub=`echo $5 | grep -F ':'`
set -e
if test -z "${extension_type}"; then
  echo "ERROR: $3 is not txt or bin." >&2
  exit 199
fi
if test ! -s ${input_index_file}; then
  echo "ERROR: $4 is not a file or empty." >&2
  exit 199
fi
if test -z "${destination_rclone_remote_bucket_main_sub}"; then
  echo "ERROR: $5 is not rclone_remote:bucket." >&2
  exit 199
fi
if test ! -f $6; then
  echo "ERROR: $6 is not a file." >&2
  exit 199
fi
inclusive_pattern_file=$6
if test ! -f $7; then
  echo "ERROR: $7 is not a file." >&2
  exit 199
fi
exclusive_pattern_file=$7
work_directory=${local_work_directory}/${job_directory}/${unique_center_id}/${extension_type}
processed_directory=${local_work_directory}/${job_directory}/processed/${extension_type}
mkdir -p ${work_directory} ${processed_directory}
if test -s ${work_directory}/pid.txt; then
  if test ${no_check_pid} -eq 0; then
    running=`cat ${work_directory}/pid.txt | xargs -r ps ho 'pid comm args' | grep -F " $0 " | grep -F " ${unique_center_id} " | grep -F " ${extension_type} " | wc -l`
  else
    exit 0
  fi
else
  running=0
fi
if test ${running} -eq 0; then
  publish &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  set +e
  wait ${pid}
  ec=$?
  set -e
  rm ${work_directory}/pid.txt
fi
exit ${ec}
