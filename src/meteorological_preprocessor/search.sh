#!/bin/bash
set -e
IFS=$'\n'
bandwidth_limit_k_bytes_per_s=0
config=$HOME/.config/rclone/rclone.conf
end_yyyymmddhhmm=999999999999
job_directory=4Search.tmp
out=0
parallel=4
pubsub_index_directory=4PubSub
rclone_timeout=600
search_index_directory=4Search
start_yyyymmddhhmm=0
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--bandwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--config") config=$2;shift;shift;;
    "--end" ) end_yyyymmddhhmm=$2;shift;shift;;
    "--help" ) echo "$0 [--bandwidth_limit bandwidth_limit_k_bytes_per_s] [--config config_file] [--parallel number_of_parallel_transfer] [--timeout rclone_timeout] [--start yyyymmddhhmm] [--end yyyymmddhhmm] [--out] local_work_directory extension_type rclone_remote_bucket keyword_pattern|inclusive_pattern_file [exclusive_pattern_file]"; exit 0;;
    "--out" ) out=1;shift;;
    "--parallel" ) parallel=$2;shift;shift;;
    "--start" ) start_yyyymmddhhmm=$2;shift;shift;;
    "--timeout" ) rclone_timeout=$2;shift;shift;;
  esac
done
if test -z $4; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
local_work_directory=$1
set +e
extension_type=`echo $2 | grep -E '^(txt|bin)$'`
rclone_remote_bucket=`echo $3 | grep -F ':'`
set -e
if test -z ${extension_type}; then
  echo "ERROR: $2 is not txt or bin." >&2
  exit 199
fi
if test -z "${rclone_remote_bucket}"; then
  echo "ERROR: $3 is not rclone_remote:bucket." >&2
  exit 199
fi
temporary_directory=`id -un`/${extension_type}/`date -u +"%Y%m%d%H%M%S%N"`
keyword_pattern=''
inclusive_pattern_file=''
if test -f $4; then
  inclusive_pattern_file=$4
else
  keyword_pattern=$4
fi
exclusive_pattern_file=''
if test -n $5; then
  if test ! -f $5; then
    echo "ERROR: $5 is not a file." >&2
    exit 199
  else
    exclusive_pattern_file=$5
  fi
fi
set -u
if test -n "${start_yyyymmddhhmm}"; then
  set +e
  start_yyyymmddhhmm=`echo "${start_yyyymmddhhmm}" | grep -E "^([0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]|0)$"`
  set -e
  if test -z ${start_yyyymmddhhmm}; then
    echo "ERROR: start_yyyymmddhhmm is not yyyymmddhhmm." >&2
    exit 199
  fi
  set +e
  start_yyyymmddhhmm=`expr 0 + ${start_yyyymmddhhmm}`
  set -e
fi
if test -n "${end_yyyymmddhhmm}"; then
  set +e
  end_yyyymmddhhmm=`echo "${end_yyyymmddhhmm}" | grep -E "^([0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])$"`
  set -e
  if test -z ${end_yyyymmddhhmm}; then
    echo "ERROR: end_yyyymmddhhmm is not yyyymmddhhmm." >&2
    exit 199
  fi
  set +e
  end_yyyymmddhhmm=`expr 0 + ${end_yyyymmddhhmm}`
  set -e
fi
work_directory=${local_work_directory}/${job_directory}/${temporary_directory}
if test -e ${work_directory}; then
  echo "ERROR: exist ${work_directory}." >&2
  exit 199
fi
mkdir -p ${work_directory}
cp /dev/null ${work_directory}/err_log.tmp
set +e
timeout -k 3 30 rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${extension_type}/ > ${work_directory}/search_index_dir_list.tmp
exit_code=$?
set -e
if test ${exit_code} -ne 0; then
  cat ${work_directory}/err_log.tmp >&2
  echo "ERROR: can not get a list of index directory from ${rclone_remote_bucket}/${search_index_directory}/${extension_type}." >&2
  rm -rf ${work_directory}
  exit ${exit_code}
fi
for index_directory in `cat ${work_directory}/search_index_dir_list.tmp`; do
  yyyymmddhh=`echo ${index_directory} | cut -c1-10`
  set +e
  yyyymmddhh00=`expr 100 \* ${yyyymmddhh}`
  yyyymmddhh99=`expr 99 + ${yyyymmddhh00}`
  set -e
  if test ${yyyymmddhh99} -ge ${start_yyyymmddhhmm} -a ${yyyymmddhh00} -le ${end_yyyymmddhhmm}; then
    cp /dev/null ${work_directory}/err_log.tmp
    set +e
    timeout -k 3 30 rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${search_index_directory}/${extension_type}/${yyyymmddhh}/ > ${work_directory}/search_index_list.tmp
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      cat ${work_directory}/err_log.tmp >&2
      echo "ERROR: can not get a list of index file from ${rclone_remote_bucket}/${search_index_directory}/${extension_type}/${yyyymmddhh}." >&2
      rm -rf ${work_directory}
      exit ${exit_code}
    fi
    for index_file in `cat ${work_directory}/search_index_list.tmp`; do
      mm=`echo ${index_file} | cut -c1-2`
      set +e
      mm=`expr 0 + ${mm}`
      yyyymmddhhmm=`expr ${yyyymmddhh00} + ${mm}`
      set -e
      if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm} -a ${yyyymmddhhmm} -le ${end_yyyymmddhhmm}; then
        index_path="${search_index_directory}/${extension_type}/${yyyymmddhh}/${index_file}"
        echo ${index_path} > ${work_directory}/search_raw.tmp
        cp /dev/null ${work_directory}/err_log.tmp
        set +e
        timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --config ${config} --contimeout ${timeout} --files-from-raw ${work_directory}/search_raw.tmp --local-no-set-modtime --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${work_directory}
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          cat ${work_directory}/err_log.tmp >&2
          echo "ERROR: can not get a index file from ${rclone_remote_bucket}/${search_index_directory}/${extension_type}/${yyyymmddhh}/${index_file}." >&2
          rm -rf ${work_directory}
          exit ${exit_code}
        fi
        mv -f ${work_directory}/${search_index_directory}/${extension_type}/${yyyymmddhh}/${index_file} ${work_directory}/search_index.tmp
        cp /dev/null ${work_directory}/search_file.tmp
        set +e
        if test -n "${exclusive_pattern_file}"; then
          zcat ${work_directory}/search_index.tmp | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/search_file.tmp
        elif test -n "${inclusive_pattern_file}"; then
          zcat ${work_directory}/search_index.tmp | grep -E -f ${inclusive_pattern_file} > ${work_directory}/search_file.tmp
        else
          zcat ${work_directory}/search_index.tmp | grep -E ${keyword_pattern} > ${work_directory}/search_file.tmp
        fi
        set -e
        if test -s ${work_directory}/search_file.tmp; then
          if test ${out} -eq 0; then
            cat ${work_directory}/search_file.tmp
          else
            cp /dev/null ${work_directory}/err_log.tmp
            set +e
            timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --config ${config} --contimeout ${timeout} --files-from-raw ${work_directory}/search_file.tmp --local-no-set-modtime --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${local_work_directory}
            exit_code=$?
            set -e
            if test ${exit_code} -ne 0; then
              cat ${work_directory}/err_log.tmp >&2
              echo "ERROR: can not get files from ${rclone_remote_bucket} ${extension_type}." >&2
              rm -rf ${work_directory}
              exit ${exit_code}
            fi
            sed -e "s|^|${local_work_directory}/|g" ${work_directory}/search_file.tmp | xargs -r -n 64 -P ${parallel} gunzip -f
          fi
        fi
      fi
    done
  fi
done
cp /dev/null ${work_directory}/err_log.tmp
set +e
timeout -k 3 30 rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --contimeout ${timeout} --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/ > ${work_directory}/pubsub_index_list.tmp
exit_code=$?
set -e
if test ${exit_code} -ne 0; then
  cat ${work_directory}/err_log.tmp >&2
  echo "ERROR: can not get a list of index file from ${rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}." >&2
  rm -rf ${work_directory}
  exit ${exit_code}
fi
for index_file in `cat ${work_directory}/pubsub_index_list.tmp`; do
  yyyymmddhhmm=`echo ${index_file} | cut -c1-12`
  set +e
  yyyymmddhhmm=`expr 0 + ${yyyymmddhhmm}`
  set -e
  if test ${yyyymmddhhmm} -ge ${start_yyyymmddhhmm} -a ${yyyymmddhhmm} -le ${end_yyyymmddhhmm}; then
    cp /dev/null ${work_directory}/err_log.tmp
    index_path="${pubsub_index_directory}/${extension_type}/${index_file}"
    echo ${index_path} > ${work_directory}/pubsub_raw.tmp
    set +e
    timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --config ${config} --contimeout ${timeout} --files-from-raw ${work_directory}/pubsub_raw.tmp --local-no-set-modtime --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${work_directory}
    exit_code=$?
    set -e
    if test ${exit_code} -ne 0; then
      cat ${work_directory}/err_log.tmp >&2
      echo "ERROR: can not get a index file from ${rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/${index_file}." >&2
      rm -rf ${work_directory}
      exit ${exit_code}
    fi
    mv -f ${work_directory}/${pubsub_index_directory}/${extension_type}/${index_file} ${work_directory}/pubsub_index.tmp
    cp /dev/null ${work_directory}/pubsub_file.tmp
    set +e
    if test -n "${exclusive_pattern_file}"; then
      zcat ${work_directory}/pubsub_index.tmp | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${work_directory}/pubsub_file.tmp
    elif test -n "${inclusive_pattern_file}"; then
      zcat ${work_directory}/pubsub_index.tmp | grep -E -f ${inclusive_pattern_file} > ${work_directory}/pubsub_file.tmp
    else
      zcat ${work_directory}/pubsub_index.tmp | grep -E ${keyword_pattern} > ${work_directory}/pubsub_file.tmp
    fi
    set -e
    if test -s ${work_directory}/pubsub_file.tmp; then
      if test ${out} -eq 0; then
        cat ${work_directory}/pubsub_file.tmp
      else
        cp /dev/null ${work_directory}/err_log.tmp
        set +e
        timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --config ${config} --contimeout ${timeout} --files-from-raw ${work_directory}/pubsub_file.tmp --local-no-set-modtime --log-file ${work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${rclone_remote_bucket} ${local_work_directory}
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          cat ${work_directory}/err_log.tmp >&2
          echo "ERROR: can not get files from ${rclone_remote_bucket} ${extension_type}." >&2
          rm -rf ${work_directory}
          exit ${exit_code}
        fi
        sed -e "s|^|${local_work_directory}/|g" ${work_directory}/pubsub_file.tmp | xargs -r -n 64 -P ${parallel} gunzip -f
      fi
    fi
  fi
done
rm -rf ${work_directory}
