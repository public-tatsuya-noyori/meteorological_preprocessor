#!/bin/bash
set -e
IFS=$'\n'
clone() {
  return_code=0
  exit_code=0
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
    echo "ERROR: can not access on ${destination_rclone_remote_bucket_main_sub}." >&2
    return ${exit_code}
  fi
  for source_rclone_remote_bucket in `echo ${source_rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
    if test ${exit_code} -ne 0; then
      return_code=${exit_code}
    fi
    source_rclone_remote_bucket_directory=`echo ${source_rclone_remote_bucket} | tr ':' '_'`
    source_work_directory=${work_directory}/${source_rclone_remote_bucket_directory}
    mkdir -p ${source_work_directory}
    if test ! -f ${source_work_directory}/${pubsub_index_directory}_index.txt; then
      cp /dev/null ${source_work_directory}/err_log.tmp
      set +e
      timeout -k 3 30 rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --contimeout ${timeout} --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/ > ${source_work_directory}/${pubsub_index_directory}_index.txt
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        cat ${source_work_directory}/err_log.tmp >&2
        echo "ERROR: ${exit_code}: can not get a list of index file on ${source_rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}." >&2
        rm -f ${source_work_directory}/${pubsub_index_directory}_index.txt
        continue
      fi
    fi
    cp /dev/null ${source_work_directory}/${pubsub_index_directory}_index_diff.txt
    for get_pubsub_index_retry_count in `seq 2`; do
      rm -rf ${source_work_directory}/${pubsub_index_directory}/${extension_type}
      cp /dev/null ${source_work_directory}/${pubsub_index_directory}_newly_created_index.tmp
      cp /dev/null ${source_work_directory}/${pubsub_index_directory}_new_index.tmp
      cp /dev/null ${source_work_directory}/err_log.tmp
      set +e
      timeout -k 3 30 rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --contimeout ${timeout} --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/ > ${source_work_directory}/${pubsub_index_directory}_new_index.tmp
      exit_code=$?
      set -e
      if test ${exit_code} -ne 0; then
        cat ${source_work_directory}/err_log.tmp >&2
        echo "ERROR: ${exit_code}: can not get a list of index file on ${source_rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}." >&2
        continue
      fi
      cp /dev/null ${source_work_directory}/${pubsub_index_directory}_index_diff.txt
      if test ! -s ${source_work_directory}/${pubsub_index_directory}_new_index.tmp; then
        continue
      fi
      set +e
      diff ${source_work_directory}/${pubsub_index_directory}_index.txt ${source_work_directory}/${pubsub_index_directory}_new_index.tmp | grep -F '>' | cut -c3- | grep -v '^ *$' > ${source_work_directory}/${pubsub_index_directory}_index_diff.txt
      set -e
      if test ! -s ${source_work_directory}/${pubsub_index_directory}_index_diff.txt; then
        break
      fi
      sed -e "s|^|/${pubsub_index_directory}/${extension_type}/|g" ${source_work_directory}/${pubsub_index_directory}_index_diff.txt > ${source_work_directory}/${pubsub_index_directory}_newly_created_index.tmp
      cp /dev/null ${source_work_directory}/err_log.tmp
      set +e
      timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --config ${config} --contimeout ${timeout} --files-from-raw ${source_work_directory}/${pubsub_index_directory}_newly_created_index.tmp --local-no-set-modtime --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${source_work_directory}
      exit_code=$?
      set -e
      if test ${exit_code} -eq 0; then
        break
      else
        sleep 8
      fi
    done
    if test ${exit_code} -ne 0; then
      cat ${source_work_directory}/err_log.tmp >&2
      echo "ERROR: ${exit_code}: can not get index files on ${source_rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}." >&2
      continue
    fi
    if test -s ${source_work_directory}/${pubsub_index_directory}_index_diff.txt; then
      set +e
      cmp -s ${source_work_directory}/${pubsub_index_directory}_index_diff.txt ${source_work_directory}/${pubsub_index_directory}_new_index.tmp
      cmp_exit_code=$?
      set -e
      if test ${cmp_exit_code} -gt 1; then
        exit_code=${cmp_exit_code}
        echo "ERROR: ${exit_code}: can not compare." >&2
        continue
      fi
      rm -rf ${source_work_directory}/${search_index_directory}/${extension_type}
      cp /dev/null ${source_work_directory}/${search_index_directory}_newly_created_index.tmp
      cp /dev/null ${source_work_directory}/${search_index_directory}_new_index.tmp
      if test ${cmp_exit_code} -eq 0; then
        date -u "+%Y%m%d%H%M%S" > ${work_directory}/searched_datetime.txt
        cp /dev/null ${source_work_directory}/err_log.tmp
        set +e
        timeout -k 3 30 rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --contimeout ${timeout} --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${extension_type}/ > ${source_work_directory}/${search_index_directory}_date_hour_slash_directory.tmp
        exit_code=$?
        set -e
        if test ${exit_code} -ne 0; then
          cat ${source_work_directory}/err_log.tmp >&2
          echo "ERROR: ${exit_code}: can not get a list of index directory on ${source_rclone_remote_bucket}/${search_index_directory}/${extension_type}." >&2
          continue
        fi
        sed -e 's|/$||g' ${source_work_directory}/${search_index_directory}_date_hour_slash_directory.tmp > ${source_work_directory}/${search_index_directory}_date_hour_directory.tmp
        if test -s ${source_work_directory}/${search_index_directory}_date_hour_directory.tmp; then
          former_index_file_first_line_prefix=`head -1 ${source_work_directory}/${pubsub_index_directory}_index.txt | cut -c1-12`
          search_index_directory_exit_code=0
          for date_hour_directory in `grep -E "^(${inclusive_index_date_hour_pattern})$" ${source_work_directory}/${search_index_directory}_date_hour_directory.tmp | tac`; do
            cp /dev/null ${source_work_directory}/err_log.tmp
            set +e
            timeout -k 3 30 rclone lsf --bwlimit ${bandwidth_limit_k_bytes_per_s} --config ${config} --contimeout ${timeout} --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --max-depth 1 --no-traverse --quiet --retries 3 --stats 0 --timeout ${timeout} ${source_rclone_remote_bucket}/${search_index_directory}/${extension_type}/${date_hour_directory}/ > ${source_work_directory}/${search_index_directory}_minute_second_index.tmp
            exit_code=$?
            set -e
            if test ${exit_code} -ne 0; then
              search_index_directory_exit_code=${exit_code}
              cat ${source_work_directory}/err_log.tmp >&2
              echo "ERROR: ${exit_code}: can not get a list of index file on ${source_rclone_remote_bucket}/${search_index_directory}/${extension_type}/${date_hour_directory}." >&2
              break
            fi
            sed -e "s|^|${date_hour_directory}|g" ${source_work_directory}/${search_index_directory}_minute_second_index.tmp > ${source_work_directory}/${search_index_directory}_index.tmp
            if test -s ${source_work_directory}/${search_index_directory}_index.tmp; then
              former_index_file_first_line_prefix_count=0
              if test -n "${former_index_file_first_line_prefix}"; then
                former_index_file_first_line_prefix_count=`grep -F ${former_index_file_first_line_prefix} ${source_work_directory}/${search_index_directory}_index.tmp | wc -l`
              fi
              if test ${former_index_file_first_line_prefix_count} -eq 0; then
                set +e
                grep -v -F -f ${source_work_directory}/${pubsub_index_directory}_index.txt ${source_work_directory}/${search_index_directory}_index.tmp >> ${source_work_directory}/${search_index_directory}_new_index.tmp
                set -e
              else
                set +e
                sed -ne "/${former_index_file_first_line_prefix}/,\$p" ${source_work_directory}/${search_index_directory}_index.tmp | grep -v -F -f ${source_work_directory}/${pubsub_index_directory}_index.txt >> ${source_work_directory}/${search_index_directory}_new_index.tmp
                set -e
                break
              fi
            fi
          done
          if test ${search_index_directory_exit_code} -ne 0; then
            continue
          fi
          cat ${source_work_directory}/${search_index_directory}_new_index.tmp | sort -u | xargs -r -n 1 -I {} sh -c 'index_file={};index_file_date_hour=`echo ${index_file} | cut -c1-10`;index_file_minute_second_extension=`echo ${index_file} | cut -c11-`;echo /'${search_index_directory}/${extension_type}'/${index_file_date_hour}/${index_file_minute_second_extension}' > ${source_work_directory}/${search_index_directory}_newly_created_index.tmp
          cp /dev/null ${source_work_directory}/err_log.tmp
          set +e
          timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --config ${config} --contimeout ${timeout} --files-from-raw ${source_work_directory}/${search_index_directory}_newly_created_index.tmp --local-no-set-modtime --log-file ${source_work_directory}/err_log.tmp --low-level-retries 3 --no-traverse --quiet --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${source_work_directory}
          exit_code=$?
          set -e
          if test ${exit_code} -ne 0; then
            cat ${source_work_directory}/err_log.tmp >&2
            echo "ERROR: ${exit_code}: can not get index files on ${source_rclone_remote_bucket}/${search_index_directory}/${extension_type}." >&2
            continue
          fi
        fi
      fi
      cp /dev/null ${source_work_directory}/newly_created_file.tmp
      set +e
      if test -s ${source_work_directory}/${search_index_directory}_newly_created_index.tmp; then
        cat ${source_work_directory}/${search_index_directory}_newly_created_index.tmp ${source_work_directory}/${pubsub_index_directory}_newly_created_index.tmp | sed -e "s|^|${source_work_directory}|g" | xargs -r zcat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/newly_created_file.tmp
      else
        sed -e "s|^|${source_work_directory}|g" ${source_work_directory}/${pubsub_index_directory}_newly_created_index.tmp | xargs -r zcat | grep -v -E -f ${exclusive_pattern_file} | grep -E -f ${inclusive_pattern_file} > ${source_work_directory}/newly_created_file.tmp
      fi
      set -e
      cp /dev/null ${source_work_directory}/filtered_newly_created_file.tmp
      if test -s ${source_work_directory}/newly_created_file.tmp; then
        cp /dev/null ${work_directory}/all_processed_file.txt
        set +e
        find ${processed_directory} -regextype posix-egrep -regex "^${processed_directory}/[0-9]{14}_[^/]*\.txt.gz$" -type f 2>/dev/null | xargs -r zcat > ${work_directory}/all_processed_file.txt 2>/dev/null
        grep -v -F -f ${work_directory}/all_processed_file.txt ${source_work_directory}/newly_created_file.tmp > ${source_work_directory}/filtered_newly_created_file.tmp
        set -e
      fi
      cp /dev/null ${work_directory}/processed_file.txt
      if test -s ${source_work_directory}/filtered_newly_created_file.tmp; then
        if test ${index_only} -eq 0; then
          cp /dev/null ${source_work_directory}/info_log.tmp
          set +e
          timeout -k 3 ${rclone_timeout} rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --config ${config} --contimeout ${timeout} --files-from-raw ${source_work_directory}/filtered_newly_created_file.tmp --log-file ${source_work_directory}/info_log.tmp --log-level DEBUG --low-level-retries 3 --no-check-dest --no-traverse --retries 3 --s3-no-check-bucket --s3-no-head --s3-no-head-object --azureblob-no-head-object --stats 0 --timeout ${timeout} --transfers ${parallel} ${source_rclone_remote_bucket} ${destination_rclone_remote_bucket}
          exit_code=$?
          set -e
          if test ${exit_code} -ne 0; then
            if test ${exit_code} -eq 124; then
              touch ${source_work_directory}/rclone_timeout.txt
              if test -s ${source_work_directory}/rclone_timeout.txt; then
                echo "ERROR: rclone timeout ${exit_code}: terminated get ${extension_type} files on ${source_rclone_remote_bucket}." >&2
                rm -f ${source_work_directory}/${pubsub_index_directory}_index.txt
                echo "INFO: clear index: deleted ${source_work_directory}/${pubsub_index_directory}_index.txt." >&2
                cp /dev/null ${source_work_directory}/rclone_timeout.txt
              else
                echo 124 > ${source_work_directory}/rclone_timeout.txt
                echo "ERROR: rclone timeout ${exit_code}: terminated get ${extension_type} files on ${source_rclone_remote_bucket}." >&2
              fi
            else
              set +e
              grep -F ERROR ${source_work_directory}/info_log.tmp >&2
              set -e
              echo "ERROR: ${exit_code}: can not get ${extension_type} files on ${source_rclone_remote_bucket}." >&2
            fi
            continue
          fi
          cp /dev/null ${source_work_directory}/rclone_timeout.txt
          grep -E "^(.* DEBUG *: *[^ ]* *:.* Unchanged skipping.*|.* INFO *: *[^ ]* *:.* Copied .*)$" ${source_work_directory}/info_log.tmp | sed -e "s|^.* DEBUG *: *\([^ ]*\) *:.* Unchanged skipping.*$|/\1|g" -e "s|^.* INFO *: *\([^ ]*\) *:.* Copied .*$|/\1|g" -e 's|^/||g' | grep -v '^ *$' | sort -u > ${work_directory}/processed_file.txt
          if test -s ${work_directory}/processed_file.txt; then
            for retry_count in `seq ${retry_num}`; do
              rm -rf ${work_directory}/prepare
              mkdir ${work_directory}/prepare
              now=`date -u "+%Y%m%d%H%M%S"`
              cp ${work_directory}/processed_file.txt ${work_directory}/prepare/${now}_${unique_center_id}.txt
              gzip -f ${work_directory}/prepare/${now}_${unique_center_id}.txt
              cp /dev/null ${work_directory}/err_log.tmp
              set +e
              timeout -k 3 30 rclone copy --bwlimit ${bandwidth_limit_k_bytes_per_s} --checksum --config ${config} --contimeout ${timeout} --immutable --log-file ${work_directory}/err_log.tmp --low-level-retries 1 --no-traverse --quiet --retries 1 --s3-no-check-bucket --s3-no-head --stats 0 --timeout ${timeout} ${work_directory}/prepare/ ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/
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
              echo "ERROR: ${exit_code}: can not put ${now}.txt on ${destination_rclone_remote_bucket}/${pubsub_index_directory}/${extension_type}/." >&2
            fi
          fi
        else
          rm -rf ${work_directory}/prepare
          mkdir ${work_directory}/prepare
          now=`date -u "+%Y%m%d%H%M%S"`
          cp ${source_work_directory}/filtered_newly_created_file.tmp ${work_directory}/prepare/${now}_${unique_center_id}.txt
          gzip -f ${work_directory}/prepare/${now}_${unique_center_id}.txt
          mv ${work_directory}/prepare/${now}_${unique_center_id}.txt.gz ${processed_directory}/
        fi
      fi
      if test ${exit_code} -eq 0; then
        mv -f ${source_work_directory}/${pubsub_index_directory}_new_index.tmp ${source_work_directory}/${pubsub_index_directory}_index.txt
      fi
    fi
    if test ${exit_code} -eq 0; then
      find ${processed_directory} -regextype posix-egrep -regex "^${processed_directory}/[0-9]{14}_${unique_center_id}\.txt.gz$" -type f -mmin +${delete_index_minute} 2>/dev/null | xargs -r rm -f
    fi
  done
  if test ${exit_code} -ne 0; then
    return_code=${exit_code}
  fi
  return ${return_code}
}
bandwidth_limit_k_bytes_per_s=0
config=$HOME/.config/rclone/rclone.conf
delete_index_minute=480
ec=0
index_only=0
job_directory=4PubClone
no_check_pid=0
parallel=4
pubsub_index_directory=4PubSub
rclone_timeout=480
retry_num=4
search_index_directory=4Search
timeout=8s
for arg in "$@"; do
  case "${arg}" in
    "--bandwidth_limit") bandwidth_limit_k_bytes_per_s=$2;shift;shift;;
    "--config") config=$2;shift;shift;;
    "--delete_index_minute" ) delete_index_minute=$2;shift;shift;;
    '--help' ) echo "$0 [--bandwidth_limit bandwidth_limit_k_bytes_per_s] [--config config_file] [--delete_index_minute delete_index_minute] [--index_only] [--no_check_pid] [--parallel number_of_parallel_transfer] [--timeout rclone_timeout] local_work_directory unique_center_id extension_type 'source_rclone_remote_bucket_main[;source_rclone_remote_bucket_sub]' 'destination_rclone_remote_bucket_main[;destination_rclone_remote_bucket_sub]' inclusive_pattern_file exclusive_pattern_file"; exit 0;;
    "--index_only" ) index_only=1;shift;;
    "--no_check_pid" ) no_check_pid=1;shift;;
    "--parallel" ) parallel=$2;shift;shift;;
    "--timeout" ) rclone_timeout=$2;shift;shift;;
  esac
done
if test -z "$7"; then
  echo "ERROR: The number of arguments is incorrect.\nTry $0 --help for more information." >&2
  exit 199
fi
set -u
local_work_directory=$1
unique_center_id=$2
set +e
extension_type=`echo $3 | grep -E '^(txt|bin)$'`
source_rclone_remote_bucket_main_sub=`echo $4 | grep -F ':'`
destination_rclone_remote_bucket_main_sub=`echo $5 | grep -F ':'`
set -e
if test -z "${extension_type}"; then
  echo "ERROR: $3 is not txt or bin." >&2
  exit 199
fi
if test -z "${source_rclone_remote_bucket_main_sub}"; then
  echo "ERROR: $4 is not rclone_remote:bucket." >&2
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
datetime=`date -u "+%Y%m%d%H%M%S"`
datetime_date=`echo ${datetime} | cut -c1-8`
datetime_hour=`echo ${datetime} | cut -c9-10`
inclusive_index_date_hour_pattern=${datetime_date}${datetime_hour}
set +e
inclusive_index_hour=`expr ${delete_index_minute} / 60`
if test ${inclusive_index_hour} -gt 1; then
  inclusive_index_hour=`expr ${inclusive_index_hour} - 1`
fi
set -e
for hour_count in `seq ${inclusive_index_hour}`; do
  inclusive_index_date_hour_pattern="${inclusive_index_date_hour_pattern}|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour ago" "+%Y%m%d%H"`"|"`date -u -d "${datetime_date} ${datetime_hour}:00 ${hour_count} hour" "+%Y%m%d%H"`
done
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
  clone &
  pid=$!
  echo ${pid} > ${work_directory}/pid.txt
  set +e
  wait ${pid}
  ec=$?
  set -e
  rm ${work_directory}/pid.txt
fi
exit ${ec}
