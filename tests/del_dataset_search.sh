#!/bin/sh
rclone lsf minio:aa-open-dataset/4Search/4all_arrow/ | grep -v -E '('`date --date '1 day ago' "+%Y%m%d"`'|'`date -u "+%Y%m%d"`')' | xargs -n1 -I{} rclone delete minio:aa-open-dataset/4Search/4all_arrow/{}
rclone lsf minio:aa-open-dataset/4Search/arrow/ | grep -v -E '('`date --date '1 day ago' "+%Y%m%d"`'|'`date -u "+%Y%m%d"`')' | xargs -n1 -I{} rclone delete minio:aa-open-dataset/4Search/arrow/{}
