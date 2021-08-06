#!/bin/sh
uid=`grep MINIO_ROOT_USER /home/noyori/minio_uid_passwd.txt | cut -d':' -f2`
passwd=`grep MINIO_ROOT_PASSWORD /home/noyori/minio_uid_passwd.txt | cut -d':' -f2`
MINIO_BROWSER=off MINIO_ROOT_USER=${uid} MINIO_ROOT_PASSWORD=${passwd} /home/noyori/minio server /home/noyori/minio_data 1>> /home/noyori/minio.log 2>>/home/noyori/minio.log &
echo $! > /home/noyori/minio_pid.txt
sleep 10
/home/noyori/mc admin update minio/
