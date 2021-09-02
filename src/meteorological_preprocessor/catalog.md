# Catalog

## Data catalog
[Tokyo data catalog](inclusive_pattern.txt)

## Data explorer
[Tokyo data explorer](http://202.32.195.138:9000/center-aa-cloud-a-region-a-open-main/4Site/explore.html)

## How to Subscribe
1. Install rclone.  
See [here](https://rclone.org/install/) 
1. Configure rclone.
```
vi $HOME/.config/rclone/rclone.conf

[minio]
type = s3
provider = Minio
env_auth = false
access_key_id =
secret_access_key =
region =
endpoint = http://202.32.195.138:9000
location_constraint =
server_side_encryption =

:wq
```
1. Download [sub.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/sub.sh), [inclusive_pattern.txt](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/inclusive_pattern.txt) and [exclusive_pattern.txt](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/exclusive_pattern.txt).
1. If needed, edit inclusive_pattern.txt and exclusive_pattern.txt.
1. Configure Cron.
```
crontab -e

* * * * * /path/to/sub.sh /path/to/work_directory center_ID txt 'minio:center-aa-cloud-a-region-a-open-main;minio:center-aa-cloud-a-region-b-open-sub' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt
* * * * * /path/to/sub.sh /path/to/work_directory center_ID bin 'minio:center-aa-cloud-a-region-a-open-main;minio:center-aa-cloud-a-region-b-open-sub' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt

:wq
```
