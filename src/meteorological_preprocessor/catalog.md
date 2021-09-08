# Catalog

## Data catalog
[JMA](inclusive_pattern.txt)

## Data explorer
[JMA:Main](http://202.32.195.138:9000/center-aa-cloud-a-region-a-open-main/4Site/explore.html), [JMA:Sub](http://202.32.195.138:9000/center-aa-cloud-a-region-b-open-sub/4Site/explore.html)


## How to subscribe data and search data
1. Install rclone.  
See [here](https://rclone.org/install/) 
2. Configure rclone.conf.
```
$ vi $HOME/.config/rclone/rclone.conf

[jma]
type = s3
env_auth = false
access_key_id =
secret_access_key =
region =
endpoint = http://202.32.195.138:9000
acl = public-read

:wq
```
3. Download [sub.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/sub.sh), [inclusive_pattern.txt](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/inclusive_pattern.txt) and [exclusive_pattern.txt](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/exclusive_pattern.txt).
4. If needed, edit inclusive_pattern.txt and exclusive_pattern.txt.
5. Configure Cron for sub.sh.
```
$ crontab -e

* * * * * /path/to/sub.sh /path/to/sub_search_work_directory jma txt 'jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt
* * * * * /path/to/sub.sh /path/to/sub_search_work_directory jma bin 'jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt

:wq
```
6. Download [search.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/search.sh)
7. Run search.sh.
```
$ /path/to/search.sh /path/to/sub_search_work_directory txt minio:center-aa-cloud-a-region-a-open-main /synop/
```
## How to publish data and clone data
1. Configure rclone.conf.
```
$ vi $HOME/.config/rclone/rclone.conf

# When using AWS, write as follows.
[***your_center_ID***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key***
secret_access_key = ***your_secret_access_key***
region = ***your_region***
endpoint = https://s3.***your_region***.amazonaws.com
acl = public-read

# When using Microsoft Azure, write as follows.
[***your_center_ID***_main]
type = azureblob
account = ***your_account***
key = ***your_key***

# When using Alibaba Cloud, write as follows.
[***your_center_ID***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key***
secret_access_key = ***your_secret_access_key***
region = 
endpoint = oss-cn-***your_region***.aliyuncs.com
acl = public-read
force_path_style = false

# When using Google Cloud, write as follows.
[***your_center_ID***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key***
secret_access_key = ***your_secret_access_key***
region = ***your_region***
endpoint = https://storage.googleapis.com
acl = public-read

# When using Wasabi, write as follows.
[***your_center_ID***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key***
secret_access_key = ***your_secret_access_key***
region = 
endpoint = https://s3.**your_region***.wasabisys.com
acl = public-read

# When using Minio, write as follows.
[***your_center_ID***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key***
secret_access_key = ***your_secret_access_key***
region =
endpoint = **your_endpoint***
acl = public-read

:wq
```
2. Prepare data file and index file.
```
$ cp /path/to/your_synop_bulletin.txt /path/to/pub_clone_work_directory/***your_CCCC***/alphanumeric/surface/synop/202109080000/C_***your_CCCC***_20210908001003846866.txt
$ echo '***your_CCCC***/alphanumeric/surface/synop/202109080000/C_***your_CCCC***_20210908001003846866.txt' > index.txt
```
3. Download [pub.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/pub.sh)
4. Run pub.sh.
```
$ /path/to/pub.sh /path/to/pub_clone_work_directory ***your_center_ID*** txt /path/to/index.txt '***your_center_ID***_main:***your_bucket_on_cloud_storage***' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt
```
5. Download [clone.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/clone.sh) and [move_4PubSub_4Search.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/move_4PubSub_4Search.sh).
6. Configure Cron for clone.sh and move_4PubSub_4Search.sh.
```
$ crontab -e

* * * * * /path/to/clone.sh /path/to/pub_clone_work_directory jma txt 'jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub' '***your_center_ID***_main:***your_bucket_on_cloud_storage***' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt
* * * * * /path/to/move_4PubSub_4Search.sh /path/to/pub_clone_work_directory ***your_center_ID***_main txt '***your_center_ID***_main:***your_bucket_on_cloud_storage***'

:wq
```
