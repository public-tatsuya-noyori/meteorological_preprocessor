# Catalog

## Data catalog
[JMA](inclusive_pattern.txt)

## Data explorer
[JMA:Tokyo](http://202.32.195.138:9000/center-aa-cloud-a-region-a-open-main/4Site/explore.html), [JMA:Osaka](http://202.32.195.138:9000/center-aa-cloud-a-region-b-open-sub/4Site/explore.html)


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
5. Configure Cron.
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
2. Configure rclone.conf.
```
$ vi $HOME/.config/rclone/rclone.conf

# When using aws, write as follows.
[***your_center_ID***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key***
secret_access_key = ***your_secret_access_key***
region = ***your_region***
endpoint = https://s3.***your_region***.amazonaws.com
acl = public-read

# When using Microsoft azure, write as follows.
[***your_center_ID***_main]
type = azureblob
account = ***your_account***
key = ***your_key***

# When using Alibaba cloud, write as follows.
[***your_center_ID***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key***
secret_access_key = ***your_secret_access_key***
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
region = **your_region***
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
