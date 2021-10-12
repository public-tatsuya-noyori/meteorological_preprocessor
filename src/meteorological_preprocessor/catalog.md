# Catalog

## Project page
[Tokyo Cloud project / Data exchange with cloud storage](https://public-tatsuya-noyori.github.io/tokyo_cloud_project/cloud_project)

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
4. Change file mode.
```
$ chmod 600 $HOME/.config/rclone/rclone.conf
$ chmod 755 /path/to/sub.sh
```
5. If needed, edit inclusive_pattern.txt and exclusive_pattern.txt.
6. Configure Cron for sub.sh.
```
$ crontab -e

* * * * * /path/to/sub.sh /path/to/sub_search_work_directory jma txt 'jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt
* * * * * /path/to/sub.sh /path/to/sub_search_work_directory jma bin 'jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt

:wq
```
7. Download [search.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/search.sh)
8. Change file mode.
```
$ chmod 755 /path/to/search.sh
```
9. Run search.sh to search keyword.
```
$ /path/to/search.sh /path/to/sub_search_work_directory txt jma:center-aa-cloud-a-region-a-open-main /synop/
```
10. Run search.sh to download the searched files.
```
$ /path/to/search.sh --out /path/to/sub_search_work_directory txt jma:center-aa-cloud-a-region-a-open-main /synop/
```
11. To see the command options, run the command with --help.
```
$ /path/to/search.sh --help
search.sh [--bnadwidth_limit bandwidth_limit_k_bytes_per_s] [--config config_file] [--parallel the_number_of_parallel_transfer] [--timeout rclone_timeout] [--start yyyymmddhhmm] [--end yyyymmddhhmm] [--out] local_work_directory extension_type rclone_remote_bucket keyword_pattern|inclusive_pattern_file [exclusive_pattern_file]
```
## How to publish data and clone data
1. Configure rclone.conf.
```
$ vi $HOME/.config/rclone/rclone.conf

# When using AWS, write as follows.
[***your_center_id***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key_id***
secret_access_key = ***your_secret_access_key***
region = ***your_region***
endpoint = https://s3.***your_region***.amazonaws.com
acl = public-read

# When using Microsoft Azure, write as follows.
[***your_center_id***_main]
type = azureblob
account = ***your_account***
key = ***your_key***

# When using Alibaba Cloud, write as follows.
[***your_center_id***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key_id***
secret_access_key = ***your_secret_access_key***
region = 
endpoint = oss-cn-***your_region***.aliyuncs.com
acl = public-read
force_path_style = false

# When using Google Cloud, write as follows.
[***your_center_id***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key_id***
secret_access_key = ***your_secret_access_key***
region = ***your_region***
endpoint = https://storage.googleapis.com
acl = public-read

# When using Wasabi, write as follows.
[***your_center_id***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key_id***
secret_access_key = ***your_secret_access_key***
region = 
endpoint = https://s3.**your_region***.wasabisys.com
acl = public-read

# When using Minio, write as follows.
[***your_center_id***_main]
type = s3
env_auth = false
access_key_id = ***your_access_key_id***
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
4. Change file mode.
```
$ chmod 755 /path/to/pub.sh
```
5. Run pub.sh.
```
$ /path/to/pub.sh /path/to/pub_clone_work_directory ***your_center_id*** txt /path/to/index.txt '***your_center_id***_main:***your_bucket_on_cloud_storage***' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt
```
6. Download [clone.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/clone.sh) and [move_4PubSub_4Search.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/move_4PubSub_4Search.sh).
7. Change file mode.
```
$ chmod 755 /path/to/clone.sh /path/to/move_4PubSub_4Search.sh
```
8. Configure Cron for clone.sh and move_4PubSub_4Search.sh.
```
$ crontab -e

* * * * * /path/to/clone.sh /path/to/pub_clone_work_directory jma txt 'jma:center-aa-cloud-a-region-a-open-main;jma:center-aa-cloud-a-region-b-open-sub' '***your_center_id***_main:***your_bucket_on_cloud_storage***' /path/to/inclusive_pattern.txt /path/to/exclusive_pattern.txt
* * * * * /path/to/move_4PubSub_4Search.sh /path/to/pub_clone_work_directory ***your_center_id***_main txt '***your_center_id***_main:***your_bucket_main***'

:wq
```
## How to clone data on cloud with serverless computing
### When using AWS
1. [Install awscliv2](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-linux.html) and configure awscliv2
```
$ sudo apt install groff-base
$ sudo apt install zip
$ curl https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip
$ unzip awscliv2.zip
$ sudo ./aws/install
$ aws configure
AWS Access Key ID [None]: ***your_access_key_id***
AWS Secret Access Key [None]: ***your_secret_access_key***
Default region name [None]: ***your_region***
Default output format [None]: None
```
2. Download [deploy.sh](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/aws/deploy.sh), [clone_jma.zip](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/aws/clone_jma.zip) and [bootstrap_body.txt](https://raw.githubusercontent.com/public-tatsuya-noyori/meteorological_preprocessor/master/src/meteorological_preprocessor/aws/bootstrap_body.txt).
3. Change file mode.
```
$ chmod 755 /path/to/deploy.sh
```
4. Run deploy.sh
```
$ /path/to/deploy.sh /path/to/clone_jma.zip /path/to/bootstrap_body.txt '***your_region_main***;***your_region_sub***' '***your_center_id***_main:***your_bucket_main***;***your_center_id***_sub:***your_bucket_sub***' ***your_center_id*** ***your_email_address***
```
## How to conver message file to 24h-cache
1. Install tools to convert
```
$ sudo apt install python3
$ sudo apt install python3-pip
$ sudo apt install libeccodes-tools
$ sudo apt install git
$ git clone https://github.com/public-tatsuya-noyori/meteorological_preprocessor
$ cd meteorological_preprocessor
$ pip3 install .
$ exit
```
2. Prepare files of accumulated messages of CCCCNNNNNNNN.ext on your MSS
 or prepare files of A_TTAAiiCCCCYYGGgg_C_CCCC_yyyyMMddhhmmss.type on [https://www.wis-jma.go.jp/d/o/*/*/*/*/*/*/*](https://www.wis-jma.go.jp/d/o/)
 or prepare files of sn.[0-9][0-9][0-9][0-9].type on [https://tgftp.nws.noaa.gov/SL.us008001/*/*](https://tgftp.nws.noaa.gov/SL.us008001/).
3. Run met_pre_batch_to_cache
```
$ met_pre_batch_to_cache ***your_cccc*** ***your_directory_of_prepared_files*** ***your_24h-cache_directory*** checksum.arrow
```
4. To see the command options, run the command with --help.
