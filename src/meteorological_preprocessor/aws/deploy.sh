#!/bin/bash
set -e

deploy(){
  set +ex
  subarn=`aws sns list-subscriptions-by-topic --topic-arn arn:aws:sns:${region}:${account}:${function} 2>/dev/null | grep SubscriptionArn | cut -d: -f2- | sed -e 's|[" ,]||g'`
  aws sns unsubscribe --subscription-arn ${subarn} 1>/dev/null 2>/dev/null
  aws sns delete-topic --topic-arn arn:aws:sns:${region}:${account}:${function} 1>/dev/null 2>/dev/null
  aws events list-targets-by-rule --rule ${function} 2>/dev/null | grep '"Id":' | cut -d: -f2- | sed -e 's|[" ,]||g' | xargs -r -n 1 -I {} aws events remove-targets --rule ${function} --ids {} 1>/dev/null 2>/dev/null
  aws events delete-rule --name ${function} 1>/dev/null 2>/dev/null
  aws stepfunctions delete-state-machine --state-machine-arn arn:aws:states:${region}:${account}:stateMachine:${function} 1>/dev/null 2>/dev/null
  aws lambda delete-function --function-name ${function} 1>/dev/null 2>/dev/null
  aws logs delete-log-group --log-group-name /aws/lambda/${function} 1>/dev/null 2>/dev/null
  aws logs delete-log-group --log-group-name /aws/step_functions/${function} 1>/dev/null 2>/dev/null
  aws iam detach-role-policy --role-name ${function}_lambda --policy-arn arn:aws:iam::${account}:policy/${function}_lambda_list_executions 1>/dev/null 2>/dev/null
  aws iam detach-role-policy --role-name ${function}_lambda --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole 1>/dev/null 2>/dev/null
  aws iam detach-role-policy --role-name ${function}_step_functions --policy-arn arn:aws:iam::${account}:policy/${function}_step_functions_lambda 1>/dev/null 2>/dev/null
  aws iam detach-role-policy --role-name ${function}_step_functions --policy-arn arn:aws:iam::${account}:policy/${function}_step_functions_cloud_watch 1>/dev/null 2>/dev/null
  aws iam detach-role-policy --role-name ${function}_events --policy-arn arn:aws:iam::${account}:policy/${function}_events_step_functions 1>/dev/null 2>/dev/null
  aws iam delete-policy --policy-arn arn:aws:iam::${account}:policy/${function}_step_functions_lambda 1>/dev/null 2>/dev/null
  aws iam delete-policy --policy-arn arn:aws:iam::${account}:policy/${function}_step_functions_cloud_watch 1>/dev/null 2>/dev/null
  aws iam delete-policy --policy-arn arn:aws:iam::${account}:policy/${function}_lambda_list_executions 1>/dev/null 2>/dev/null
  aws iam delete-policy --policy-arn arn:aws:iam::${account}:policy/${function}_events_step_functions 1>/dev/null 2>/dev/null
  aws iam delete-role-policy --role-name ${function}_lambda --policy-name ${function}_sns 1>/dev/null 2>/dev/null
  aws iam delete-role --role-name ${function}_lambda 1>/dev/null 2>/dev/null
  aws iam delete-role --role-name ${function}_step_functions 1>/dev/null 2>/dev/null
  aws iam delete-role --role-name ${function}_events 1>/dev/null 2>/dev/null
  if test "$7" = 'delete'; then
    return
  fi
  sleep 10
  set -ex
  aws iam create-role --role-name ${function}_lambda --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'
  aws iam attach-role-policy --role-name ${function}_lambda --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  sleep 10
  aws lambda create-function --function-name ${function} --handler ${function}.handler --runtime provided --role arn:aws:iam::${account}:role/${function}_lambda --zip-file fileb://${function_zip} --timeout ${timeout_seconds}
  aws lambda put-function-event-invoke-config --function-name ${function} --maximum-event-age-in-seconds 60 --maximum-retry-attempts 0
  aws lambda put-function-concurrency --function-name ${function} --reserved-concurrent-executions 2
  aws iam create-role --role-name ${function}_step_functions --path /service-role/ --assume-role-policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'
  aws iam create-policy --policy-name ${function}_step_functions_lambda --policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["lambda:InvokeFunction"], "Resource": ["arn:aws:lambda:'${region}:${account}:function:${function}':*"]}, {"Effect": "Allow", "Action": ["lambda:InvokeFunction"], "Resource": ["arn:aws:lambda:'${region}:${account}:function:${function}'"]}]}'
  aws iam attach-role-policy --role-name ${function}_step_functions --policy-arn arn:aws:iam::${account}:policy/${function}_step_functions_lambda
  aws iam create-policy --policy-name ${function}_step_functions_cloud_watch --policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"], "Resource": "*"}]}'
  aws iam attach-role-policy --role-name ${function}_step_functions --policy-arn arn:aws:iam::${account}:policy/${function}_step_functions_cloud_watch
  aws logs create-log-group --log-group-name /aws/lambda/${function}
  aws logs create-log-group --log-group-name /aws/step_functions/${function}
  sleep 20
  aws stepfunctions create-state-machine --name ${function} --definition '{"StartAt": "'${function}'", "States": {"'${function}'": {"Type": "Task", "Resource": "arn:aws:lambda:'${region}:${account}:function:${function}'", "End": true}}, "TimeoutSeconds":'${timeout_seconds}'}' --role-arn arn:aws:iam::${account}:role/service-role/${function}_step_functions --logging-configuration '{"level": "ERROR", "includeExecutionData": false, "destinations": [{"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:'${region}:${account}:log-group:/aws/step_functions/${function}':*"}}]}'
  aws iam create-policy --policy-name ${function}_lambda_list_executions --policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["states:ListExecutions"], "Resource": "arn:aws:states:'${region}:${account}:stateMachine:${function}'"}]}'
  aws iam attach-role-policy --role-name ${function}_lambda --policy-arn arn:aws:iam::${account}:policy/${function}_lambda_list_executions
  if test -n "${email}"; then
    aws sns create-topic --name ${function}
    aws sns subscribe --topic-arn arn:aws:sns:${region}:${account}:${function} --protocol email --notification-endpoint ${email}
    aws iam put-role-policy --role-name ${function}_lambda --policy-name ${function}_sns --policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sns:Publish", "Resource": "arn:aws:sns:'${region}:${account}:${function}'"}]}'
  fi
  aws events put-rule --name ${function} --schedule-expression 'rate(1 minute)'
  if test ${disable_schedule} -eq 1; then
    aws events disable-rule --name ${function}
  fi
  aws iam create-role --role-name ${function}_events --path /service-role/ --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'
  aws iam create-policy --policy-name ${function}_events_step_functions --policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["states:StartExecution"], "Resource": ["arn:aws:states:'${region}:${account}:stateMachine:${function}'"]}]}'
  aws iam attach-role-policy --role-name ${function}_events --policy-arn arn:aws:iam::${account}:policy/${function}_events_step_functions
  if test -n "${email}"; then
    aws lambda update-function-event-invoke-config --function-name ${function} --destination-config '{"OnFailure":{"Destination": "arn:aws:sns:'${region}:${account}:${function}'"}}'
  fi
  aws lambda add-permission --function-name ${function} --statement-id ${function} --action 'lambda:InvokeFunction' --principal events.amazonaws.com --source-arn arn:aws:events:${region}:${account}:rule/${function}
  aws events put-targets --rule ${function} --targets 'Id='${function}'_step_functions,Arn=arn:aws:states:'${region}:${account}:stateMachine:${function},RoleArn=arn:aws:iam::${account}:role/service-role/${function}_events
  aws logs put-retention-policy --log-group-name /aws/lambda/${function} --retention-in-days 1
  aws logs put-retention-policy --log-group-name /aws/step_functions/${function} --retention-in-days 1
}

if test -z "$6"; then
  echo "ERROR: The number of arguments is incorrect." >&2
  exit 199
fi

account=`aws sts get-caller-identity | grep '"Account"' | cut -d: -f2 | sed -e 's|[", ]||g'`
access_key_id=`cat $HOME/.aws/credentials | grep ^aws_access_key_id | cut -d= -f2 | sed -e 's| ||g'`
secret_access_key=`cat $HOME/.aws/credentials | grep ^aws_secret_access_key | cut -d= -f2 | sed -e 's| ||g'`
function_zip=$1
bootstrap_body_file=$2
region_main_sub="$3"
rclone_remote_bucket_main_sub="$4"
center_id=$5
email=$6

cp /dev/null rclone.conf

rclone_remote_bucket_count=1
for rclone_remote_bucket in `echo ${rclone_remote_bucket_main_sub} | tr ';' '\n'`; do
  region=`echo "${region_main_sub}" | cut -d';' -f${rclone_remote_bucket_count}`
  rclone_remote=`echo ${rclone_remote_bucket} | cut -d':' -f1`
  echo "[${rclone_remote}]
type = s3
env_auth = false
access_key_id = ${access_key_id}
secret_access_key = ${secret_access_key}
region = ${region}
endpoint = https://s3.${region}.amazonaws.com
acl = public-read
" >> rclone.conf
  rclone_remote_bucket_count=`expr 1 + ${rclone_remote_bucket_count}`
  set -x
  rclone --config rclone.conf mkdir ${rclone_remote_bucket}
  bucket=`echo ${rclone_remote_bucket} | cut -d: -f2`
  aws s3api put-bucket-lifecycle-configuration --bucket ${bucket} --lifecycle-configuration '{"Rules": [{"Expiration": {"Days": 1}, "ID": "Delete objects that are one day old", "Filter": {}, "Status": "Enabled", "NoncurrentVersionExpiration": {"NoncurrentDays": 1}, "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]}'
  set +x
done

echo "[jma]
type = s3
env_auth = false
access_key_id =
secret_access_key =
region =
endpoint = http://202.32.195.138:9000
acl = public-read" >> rclone.conf
chmod 644 rclone.conf
zip -ll ${function_zip} rclone.conf

region=`echo "${region_main_sub}" | cut -d';' -f1`

echo "#!/bin/sh
set -euo pipefail

access_key_id=${access_key_id}
secret_access_key=${secret_access_key}
region_main_sub='${region_main_sub}'
rclone_remote_bucket_main_sub='${rclone_remote_bucket_main_sub}'
center_id=${center_id}
region=${region}" > bootstrap

cat ${bootstrap_body_file} >> bootstrap
chmod 755 bootstrap
zip -ll ${function_zip} bootstrap

set +e
aws events disable-rule --name clone_jma_txt_main_function 1>/dev/null 2>/dev/null
aws events disable-rule --name tar_txt_index_main_function 1>/dev/null 2>/dev/null
aws events disable-rule --name clone_jma_bin_main_function 1>/dev/null 2>/dev/null
aws events disable-rule --name tar_bin_index_main_function 1>/dev/null 2>/dev/null
aws events disable-rule --name move_4PubSub_4Search_main_function 1>/dev/null 2>/dev/null
aws events disable-rule --name clone_jma_txt_sub_function 1>/dev/null 2>/dev/null
aws events disable-rule --name tar_txt_index_sub_function 1>/dev/null 2>/dev/null
aws events disable-rule --name clone_jma_bin_sub_function 1>/dev/null 2>/dev/null
aws events disable-rule --name tar_bin_index_sub_function 1>/dev/null 2>/dev/null
aws events disable-rule --name move_4PubSub_4Search_sub_function 1>/dev/null 2>/dev/null
set -e

region_count=1
for region in `echo ${region_main_sub} | tr ';' '\n'`; do
  if test ${region_count} -eq 1; then
    function=clone_jma_txt_main_function
    timeout_seconds=600
    email=''
    disable_schedule=0
    deploy
    set +x

    function=tar_txt_index_main_function
    timeout_seconds=240
    email=''
    disable_schedule=0
    deploy
    set +x

    function=clone_jma_bin_main_function
    timeout_seconds=600
    email=''
    disable_schedule=0
    deploy
    set +x

    function=tar_bin_index_main_function
    timeout_seconds=240
    email=''
    disable_schedule=0
    deploy
    set +x

    function=move_4PubSub_4Search_main_function
    timeout_seconds=240
    email=$6
    disable_schedule=0
    deploy
    set +x
  else
    function=clone_jma_txt_sub_function
    timeout_seconds=600
    email=''
    disable_schedule=1
    deploy
    set +x

    function=tar_txt_index_sub_function
    timeout_seconds=240
    email=''
    disable_schedule=1
    deploy
    set +x

    function=clone_jma_bin_sub_function
    timeout_seconds=600
    email=''
    disable_schedule=1
    deploy
    set +x

    function=tar_bin_index_sub_function
    timeout_seconds=240
    email=''
    disable_schedule=1
    deploy
    set +x

    function=move_4PubSub_4Search_sub_function
    timeout_seconds=240
    email=$6
    disable_schedule=0
    deploy
    set +x
  fi
  region_count=`expr 1 + ${region_count}`
done
