#!/bin/bash
set -e

deploy(){
  set +ex
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
  aws logs delete-log-group --log-group-name /aws/step_functions/${function} 1>/dev/null 2>/dev/null
  aws lambda delete-function --function-name ${function} 1>/dev/null 2>/dev/null
  aws stepfunctions delete-state-machine --state-machine-arn arn:aws:states:${region}:${account}:stateMachine:${function} 1>/dev/null 2>/dev/null
  subarn=`aws sns list-subscriptions-by-topic --topic-arn arn:aws:sns:${region}:${account}:${function} 2>/dev/null | grep SubscriptionArn | cut -d: -f2- | sed -e 's|[" ,]||g'`
  aws sns unsubscribe --subscription-arn ${subarn} 1>/dev/null 2>/dev/null
  aws sns delete-topic --topic-arn arn:aws:sns:${region}:${account}:${function} 1>/dev/null 2>/dev/null
  aws events list-targets-by-rule --rule ${function} 2>/dev/null | grep '"Id":' | cut -d: -f2- | sed -e 's|[" ,]||g' | xargs -r -n 1 -I {} aws events remove-targets --rule ${function} --ids {} 1>/dev/null 2>/dev/null
  aws events delete-rule --name ${function} 1>/dev/null 2>/dev/null
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
  aws logs create-log-group --log-group-name /aws/step_functions/${function}
  sleep 10
  aws stepfunctions create-state-machine --name ${function} --definition '{"StartAt": "'${function}'", "States": {"'${function}'": {"Type": "Task", "Resource": "arn:aws:lambda:'${region}:${account}:function:${function}'", "End": true}}, "TimeoutSeconds":'${timeout_seconds}'}' --role-arn arn:aws:iam::${account}:role/service-role/${function}_step_functions --logging-configuration '{"level": "ERROR", "includeExecutionData": false, "destinations": [{"cloudWatchLogsLogGroup": {"logGroupArn": "arn:aws:logs:'${region}:${account}:log-group:/aws/step_functions/${function}':*"}}]}'
  aws iam create-policy --policy-name ${function}_lambda_list_executions --policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["states:ListExecutions"], "Resource": "arn:aws:states:'${region}:${account}:stateMachine:${function}'"}]}'
  aws iam attach-role-policy --role-name ${function}_lambda --policy-arn arn:aws:iam::${account}:policy/${function}_lambda_list_executions
  aws sns create-topic --name ${function}
  aws sns subscribe --topic-arn arn:aws:sns:${region}:${account}:${function} --protocol email --notification-endpoint ${email}
  aws iam put-role-policy --role-name ${function}_lambda --policy-name ${function}_sns --policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sns:Publish", "Resource": "arn:aws:sns:'${region}:${account}:${function}'"}]}'
  aws events put-rule --name ${function} --schedule-expression 'rate(1 minute)'
  aws iam create-role --role-name ${function}_events --path /service-role/ --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'
  aws iam create-policy --policy-name ${function}_events_step_functions --policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": ["states:StartExecution"], "Resource": ["arn:aws:states:'${region}:${account}:stateMachine:${function}'"]}]}'
  aws iam attach-role-policy --role-name ${function}_events --policy-arn arn:aws:iam::${account}:policy/${function}_events_step_functions
  aws lambda update-function-event-invoke-config --function-name ${function} --destination-config '{"OnFailure":{"Destination": "arn:aws:sns:'${region}:${account}:${function}'"}}'
  aws lambda add-permission --function-name ${function} --statement-id ${function} --action 'lambda:InvokeFunction' --principal events.amazonaws.com --source-arn arn:aws:events:${region}:${account}:rule/${function}
  aws events put-targets --rule ${function} --targets 'Id='${function}'_step_functions,Arn=arn:aws:states:'${region}:${account}:stateMachine:${function},RoleArn=arn:aws:iam::${account}:role/service-role/${function}_events
}

if test -z "$6"; then
  echo "ERROR: The number of arguments is incorrect." >&2
  exit 199
fi

account=`aws sts get-caller-identity | grep '"Account"' | cut -d: -f2 | sed -e 's|[", ]||g'`
region=`cat $HOME/.aws/config | grep ^region | cut -d= -f2 | sed -e 's| ||g'`
access_key_id=`cat $HOME/.aws/credentials | grep ^aws_access_key_id | cut -d= -f2 | sed -e 's| ||g'`
secret_access_key=`cat $HOME/.aws/credentials | grep ^aws_secret_access_key | cut -d= -f2 | sed -e 's| ||g'`
function_zip=$1
bootstrap_body_file=$2
email=$3
center_id=$4
main_sub=$5
bucket=$6

echo "#!/bin/sh
set -euo pipefail

access_key_id=${access_key_id}
secret_access_key=${secret_access_key}
region=${region}
center_id=${center_id}
main_sub=${main_sub}
bucket=${bucket}
" > bootstrap
cat ${bootstrap_body_file} >> bootstrap
chmod 755 bootstrap
zip -ll ${function_zip} bootstrap

echo "[${center_id}_${main_sub}]
type = s3
env_auth = false
access_key_id = ${access_key_id}
secret_access_key = ${secret_access_key}
region = ${region}
endpoint = https://s3.${region}.amazonaws.com
acl = public-read

[jma]
type = s3
env_auth = false
access_key_id =
secret_access_key =
region =
endpoint = http://202.32.195.138:9000
acl = public-read" > rclone.conf
chmod 644 rclone.conf
zip -ll ${function_zip} rclone.conf

set -ex

function=clone_jma_txt_function
timeout_seconds=600
deploy

function=clone_jma_bin_function
timeout_seconds=600
deploy

function=move_4PubSub_4Search_function
timeout_seconds=240
deploy

function=tar_txt_index_function
timeout_seconds=600
deploy

function=tar_bin_index_function
timeout_seconds=600
deploy