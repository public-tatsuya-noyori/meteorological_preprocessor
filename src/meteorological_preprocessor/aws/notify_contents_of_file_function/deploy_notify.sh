#!/bin/bash
set -ex

deploy(){
  set +ex
  subarn=`aws sns list-subscriptions-by-topic --topic-arn arn:aws:sns:${region}:${account}:${function} 2>/dev/null | grep SubscriptionArn | cut -d: -f2- | sed -e 's|[" ,]||g'`
  aws sns unsubscribe --subscription-arn ${subarn} 1>/dev/null 2>/dev/null
  aws sns delete-topic --topic-arn arn:aws:sns:${region}:${account}:${function} 1>/dev/null 2>/dev/null
  aws lambda delete-function --function-name ${function} 1>/dev/null 2>/dev/null
  aws logs delete-log-group --log-group-name /aws/lambda/${function} 1>/dev/null 2>/dev/null
  aws iam detach-role-policy --role-name ${function}_lambda --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole 1>/dev/null 2>/dev/null
  aws iam delete-role-policy --role-name ${function}_lambda --policy-name ${function}_s3 1>/dev/null 2>/dev/null
  aws iam delete-role-policy --role-name ${function}_lambda --policy-name ${function}_sns 1>/dev/null 2>/dev/null
  aws iam delete-role --role-name ${function}_lambda 1>/dev/null 2>/dev/null
  aws s3api put-bucket-notification-configuration --bucket ${bucket} --notification-configuration '{}'
  if test "${delete}" = 'delete'; then
    return
  fi
  sleep 20
  set -ex
  aws iam create-role --role-name ${function}_lambda --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'
  aws iam attach-role-policy --role-name ${function}_lambda --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  sleep 10
  aws lambda create-function --function-name ${function} --handler notify_function.notify_handler --runtime python3.9 --role arn:aws:iam::${account}:role/${function}_lambda --zip-file fileb://${function_zip}
  aws logs create-log-group --log-group-name /aws/lambda/${function}
  aws sns create-topic --name ${function}
  for email in `echo ${email_list} | tr ';' '\n'`; do
    aws sns subscribe --topic-arn arn:aws:sns:${region}:${account}:${function} --protocol email --notification-endpoint ${email}
  done
  aws iam put-role-policy --role-name ${function}_lambda --policy-name ${function}_sns --policy-document '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sns:Publish", "Resource": "arn:aws:sns:'${region}:${account}:${function}'"}]}'
  aws iam put-role-policy --role-name ${function}_lambda --policy-name ${function}_s3 --policy-document '{"Version":"2012-10-17", "Statement":[{"Effect":"Allow", "Action":["s3:GetObject"], "Resource":"arn:aws:s3:::*"}]}'
  aws lambda add-permission --function-name ${function} --action lambda:InvokeFunction --statement-id ${function} --principal s3.amazonaws.com --source-arn arn:aws:s3:::${bucket}
  aws s3api put-bucket-notification-configuration --bucket ${bucket} --notification-configuration '{"LambdaFunctionConfigurations":[{"Id":"'${function}'","LambdaFunctionArn":"arn:aws:lambda:'${region}:${account}:function:${function}'","Events":["s3:ObjectCreated:*"],"Filter":{"Key":{"FilterRules":[{"Name":"Prefix","Value":"'${object_directory_path_prefix}'"}]}}}]}'
  aws logs put-retention-policy --log-group-name /aws/lambda/${function} --retention-in-days 1
}

set +x
if test -z "$4"; then
  echo "ERROR: The number of arguments is incorrect." >&2
  exit 199
fi

account=`aws sts get-caller-identity | grep '"Account"' | cut -d: -f2 | sed -e 's|[", ]||g'`
function_zip=$1
region_bucket_main_sub="$2"
object_directory_path_prefix=$3
email_list="$4"
delete=$5

count=1
for region_bucket in `echo ${region_bucket_main_sub} | tr ';' '\n'`; do
  region=`echo ${region_bucket} | cut -d: -f1`
  bucket=`echo ${region_bucket} | cut -d: -f2`
  function=notify_`echo ${count}/${object_directory_path_prefix} | sed -e 's|/|_|g'`
  echo arn:aws:sns:${region}:${account}:${function} > sns_topic_arn.txt
  zip -ll ${function_zip} sns_topic_arn.txt
  deploy
  set +x
  count=`expr 1 + ${count}`
done
