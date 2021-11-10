import boto3
import json
import gzip

def notify_handler(event, context):
    topic_arn=''
    with open('sns_topic_arn.txt', 'r', encoding='ascii') as f:
        topic_arn = f.read().strip()
    boto3_client_sns = boto3.client('sns')
    boto3_client_s3 = boto3.client('s3')
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_path = record['s3']['object']['key']
        response = boto3_client_s3.get_object(Bucket=bucket_name, Key=object_path)
        response_body = response['Body'].read()
        message = gzip.decompress(response_body).decode('ascii', errors='ignore')
        subject = '/'.join([bucket_name, object_path])
        boto3_client_sns.publish(TopicArn=topic_arn, Message=message, Subject=subject)
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }