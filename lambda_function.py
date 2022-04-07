import os
import json
import boto3
from urllib.parse import unquote_plus


def lambda_handler(event, context):

    awsTextract = boto3.client("textract")
    try:
        if "Records" in event:
            response = awsTextract.start_document_analysis(
                DocumentLocation={"S3Object": {"Bucket": str(event["Records"][0]["s3"]["bucket"]["name"]), "Name": unquote_plus(str(event["Records"][0]["s3"]["object"]["key"]))}},
                FeatureTypes=[
                    "TABLES",
                    "FORMS"
                ],
                OutputConfig={
                    "S3Bucket": os.environ["OUTPUTBUCKET"],
                    "S3Prefix": os.environ["S3_PREFIX"]
                },
                NotificationChannel={
                    "SNSTopicArn": os.environ["SNS_ARN"],
                    "RoleArn": os.environ["ROLE_ARN"],
                },
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return {
                    "statusCode": 200,
                    "body": json.dumps("Job created successfully!"),
                }
            else:
                return {
                    "statusCode": 500,
                    "body": json.dumps("Textract failed to parse!"),
                }
    except Exception:
        return {"statusCode": 500, "body": json.dumps("Invalid trigger event!")}