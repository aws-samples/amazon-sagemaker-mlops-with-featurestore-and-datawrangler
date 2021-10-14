import json
import logging
import os

import boto3

s3_client = boto3.client("s3")
topic_arn = os.getenv("TOPIC_ARN")


def lambda_handler(event, context):
    """ """
    logging.info(event)

    # The name of the model created in the Pipeline CreateModelStep
    bucket_name = event["bucket_name"]
    key_name = event["key_name"]

    # Check if data is fresh
    # TODO: Logic for checking data freshness
    s3_client.download_file(
        Bucket=bucket_name, Key=key_name, Filename="/tmp/result.csv"
    )

    data_fresh = 1

    # TODO: create message to SNS
    """
    client = boto3.client('sns')
    message = "MLOps is starting to run..., data is {}".format(data_fresh)
    response = client.publish(
        TargetArn=topic_arn,
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
    )
    """
    return {"statusCode": 200, "body": json.dumps(data_fresh)}
