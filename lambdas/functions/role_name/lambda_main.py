import os

import boto3
import cfnresponse
from aws_lambda_powertools import Logger

logger = Logger()
s3 = boto3.resource("s3")

bucket = os.getenv("SeedBucket")


@logger.inject_lambda_context(log_event=True)
def lambda_handler(event, context):
    response_status = cfnresponse.SUCCESS
    try:
        if "RequestType" in event and event["RequestType"] == "Create":
            logger.info("Processing CREATE event")
            response_data = on_create(event, context)
        if "RequestType" in event and event["RequestType"] == "Update":
            logger.info("Processing UPDATE event")
            response_data = on_create(event, context)
        if "RequestType" in event and event["RequestType"] == "Delete":
            logger.info("Processing DELETE event")
            response_data = no_op(event, context)
    except Exception:
        logger.exception("Something went wrong")
        response_status = cfnresponse.FAILED
        response_data = {}

    cfnresponse.send(event, context, response_status, response_data, "")


def on_create(event, _):
    props = event["ResourceProperties"]
    role_arn = props["RoleArn"]
    role_name = role_arn.split('/')[-1]

    return dict(
        RoleName=role_name,
    )


def no_op(_, __):
    pass
