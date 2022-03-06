import json
import logging
import os
from datetime import date, datetime

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

# Retrieve region where Lambda is being executed
region_name = os.environ["AWS_REGION"]

# Retrieve state machine ARN
sm_arn = os.environ["state_machine_arn"]
target_ddb = os.getenv("TARGET_DDB_TABLE")

# Create a client for the AWS Analytical service to use
client = boto3.client("stepfunctions")

sagemaker = boto3.client("sagemaker")
s3 = boto3.client("s3")


def json_serial(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def lambda_handler(event, context):
    """Calls custom job waiter developed by user

    Arguments:
        event {dict} -- Dictionary with details on previous processing step
        context {dict} -- Dictionary with details on Lambda context

    Returns:
        {dict} -- Dictionary with Processed Bucket, Key(s) and Job Details
    """
    try:

        logger.info("Lambda event is [{}]".format(event))
        for record in event["Records"]:
            payload = json.loads(record["body"])
            logger.info("payload: ", payload)
            token = payload["token"]
            arguments = payload["arguments"]
            source_bucket = arguments["bucket"]
            key_to_process = arguments["key_to_process"]

            logger.info("Trigger execution of state machine [{}]".format(sm_arn))

            # Prepare input to state machine
            message = {
                "statusCode": 200,
                "body": {
                    "bucket": source_bucket,
                    "keysRawProc": [key_to_process],
                    "targetDDBTable": target_ddb,
                    "token": token,
                },
                "callbackToken": token
            }

            logger.info("Input Message is [{}]".format(message))

            client.start_execution(
                stateMachineArn=sm_arn, input=json.dumps(message, default=json_serial)
            )

    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        sagemaker.send_pipeline_execution_step_failure(
            CallbackToken=token, FailureReason="Fatal error"
        )
        raise e
    return 200
