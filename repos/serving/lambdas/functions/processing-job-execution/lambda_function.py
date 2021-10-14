import datetime as dt
import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

# Retrieve region where Lambda is being executed
region_name = os.environ["AWS_REGION"]  # "ap-southeast-1"

# Create a client for the AWS Analytical service to use
client = boto3.client("glue")
sagemaker = boto3.client("sagemaker")


def datetimeconverter(o):
    if isinstance(o, dt.datetime):
        return o.__str__()


def check_job_status(job_details):
    # This function checks the status of the currently running job
    job_response = client.get_job_run(
        JobName=job_details["jobName"], RunId=job_details["jobRunId"]
    )
    json_data = json.loads(json.dumps(job_response, default=datetimeconverter))
    # IMPORTANT update the status of the job based on the job_response (e.g RUNNING, SUCCEEDED, FAILED)
    job_details["jobStatus"] = json_data.get("JobRun").get("JobRunState")

    response = {"jobDetails": job_details}
    return response


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

        logger.info(event["body"])
        source_bucket = event["body"]["bucket"]
        job_name = event["body"]["targetJob"]
        ddb_table = event["body"]["targetDDBTable"]
        token = event["body"]["token"]
        s3_prefix_key_proc = event["body"]["keysRawProc"]

        logger.info(
            "[{}] [{}] [{}] [{}]".format(
                source_bucket,
                s3_prefix_key_proc,
                job_name,
                ddb_table,
            )
        )

        # Submitting a new Glue Job
        job_response = client.start_job_run(
            JobName=job_name,
            Arguments={
                # Specify any arguments needed based on bucket and keys (e.g. input/output S3 locations)
                "--job-bookmark-option": "job-bookmark-enable",
                "--additional-python-modules": "pyarrow==2,awswrangler==2.9.0",
                # Custom arguments below
                "--TARGET_DDB_TABLE": ddb_table,
                "--S3_BUCKET": source_bucket,
                "--S3_PREFIX_PROCESSED": s3_prefix_key_proc[0]
                #
            },
            MaxCapacity=2.0,
        )

        logger.info("Response is [{}]".format(job_response))

        # Collecting details about Glue Job after submission (e.g. jobRunId for Glue)
        json_data = json.loads(json.dumps(job_response, default=datetimeconverter))

        job_details = {
            "jobName": job_name,
            "jobRunId": json_data.get("JobRunId"),
            "jobStatus": "STARTED",
            "token": token,
        }

        response = {"jobDetails": job_details}

    except Exception as e:
        logger.error("Fatal error", exc_info=True)

        sagemaker.send_pipeline_execution_step_failure(
            CallbackToken=token, FailureReason="error"
        )

        raise e
    return response
