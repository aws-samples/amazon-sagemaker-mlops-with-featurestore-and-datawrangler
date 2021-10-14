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


def send_pipeline_execution_success(token):
    try:
        sagemaker.send_pipeline_execution_step_success(
            CallbackToken=token,
            OutputParameters=[
                {
                    "Name": "final_status",
                    "Value": "Glue Job finished.",
                }
            ],
        )
    except:
        logger.info(
            (
                "An error occurred: Step GluePrepCallbackStep is already"
                " in a terminal status, but Step function shouldbe"
            )
        )


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

        job_details = event["body"]["job"]["Payload"]["jobDetails"]

        logger.info("Checking Job Status with user custom code")
        # transform_handler = TransformHandler().stage_transform(team, dataset, stage)
        response = check_job_status(job_details)  # custom user code called

        if response["jobDetails"]["jobStatus"] == "SUCCEEDED":
            send_pipeline_execution_success(job_details["token"])
        elif response["jobDetails"]["jobStatus"] == "FAILED":
            sagemaker.send_pipeline_execution_step_failure(
                CallbackToken=job_details["token"], FailureReason="unknown reason"
            )

        logger.info("Response is [{}]".format(response))

    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        sagemaker.send_pipeline_execution_step_failure(
            CallbackToken=job_details["token"], FailureReason=str(e)
        )

        raise e
    return response
